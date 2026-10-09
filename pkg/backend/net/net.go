// Package net implements a backend that communicates over a network.
// It uses HTTP and protocol buffers for communication.
package net

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/bookingcom/carbonapi/pkg/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ErrHTTPCode is a custom error type to distinguish HTTP errors
type ErrHTTPCode int

func (e ErrHTTPCode) Error() string {
	switch e / 100 {
	case 4:
		return fmt.Sprintf("HTTP client error %d", e)

	case 5:
		return fmt.Sprintf("HTTP server error %d", e)

	default:
		return fmt.Sprintf("HTTP unknown error %d", e)
	}
}

// NetBackend represents a host that accepts requests for metrics over HTTP.
type NetBackend struct {
	address        string
	scheme         string
	dc             string
	cluster        string
	client         *http.Client
	timeout        time.Duration
	cache          *pathCache
	cacheExpirySec int32

	// requestGzip mirrors http.Transport's transparent compression: the
	// request asks for gzip and the body is inflated here with a pooled reader.
	requestGzip bool

	responsesCount *prometheus.CounterVec

	lg *zap.Logger
}

// Config configures an HTTP backend.
//
// The only required field is Address, which must be of the form
// "address[:port]", where address is an IP address or a hostname.
// Address must be a point that can accept HTTP requests.
// TODO: Remove. This should be part of internal state.
type Config struct {
	Address string // The backend address.

	// Optional fields
	DC                 string        // The DC where backend belongs to
	Cluster            string        // The cluster where backend belongs to
	Client             *http.Client  // The client to use to communicate with backend. Defaults to http.DefaultClient.
	Timeout            time.Duration // Set request timeout. Defaults to no timeout.
	PathCacheExpirySec uint32        // Set time in seconds before items in path cache expire. Defaults to 10 minutes.
	Logger             *zap.Logger   // Logger to use. Defaults to a no-op logger.

	// TODO (grzkv): Make metrics mandatory to simplify code. Nil can be replaced by the mock metrics in tests.
	QHist     *prometheus.HistogramVec
	Responses *prometheus.CounterVec
}

var fmtProto = []string{"protobuf"}

// New creates a new backend from the given configuration.
func New(cfg Config) (*NetBackend, error) {
	b := &NetBackend{}

	if cfg.PathCacheExpirySec > 0 {
		b.cacheExpirySec = int32(cfg.PathCacheExpirySec)
	} else {
		b.cacheExpirySec = int32(10 * time.Minute / time.Second)
	}
	b.cache = newPathCache(time.Duration(b.cacheExpirySec) * time.Second)

	address, scheme, err := parseAddress(cfg.Address)
	if err != nil {
		return nil, err
	}

	b.address = address
	b.scheme = scheme
	b.cluster = cfg.Cluster
	b.dc = cfg.DC

	if cfg.Timeout > 0 {
		b.timeout = cfg.Timeout
	} else {
		b.timeout = 0
	}

	if cfg.Client != nil {
		b.client = cfg.Client
	} else {
		b.client = http.DefaultClient
	}
	b.requestGzip = transportCompresses(b.client)

	b.responsesCount = cfg.Responses

	if cfg.Logger != nil {
		b.lg = cfg.Logger
	} else {
		b.lg = zap.New(nil)
	}

	return b, nil
}

func parseAddress(address string) (string, string, error) {
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}

	u, err := url.Parse(address)
	if err != nil {
		return "", "", err
	}

	return u.Host, u.Scheme, nil
}

func (b NetBackend) url(path string) *url.URL {
	return &url.URL{
		Scheme: b.scheme,
		Host:   b.address,
		Path:   path,
	}
}

// GetServerAddress returns the server address for this backend.
func (b NetBackend) GetServerAddress() string {
	return b.address
}

// Logger returns logger for this backend. Needed to satisfy interface.
func (b NetBackend) Logger() *zap.Logger {
	return b.lg
}

func (b NetBackend) setTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.timeout > 0 {
		return context.WithTimeout(ctx, b.timeout)
	}

	return context.WithCancel(ctx)
}

// transportCompresses reports whether the client's transport would add
// "Accept-Encoding: gzip" to requests on its own.
func transportCompresses(c *http.Client) bool {
	switch t := c.Transport.(type) {
	case nil:
		return true // http.DefaultTransport
	case *http.Transport:
		return !t.DisableCompression
	default:
		return false
	}
}

func (b NetBackend) request(ctx context.Context, u *url.URL) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "", nil)
	if err != nil {
		return nil, err
	}
	req.URL = u
	if b.requestGzip {
		// Setting the header ourselves stops the transport from inflating the
		// body with a freshly allocated gzip.Reader per response.
		req.Header.Set("Accept-Encoding", "gzip")
	}

	req = util.MarshalCtx(ctx, req)

	return req, nil
}

// Response bodies are read into pooled buffers. Decoders copy everything they
// keep, so a buffer can be reused as soon as its response has been decoded.
var bodyPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// Buffers that grew past this are dropped instead of pooled, so a rare huge
// response does not pin its memory.
const maxPooledBody = 16 << 20

func getBody() *bytes.Buffer {
	return bodyPool.Get().(*bytes.Buffer)
}

func putBody(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > maxPooledBody {
		return
	}
	buf.Reset()
	bodyPool.Put(buf)
}

// gunzipper pairs a gzip reader with the buffered reader it reads through.
// Without the bufio.Reader, gzip.Reader.Reset would allocate one per call.
type gunzipper struct {
	br *bufio.Reader
	zr *gzip.Reader
}

var gunzipPool sync.Pool // *gunzipper

// readBody reads the whole response body into buf, inflating it if the
// response is gzip-encoded.
func readBody(buf *bytes.Buffer, resp *http.Response) error {
	if !strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		if resp.ContentLength > 0 {
			buf.Grow(int(resp.ContentLength) + bytes.MinRead)
		}
		_, err := buf.ReadFrom(resp.Body)
		return err
	}

	g, _ := gunzipPool.Get().(*gunzipper)
	var err error
	if g == nil {
		g = &gunzipper{br: bufio.NewReader(resp.Body)}
		g.zr, err = gzip.NewReader(g.br)
	} else {
		g.br.Reset(resp.Body)
		err = g.zr.Reset(g.br)
	}
	if err != nil {
		return err
	}
	_, err = buf.ReadFrom(g.zr)
	g.br.Reset(nil) // drop the reference to the body
	if err == nil {
		gunzipPool.Put(g)
	}
	return err
}

// do sends the request and returns the response body in a pooled buffer.
// The caller must hand the buffer back with putBody once done with it.
func (b NetBackend) do(req *http.Request, request string) (string, *bytes.Buffer, error) {
	resp, err := b.client.Do(req)

	if err != nil {
		if b.responsesCount != nil {
			b.responsesCount.WithLabelValues("http_client_error", request).Inc()
		}
		return "", nil, err
	}

	var body *bytes.Buffer
	if resp.Body != nil {
		defer resp.Body.Close()
		body = getBody()
		if bodyErr := readBody(body, resp); bodyErr != nil {
			putBody(body)
			if b.responsesCount != nil {
				b.responsesCount.WithLabelValues("http_body_error", request).Inc()
			}
			return "", nil, bodyErr
		}
	}

	if b.responsesCount != nil {
		b.responsesCount.WithLabelValues(strconv.Itoa(resp.StatusCode), request).Inc()
	}
	if resp.StatusCode != http.StatusOK {
		return "", body, ErrHTTPCode(resp.StatusCode)
	}

	return resp.Header.Get("Content-Type"), body, nil

}

// Call makes a call to a backend.
// If the backend timeout is positive, Call will override the context timeout
// with the backend timeout.
// Call ensures that the outgoing request has a UUID set.
// The returned body is pooled; release it with putBody.
func (b NetBackend) call(ctx context.Context, u *url.URL, request string) (string, *bytes.Buffer, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()

	req, err := b.request(ctx, u)
	if err != nil {
		return "", nil, err
	}

	contentType, body, err := b.do(req, request)
	return contentType, body, err
}

// Contains reports whether the backend contains any of the given targets.
func (b NetBackend) Contains(targets []string) bool {
	return b.cache.containsAny(targets)
}

// Render fetches raw metrics from a backend.
func (b NetBackend) Render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	from := request.From
	until := request.Until
	targets := request.Targets

	u := b.url("/render/")
	u = carbonapiV2RenderEncoder(u, from, until, targets)

	contentType, body, err := b.call(ctx, u, "render")
	defer putBody(body)
	if err != nil {
		if code, ok := err.(ErrHTTPCode); ok && code == http.StatusNotFound {
			return nil, types.ErrMetricsNotFound
		}

		return nil, err
	}
	resp := bodyBytes(body)

	var metrics []types.Metric

	switch contentType {
	case "application/x-protobuf", "application/protobuf", "application/octet-stream":
		metrics, err = carbonapi_v2.RenderDecoder(resp)
	case "application/text":
		return nil, errors.Errorf("Unexpected application/text response:\n%s", string(resp))

	default:
		return nil, errors.Errorf("Unknown content type '%s'", contentType)
	}

	if err != nil {
		return metrics, errors.Wrap(err, "Unmarshal failed")
	}

	if len(metrics) == 0 {
		return nil, types.ErrMetricsNotFound
	}

	b.cache.setAll(len(metrics), func(i int) string { return metrics[i].Name })

	return metrics, nil
}

func bodyBytes(body *bytes.Buffer) []byte {
	if body == nil {
		return nil
	}
	return body.Bytes()
}

// carbonapiV2RenderEncoder sets the query to exactly what url.Values.Encode
// produces for these parameters (keys sorted, values escaped), without
// building the intermediate map.
func carbonapiV2RenderEncoder(u *url.URL, from int32, until int32, targets []string) *url.URL {
	n := len("format=protobuf&from=&until=") + 2*11
	for _, t := range targets {
		n += len("&target=") + len(t)
	}
	q := make([]byte, 0, n)
	q = append(q, "format="...)
	q = append(q, fmtProto[0]...)
	q = append(q, "&from="...)
	q = strconv.AppendInt(q, int64(from), 10)
	for _, t := range targets {
		q = append(q, "&target="...)
		q = append(q, url.QueryEscape(t)...)
	}
	q = append(q, "&until="...)
	q = strconv.AppendInt(q, int64(until), 10)
	u.RawQuery = string(q)

	return u
}

// Info fetches metadata about a metric from a backend.
func (b NetBackend) Info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	metric := request.Target

	u := b.url("/info/")
	u = carbonapiV2InfoEncoder(u, metric)

	_, body, err := b.call(ctx, u, "info")
	defer putBody(body)
	resp := bodyBytes(body)

	if code, ok := err.(ErrHTTPCode); ok && code == http.StatusNotFound {
		return nil, types.ErrInfoNotFound
	}

	if err != nil {
		return nil, errors.Wrap(err, "HTTP call failed")
	}

	single, err := carbonapi_v2.IsInfoResponse(resp)
	if err != nil {
		return nil, errors.Wrap(err, "Protobuf unmarshal failed")
	}

	var infos []types.Info
	if single {
		infos, err = carbonapi_v2.SingleInfoDecoder(resp, b.address)
	} else {
		infos, err = carbonapi_v2.MultiInfoDecoder(resp)
	}

	if err != nil {
		return nil, errors.Wrap(err, "Protobuf unmarshal failed")
	}

	if len(infos) == 0 {
		return nil, types.ErrInfoNotFound
	}

	return infos, nil
}

func carbonapiV2InfoEncoder(u *url.URL, metric string) *url.URL {
	vals := url.Values{
		"target": []string{metric},
		"format": fmtProto,
	}
	u.RawQuery = vals.Encode()

	return u
}

// Find resolves globs and finds metrics in a backend.
func (b NetBackend) Find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	query := request.Query

	u := b.url("/metrics/find/")
	u = carbonapiV2FindEncoder(u, query)

	contentType, body, err := b.call(ctx, u, "find")
	defer putBody(body)
	if err != nil {
		if code, ok := err.(ErrHTTPCode); ok && code == http.StatusNotFound {
			return types.Matches{}, types.ErrMatchesNotFound
		}

		return types.Matches{}, err
	}
	resp := bodyBytes(body)

	var matches types.Matches

	switch contentType {
	case "application/x-protobuf", "application/protobuf", "application/octet-stream":
		matches, err = carbonapi_v2.FindDecoder(resp)
	default:
		return types.Matches{}, errors.Errorf("Unknown content type '%s'", contentType)
	}

	if err != nil {
		return matches, errors.Wrap(err, "Protobuf unmarshal failed")
	}

	if len(matches.Matches) == 0 {
		return matches, types.ErrMatchesNotFound
	}

	b.cache.setLeaves(matches.Matches)

	return matches, nil
}

func carbonapiV2FindEncoder(u *url.URL, query string) *url.URL {
	vals := url.Values{
		"query":  []string{query},
		"format": fmtProto,
	}
	u.RawQuery = vals.Encode()

	return u
}

func (b NetBackend) BackendInfo() (addr string, cluster string, dc string) {
	return b.address, b.cluster, b.dc
}
