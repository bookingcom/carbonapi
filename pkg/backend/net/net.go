// Package net implements a backend that communicates over a network.
// It uses HTTP and protocol buffers for communication.
package net

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bookingcom/carbonapi/pkg/prioritylimiter"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/bookingcom/carbonapi/pkg/util"

	"github.com/dgryski/go-expirecache"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/instrumentation/httptrace"
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
	limiter        *prioritylimiter.Limiter
	cache          *expirecache.Cache
	cacheExpirySec int32

	qHist          *prometheus.HistogramVec
	responsesCount *prometheus.CounterVec

	logger *zap.Logger
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
	Limit              int           // Set limit of concurrent requests to backend. Defaults to no limit.
	PathCacheExpirySec uint32        // Set time in seconds before items in path cache expire. Defaults to 10 minutes.
	Logger             *zap.Logger   // Logger to use. Defaults to a no-op logger.

	// TODO (grzkv): Make metrics mandatory to simplify code. Nil can be replaced by the mock metrics in tests.
	ActiveRequests  prometheus.Gauge
	WaitingRequests prometheus.Gauge
	LimiterEnters   prometheus.Counter
	LimiterExits    *prometheus.CounterVec
	QHist           *prometheus.HistogramVec
	Responses       *prometheus.CounterVec
}

var fmtProto = []string{"protobuf"}

// New creates a new backend from the given configuration.
func New(cfg Config) (*NetBackend, error) {
	b := &NetBackend{
		cache: expirecache.New(0),
	}

	if cfg.PathCacheExpirySec > 0 {
		b.cacheExpirySec = int32(cfg.PathCacheExpirySec)
	} else {
		b.cacheExpirySec = int32(10 * time.Minute / time.Second)
	}

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

	if cfg.Limit > 0 {
		if cfg.ActiveRequests != nil && cfg.WaitingRequests != nil &&
			cfg.LimiterEnters != nil && cfg.LimiterExits != nil {

			b.limiter = prioritylimiter.New(cfg.Limit,
				prioritylimiter.WithMetrics(cfg.ActiveRequests, cfg.WaitingRequests, cfg.LimiterEnters, cfg.LimiterExits))
		}
	}

	b.qHist = cfg.QHist
	b.responsesCount = cfg.Responses

	if cfg.Logger != nil {
		b.logger = cfg.Logger
	} else {
		b.logger = zap.New(nil)
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

// GetCluster returns the backend cluster.
func (b NetBackend) GetCluster() string {
	return b.cluster
}

// Logger returns logger for this backend. Needed to satisfy interface.
func (b NetBackend) Logger() *zap.Logger {
	return b.logger
}

func (b NetBackend) enter(ctx context.Context) error {
	if b.limiter == nil {
		return nil
	}
	priority := util.GetPriority(ctx)
	uuid := util.GetUUID(ctx)
	return b.limiter.Enter(ctx, priority, uuid)
}

func (b NetBackend) leave() error {
	if b.limiter == nil {
		return nil
	}
	return b.limiter.Leave()
}

func (b NetBackend) setTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.timeout > 0 {
		return context.WithTimeout(ctx, b.timeout)
	}

	return context.WithCancel(ctx)
}

func (b NetBackend) request(ctx context.Context, u *url.URL) (*http.Request, error) {
	req, err := http.NewRequest("GET", "", nil)
	if err != nil {
		return nil, err
	}
	req.URL = u

	req = req.WithContext(ctx)
	req = util.MarshalCtx(ctx, req)
	httptrace.Inject(ctx, req)

	return req, nil
}

func (b NetBackend) do(trace types.Trace, req *http.Request, request string) (string, []byte, error) {
	t0 := time.Now()
	resp, err := b.client.Do(req)
	trace.AddHTTPCall(t0)
	trace.ObserveOutDuration(time.Since(t0), b.dc, b.cluster)

	if err != nil {
		if b.responsesCount != nil {
			b.responsesCount.WithLabelValues("http_client_error", request).Inc()
		}
		return "", nil, err
	}

	var body []byte
	var bodyErr error
	if resp.Body != nil {
		defer resp.Body.Close()
		t1 := time.Now()
		body, bodyErr = ioutil.ReadAll(resp.Body)
		if bodyErr != nil {
			if b.responsesCount != nil {
				b.responsesCount.WithLabelValues("http_body_error", request).Inc()
			}
			return "", nil, bodyErr
		}
		trace.AddReadBody(t1)
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
func (b NetBackend) call(ctx context.Context, trace types.Trace, u *url.URL, request string) (string, []byte, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()

	var t *prometheus.Timer
	if b.qHist != nil { // TODO: remove condition when capi is merged with zipper
		t = prometheus.NewTimer(b.qHist.WithLabelValues(request))
	}

	err := b.enter(ctx)
	if err != nil {
		return "", nil, err
	}

	if t != nil {
		t.ObserveDuration()
	}

	defer func() {
		if limiterErr := b.leave(); limiterErr != nil {
			b.logger.Error("Backend limiter full",
				zap.String("host", b.address),
				zap.String("uuid", util.GetUUID(ctx)),
				zap.Error(limiterErr),
			)
		}
	}()

	t1 := time.Now()
	req, err := b.request(ctx, u)

	trace.AddMarshal(t1)
	if err != nil {
		return "", nil, err
	}

	contentType, body, err := b.do(trace, req, request)
	return contentType, body, err
}

// TODO(gmagnusson): Should Contains become something different, where instead
// of answering yes/no to whether the backend contains any of the given
// targets, it returns a filtered list of targets that the backend contains?
// Is it worth it to make the distinction? If go-carbon isn't too unhappy about
// looking up metrics that it doesn't have, we maybe don't need to do this.

// Contains reports whether the backend contains any of the given targets.
func (b NetBackend) Contains(targets []string) bool {
	for _, target := range targets {
		if _, ok := b.cache.Get(target); ok {
			return true
		}
	}

	return false
}

// Render fetches raw metrics from a backend.
func (b NetBackend) Render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	from := request.From
	until := request.Until
	targets := request.Targets

	t0 := time.Now()
	u := b.url("/render/")
	u = carbonapiV2RenderEncoder(u, from, until, targets)
	request.Trace.AddMarshal(t0)

	contentType, resp, err := b.call(ctx, request.Trace, u, "render")
	if err != nil {
		if code, ok := err.(ErrHTTPCode); ok && code == http.StatusNotFound {
			return nil, types.ErrMetricsNotFound
		}

		return nil, err
	}

	t1 := time.Now()
	defer func() {
		request.Trace.AddUnmarshal(t1)
	}()
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

	for _, metric := range metrics {
		b.cache.Set(metric.Name, struct{}{}, 0, b.cacheExpirySec)
	}

	return metrics, nil
}

func carbonapiV2RenderEncoder(u *url.URL, from int32, until int32, targets []string) *url.URL {
	vals := url.Values{
		"target": targets,
		"format": fmtProto,
		"from":   []string{strconv.Itoa(int(from))},
		"until":  []string{strconv.Itoa(int(until))},
	}
	u.RawQuery = vals.Encode()

	return u
}

// Info fetches metadata about a metric from a backend.
func (b NetBackend) Info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	metric := request.Target

	t0 := time.Now()
	u := b.url("/info/")
	u = carbonapiV2InfoEncoder(u, metric)
	request.Trace.AddMarshal(t0)

	_, resp, err := b.call(ctx, request.Trace, u, "info")

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

	t1 := time.Now()
	defer func() {
		request.Trace.AddUnmarshal(t1)
	}()
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

	t0 := time.Now()
	u := b.url("/metrics/find/")
	u = carbonapiV2FindEncoder(u, query)
	request.Trace.AddMarshal(t0)

	contentType, resp, err := b.call(ctx, request.Trace, u, "find")
	if err != nil {
		if code, ok := err.(ErrHTTPCode); ok && code == http.StatusNotFound {
			return types.Matches{}, types.ErrMatchesNotFound
		}

		return types.Matches{}, err
	}

	t1 := time.Now()
	defer func() {
		request.Trace.AddUnmarshal(t1)
	}()
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

	for _, match := range matches.Matches {
		if match.IsLeaf {
			b.cache.Set(match.Path, struct{}{}, 0, b.cacheExpirySec)
		}
	}

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
