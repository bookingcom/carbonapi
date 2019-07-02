// Package net implements a backend that communicates over a network.
// It uses HTTP and protocol buffers for communication.
package net

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/bookingcom/carbonapi/util"

	"github.com/dgryski/go-expirecache"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ErrHTTPCode is a custom error type to distinguish HTTP errors
type ErrHTTPCode struct {
	code int
	err  string
}

func NewErrHTTPCode(code int, err string) error {
	return &ErrHTTPCode{code, err}
}

func (e *ErrHTTPCode) Code() int {
	return e.code
}

func (e *ErrHTTPCode) Error() string {
	return e.err
}

func (e *ErrHTTPCode) Message() string {
	switch e.code / 100 {
	case 4:
		return fmt.Sprintf("HTTP client error %d: %s", e.code, e.err)

	case 5:
		return fmt.Sprintf("HTTP server error %d: %s", e.code, e.err)

	default:
		return fmt.Sprintf("HTTP unknown error %d: %s", e.code, e.err)
	}
}

func StripErrBody(body []byte) string {
	l := len(body)
	if l > 50 {
		l = 50
	} else if body[l-1] == '\n' {
		l--
	}
	return string(body[0:l])
}

func ExtractErr(err error) error {
	netErr, ok := err.(net.Error)
	var i int
	if ok {
		if netErr.Timeout() {
			return NewErrHTTPCode(http.StatusGatewayTimeout, "timeout")
		}
		e := err.Error()
		if i = strings.Index(e, "dial "); i > 0 {
			e = e[i:len(e)]
		} else if i = strings.Index(e, "write"); i > 0 {
			e = e[i:len(e)]
		} else if i = strings.Index(e, "read"); i > 0 {
			e = e[i:len(e)]
		}
		return NewErrHTTPCode(http.StatusServiceUnavailable, e)
	}
	return err
}

// ErrContextCancel signifies context cancellation manual or via timeout
type ErrContextCancel struct {
	Err error
}

func (err ErrContextCancel) Error() string {
	return err.Err.Error()
}

// ContextCancelCause tells why the context was cancelled
func ContextCancelCause(err error) string {
	var cause string
	switch err {
	case context.DeadlineExceeded:
		cause = "deadline"
	case context.Canceled:
		cause = "canceled"
	default:
		cause = "unknown"
	}

	return cause
}

// Backend represents a host that accepts requests for metrics over HTTP.
type Backend struct {
	address        string
	scheme         string
	client         *http.Client
	timeout        time.Duration
	limiter        chan struct{}
	logger         *zap.Logger
	cache          *expirecache.Cache
	cacheExpirySec int32
}

// Config configures an HTTP backend.
//
// The only required field is Address, which must be of the form
// "address[:port]", where address is an IP address or a hostname.
// Address must be a point that can accept HTTP requests.
type Config struct {
	Address string // The backend address.

	// Optional fields
	Client             *http.Client  // The client to use to communicate with backend. Defaults to http.DefaultClient.
	Timeout            time.Duration // Set request timeout. Defaults to no timeout.
	Limit              int           // Set limit of concurrent requests to backend. Defaults to no limit.
	PathCacheExpirySec uint32        // Set time in seconds before items in path cache expire. Defaults to 10 minutes.
	Logger             *zap.Logger   // Logger to use. Defaults to a no-op logger.
}

var fmtProto = []string{"protobuf"}

// New creates a new backend from the given configuration.
func New(cfg Config) (*Backend, error) {
	b := &Backend{
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
		b.limiter = make(chan struct{}, cfg.Limit)
	}

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

func (b Backend) url(path string) *url.URL {
	return &url.URL{
		Scheme: b.scheme,
		Host:   b.address,
		Path:   path,
	}
}

func (b Backend) GetServerAddress() string {
	return b.address
}

// Logger returns logger for this backend. Needed to satisfy interface.
func (b Backend) Logger() *zap.Logger {
	return b.logger
}

func (b Backend) enter(ctx context.Context) error {
	if b.limiter == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		b.logger.Warn("Request context cancelled",
			zap.String("host", b.address),
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(ctx.Err()),
		)
		return ctx.Err()

	case b.limiter <- struct{}{}:
		// fallthrough
	}

	return nil
}

func (b Backend) leave() error {
	if b.limiter == nil {
		return nil
	}

	select {
	case <-b.limiter:
		// fallthrough
	default:
		// this should never happen, but let's not block forever if it does
		return errors.New("Unable to return value to limiter")
	}

	return nil
}

func (b Backend) setTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.timeout > 0 {
		return context.WithTimeout(ctx, b.timeout)
	}

	return context.WithCancel(ctx)
}

func (b Backend) request(ctx context.Context, u *url.URL, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest("GET", "", body)
	if err != nil {
		return nil, err
	}
	req.URL = u

	req = req.WithContext(ctx)
	req = util.MarshalCtx(ctx, req)

	return req, nil
}

type requestRes struct {
	resp *http.Response
	err  error
}

func (b Backend) do(ctx context.Context, trace types.Trace, req *http.Request) (string, []byte, error) {

	ch := make(chan requestRes)
	t0 := time.Now()

	go func() {
		resp, err := b.client.Do(req)
		ch <- requestRes{resp: resp, err: err}
	}()

	select {
	case res := <-ch:
		trace.AddHTTPCall(t0)
		trace.ObserveOutDuration(time.Now().Sub(t0))

		var body []byte
		var bodyErr error
		if res.resp != nil && res.resp.Body != nil {
			t1 := time.Now()
			body, bodyErr = ioutil.ReadAll(res.resp.Body)
			res.resp.Body.Close()
			trace.AddReadBody(t1)
		}

		// TODO (grzkv): we should not try to interpret the body if there is an error
		if res.err != nil {
			return "", nil, ExtractErr(res.err)
		}

		if bodyErr != nil {
			return "", nil, bodyErr
		}

		if res.resp.StatusCode != http.StatusOK {
			return "", body, NewErrHTTPCode(res.resp.StatusCode, StripErrBody(body))
		} else if len(body) == 0 { // fix for graphite-clickhouse empty response
			return res.resp.Header.Get("Content-Type"), nil, NewErrHTTPCode(http.StatusNotFound, "empty response")
		}

		return res.resp.Header.Get("Content-Type"), body, nil

	case <-ctx.Done():
		trace.ObserveOutDuration(time.Now().Sub(t0))

		b.logger.Warn("Request context cancelled",
			zap.String("host", b.address),
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(ctx.Err()),
		)
		return "", nil, ctx.Err()
	}
}

// Call makes a call to a backend.
// If the backend timeout is positive, Call will override the context timeout
// with the backend timeout.
// Call ensures that the outgoing request has a UUID set.
func (b Backend) call(ctx context.Context, trace types.Trace, u *url.URL, body io.Reader) (string, []byte, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()

	t0 := time.Now()
	err := b.enter(ctx)
	trace.AddLimiter(t0)
	if err != nil {
		return "", nil, err
	}

	defer func() {
		if err := b.leave(); err != nil {
			b.logger.Error("Backend limiter full",
				zap.String("host", b.address),
				zap.String("uuid", util.GetUUID(ctx)),
				zap.Error(err),
			)
		}
	}()

	t1 := time.Now()
	req, err := b.request(ctx, u, body)
	trace.AddMarshal(t1)
	if err != nil {
		return "", nil, err
	}

	return b.do(ctx, trace, req)
}

// TODO(gmagnusson): Should Contains become something different, where instead
// of answering yes/no to whether the backend contains any of the given
// targets, it returns a filtered list of targets that the backend contains?
// Is it worth it to make the distinction? If go-carbon isn't too unhappy about
// looking up metrics that it doesn't have, we maybe don't need to do this.

// Contains reports whether the backend contains any of the given targets.
func (b Backend) Contains(targets []string) bool {
	for _, target := range targets {
		if _, ok := b.cache.Get(target); ok {
			return true
		}
	}

	return false
}

// Render fetches raw metrics from a backend.
func (b Backend) Render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	from := request.From
	until := request.Until
	targets := request.Targets

	t0 := time.Now()
	u := b.url("/render/")
	u, body := carbonapiV2RenderEncoder(u, from, until, targets)
	request.Trace.AddMarshal(t0)

	contentType, resp, err := b.call(ctx, request.Trace, u, body)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ErrContextCancel{Err: ctx.Err()}
		}

		if code, ok := err.(*ErrHTTPCode); ok && code.code == http.StatusNotFound {
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

		/* TODO(gmagnusson)
		case "application/json":

		case "application/pickle":

		case "application/x-msgpack":

		case "application/x-carbonapi-v3-pb":
		*/

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

func carbonapiV2RenderEncoder(u *url.URL, from int32, until int32, targets []string) (*url.URL, io.Reader) {
	vals := url.Values{
		"target": targets,
		"format": fmtProto,
		"from":   []string{strconv.Itoa(int(from))},
		"until":  []string{strconv.Itoa(int(until))},
	}
	u.RawQuery = vals.Encode()

	return u, nil
}

// Info fetches metadata about a metric from a backend.
func (b Backend) Info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	metric := request.Target

	t0 := time.Now()
	u := b.url("/info/")
	u, body := carbonapiV2InfoEncoder(u, metric)
	request.Trace.AddMarshal(t0)

	_, resp, err := b.call(ctx, request.Trace, u, body)
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

func carbonapiV2InfoEncoder(u *url.URL, metric string) (*url.URL, io.Reader) {
	vals := url.Values{
		"target": []string{metric},
		"format": fmtProto,
	}
	u.RawQuery = vals.Encode()

	return u, nil
}

// Find resolves globs and finds metrics in a backend.
func (b Backend) Find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	query := request.Query

	t0 := time.Now()
	u := b.url("/metrics/find/")
	u, body := carbonapiV2FindEncoder(u, query)
	request.Trace.AddMarshal(t0)

	contentType, resp, err := b.call(ctx, request.Trace, u, body)
	if err != nil {
		if ctx.Err() != nil {
			return types.Matches{}, ErrContextCancel{Err: ctx.Err()}
		}

		if code, ok := err.(*ErrHTTPCode); ok && code.code == http.StatusNotFound {
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

	/* TODO(gmagnusson)
	case "application/json":

	case "application/pickle":

	case "application/x-msgpack":

	case "application/x-carbonapi-v3-pb":
	TODO (grzkv): This is rather hypothetical
	*/
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

func carbonapiV2FindEncoder(u *url.URL, query string) (*url.URL, io.Reader) {
	vals := url.Values{
		"query":  []string{query},
		"format": fmtProto,
	}
	u.RawQuery = vals.Encode()

	return u, nil
}
