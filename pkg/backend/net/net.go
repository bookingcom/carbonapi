package net

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/protobuf/carbonapi_v2"
	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Backend represents a host that accepts requests for metrics over HTTP.
type Backend struct {
	address string
	client  *http.Client
	timeout time.Duration
	limiter chan struct{}
	logger  *zap.Logger
}

// Config configures an HTTP backend.
//
// The only required field is Address, which must be of the form
// "address[:port]", where address is an IP address or a hostname.
// Address must be a point that can accept HTTP requests.
type Config struct {
	Address string // The backend address.

	// Optional fields
	Client  *http.Client  // The client to use to communicate with backend. Defaults to http.DefaultClient.
	Timeout time.Duration // Set request timeout. Defaults to no timeout.
	Limit   int           // Set limit of concurrent requests to backend. Defaults to no limit.
	Logger  *zap.Logger   // Logger to use. Defaults to a no-op logger.
}

var fmtProto = []string{"protobuf"}

// New creates a new backend from the given configuration.
func New(cfg Config) Backend {
	b := Backend{
		address: cfg.Address,
	}

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

	return b
}

func (b Backend) URL(path string) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   b.address,
		Path:   path,
	}
}

func (b Backend) Logger() *zap.Logger {
	return b.logger
}

func (b Backend) enter(ctx context.Context) error {
	if b.limiter == nil {
		return nil
	}

	select {
	case <-ctx.Done():
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

func (b Backend) do(ctx context.Context, req *http.Request) ([]byte, error) {
	if err := b.enter(ctx); err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, err
	}

	if err := b.leave(); err != nil {
		b.logger.Error("Backend limiter full",
			zap.String("host", b.address),
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(err),
		)
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return body, errors.Errorf("Bad response code %d", resp.StatusCode)
	}

	return body, nil
}

// Call makes a call to a backend.
// If the backend timeout is positive, Call will override the context timeout
// with the backend timeout.
// Call ensures that the outgoing request has a UUID set.
func (b Backend) Call(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()

	req, err := b.request(ctx, u, body)
	if err != nil {
		return nil, err
	}

	return b.do(ctx, req)
}

func (b Backend) Probe() {}

// Render fetches raw metrics from a backend.
func (b Backend) Render(ctx context.Context, from int32, until int32, targets []string) ([]types.Metric, error) {
	u := b.URL("/render")
	u, body := carbonapiV2RenderEncoder(u, from, until, targets)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	ts, err := carbonapiV2RenderDecoder(resp)

	return ts, err
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

func carbonapiV2RenderDecoder(blob []byte) ([]types.Metric, error) {
	resp := &carbonapi_v2.Metrics{}
	if err := resp.Unmarshal(blob); err != nil {
		return nil, err
	}

	metrics := make([]types.Metric, len(resp.Metrics))
	for i, m := range resp.Metrics {
		metric := types.Metric{
			Name:      m.Name,
			StartTime: m.StartTime,
			StopTime:  m.StopTime,
			StepTime:  m.StepTime,
			Values:    m.Values,
			IsAbsent:  m.IsAbsent,
		}

		/*
			TODO(gmagnusson):
			for j, absent := range metric.IsAbsent {
				if absent {
					t.Values[i] = math.NaN
				}
			}
			and then remove Metric.IsAbsent
		*/

		metrics[i] = metric
	}

	return metrics, nil
}

// Info fetches metadata about a metric from a backend.
func (b Backend) Info(ctx context.Context, metric string) ([]types.Info, error) {
	u := b.URL("/info")
	u, body := carbonapiV2InfoEncoder(u, metric)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	infos, err := carbonapiV2InfoDecoder(resp)
	if err != nil {
		return nil, err
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

func carbonapiV2InfoDecoder(blob []byte) ([]types.Info, error) {
	s := &carbonapi_v2.Infos{}
	if err := s.Unmarshal(blob); err != nil {
		return nil, err
	}

	infos := make([]types.Info, len(s.Infos))
	for i, sInfo := range s.Infos {
		info := types.Info{
			Host:              s.Hosts[i],
			Name:              sInfo.Name,
			AggregationMethod: sInfo.AggregationMethod,
			MaxRetention:      sInfo.MaxRetention,
			XFilesFactor:      sInfo.XFilesFactor,
			Retentions:        make([]types.Retention, len(sInfo.Retentions)),
		}
		for j, inf := range sInfo.Retentions {
			info.Retentions[j] = types.Retention{
				SecondsPerPoint: inf.SecondsPerPoint,
				NumberOfPoints:  inf.NumberOfPoints,
			}
		}

		infos[i] = info
	}

	return infos, nil
}

// Find resolves globs and finds metrics in a backend.
func (b Backend) Find(ctx context.Context, query string) ([]types.Match, error) {
	u := b.URL("/metrics/find")
	u, body := carbonapiV2FindEncoder(u, query)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	find, err := carbonapiV2FindDecoder(resp)

	return find, err
}

func carbonapiV2FindEncoder(u *url.URL, query string) (*url.URL, io.Reader) {
	vals := url.Values{
		"query":  []string{query},
		"format": fmtProto,
	}
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2FindDecoder(blob []byte) ([]types.Match, error) {
	f := &carbonapi_v2.Matches{}

	if err := f.Unmarshal(blob); err != nil {
		return nil, err
	}

	matches := make([]types.Match, len(f.Matches))
	for i, match := range f.Matches {
		matches[i] = types.Match{
			Path:   match.Path,
			IsLeaf: match.IsLeaf,
		}
	}

	return matches, nil
}
