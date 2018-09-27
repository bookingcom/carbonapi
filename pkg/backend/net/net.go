// Package net implements a backend that communicates over a network.
// It uses HTTP and protocol buffers for communication.
package net

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Backend represents a host that accepts requests for metrics over HTTP.
type Backend struct {
	address string
	scheme  string
	client  *http.Client
	timeout time.Duration
	limiter chan struct{}
	logger  *zap.Logger

	tlds  map[string]struct{}
	mutex *sync.Mutex
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
func New(cfg Config) (*Backend, error) {
	b := &Backend{
		mutex: new(sync.Mutex),
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
func (b Backend) call(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()

	req, err := b.request(ctx, u, body)
	if err != nil {
		return nil, err
	}

	return b.do(ctx, req)
}

// Probe performs a single update of the backend's top-level domains.
func (b *Backend) Probe() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	matches, err := b.Find(ctx, "*")
	if err != nil {
		return
	}

	tlds := make(map[string]struct{})
	for _, m := range matches.Matches {
		tlds[m.Path] = struct{}{}
	}

	b.mutex.Lock()
	b.tlds = tlds
	b.mutex.Unlock()
}

// Contains reports whether the backend contains any of the given targets.
func (b Backend) Contains(targets []string) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.tlds) == 0 {
		return true
	}

	for _, target := range targets {
		parts := strings.SplitN(target, ".", 2)
		part := parts[0]

		if strings.ContainsAny(part, "*{}[]") {
			// NOTE(gmagnusson): Just assume we contain whatever this is if it
			// has wildcards and let the stores figure it out.
			//
			// If we want to be more clever about this, we have to start
			// worrying about first expanding {} pairs and then (mostly, kind
			// of) regex matching the rest, and it just sounds like we're so
			// far into diminishing returns by then that we shouldn't bother.
			return true
		}

		if _, ok := b.tlds[part]; ok {
			return true
		}
	}

	return false
}

// Render fetches raw metrics from a backend.
func (b Backend) Render(ctx context.Context, from int32, until int32, targets []string) ([]types.Metric, error) {
	u := b.url("/render")
	u, body := carbonapiV2RenderEncoder(u, from, until, targets)

	resp, err := b.call(ctx, u, body)
	if err != nil {
		return nil, errors.Wrap(err, "HTTP call failed")
	}

	metrics, err := carbonapi_v2.RenderDecoder(resp)
	if err != nil {
		return metrics, errors.Wrap(err, "Protobuf unmarshal failed")
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
func (b Backend) Info(ctx context.Context, metric string) ([]types.Info, error) {
	u := b.url("/info")
	u, body := carbonapiV2InfoEncoder(u, metric)

	resp, err := b.call(ctx, u, body)
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
func (b Backend) Find(ctx context.Context, query string) (types.Matches, error) {
	u := b.url("/metrics/find")
	u, body := carbonapiV2FindEncoder(u, query)

	resp, err := b.call(ctx, u, body)
	if err != nil {
		return types.Matches{}, errors.Wrap(err, "HTTP call failed")
	}

	find, err := carbonapi_v2.FindDecoder(resp)
	if err != nil {
		return find, errors.Wrap(err, "Protobuf unmarshal failed")
	}

	return find, nil
}

func carbonapiV2FindEncoder(u *url.URL, query string) (*url.URL, io.Reader) {
	vals := url.Values{
		"query":  []string{query},
		"format": fmtProto,
	}
	u.RawQuery = vals.Encode()

	return u, nil
}
