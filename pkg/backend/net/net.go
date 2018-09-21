package net

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

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
