/*
Package backend handles communication with Graphite backends.

Example use:

	server := New(Config{Address: "localhost:8080"})
	resp, _ := server.Call(ctx, "render/?target=foo", nil)
	// Do something with resp.Body

For binary payloads, use:

	server := New(Config{Address: "localhost:8080"})
	resp, _ := server.Call(ctx, "render", payload)

The package will transparently handle concurrent requests to multiple backends:

	var servers []backend
	resps, err := ScatterGather(ctx, servers, "render", payload)

The package does not know how to marshal requests or unmarshal responses.
For now, this is on purpose. As we learn to speak to more backends, use
different protocols, or different transports, this may need to change.
*/
package backend

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Backend represents a host that accepts requests for metrics.
type Backend struct {
	address string
	client  *http.Client
	timeout time.Duration
	limiter chan struct{}
	logger  *zap.Logger
}

// Config configures a backend.
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

// Response represents a backend response.
// If HTTP is non-nil, its Body will have been closed.
type Response struct {
	HTTP *http.Response
	Body []byte
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

func (b Backend) url(path string) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   b.address,
		Path:   path,
	}
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

func (b Backend) do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if err := b.enter(ctx); err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return resp, err
	}

	if err := b.leave(); err != nil {
		b.logger.Error("Backend limiter full",
			zap.String("host", b.address),
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(err),
		)
	}

	if resp.StatusCode != http.StatusOK {
		return resp, errors.Errorf("Bad response code %d", resp.StatusCode)
	}

	return resp, nil
}

// Call makes a call to a backend.
// If the backend timeout is positive, Call will override the context timeout
// with the backend timeout.
// Call ensures that the outgoing request has a UUID set.
func (b Backend) Call(ctx context.Context, u *url.URL, body io.Reader) (Response, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()

	req, err := b.request(ctx, u, body)
	if err != nil {
		return Response{}, err
	}

	resp, err := b.do(ctx, req)
	if err != nil {
		return Response{HTTP: resp}, err
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Response{HTTP: resp}, err
	}

	return Response{
		Body: respBody,
		HTTP: resp,
	}, nil
}

func combineErrors(errs []error) error {
	msgs := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			msgs = append(msgs, err.Error())
		}
	}

	if len(msgs) == 0 {
		return nil
	} else if len(msgs) == 1 {
		return errors.New(msgs[0])
	}

	return errors.Errorf("Multiple errors:\n%s", strings.Join(msgs, "\n"))
}

// ScatterGather makes concurrent Calls to multiple backends.
// A request is considered to have been successful if a single backend returned
// a successful request.
func ScatterGather(ctx context.Context, backends []Backend, u *url.URL, body io.Reader) ([]Response, error) {
	if len(backends) == 0 {
		return []Response{}, nil
	}

	resps := make([]Response, len(backends))
	errs := make([]error, len(backends))

	wg := sync.WaitGroup{}
	for i, backend := range backends {
		wg.Add(1)

		// yikes
		v := backend.url(u.Path)
		v.Opaque = u.Opaque
		v.User = u.User
		v.RawQuery = u.RawQuery

		go func(j int, b Backend, w *url.URL) {
			resps[j], errs[j] = b.Call(ctx, w, body)
			wg.Done()
		}(i, backend, v)
	}
	wg.Wait()

	responses := make([]Response, 0, len(backends))
	errcount := 0
	for i := 0; i < len(backends); i++ {
		if errs[i] == nil {
			responses = append(responses, resps[i])
		} else {
			errcount++
		}
	}

	if errcount > 0 {
		if errcount == len(backends) {
			return nil, errors.WithMessage(combineErrors(errs), "All backend requests failed")
		}

		// steal a logger
		backends[0].logger.Warn("Some requests failed",
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(combineErrors(errs)),
		)
	}

	return responses, nil
}
