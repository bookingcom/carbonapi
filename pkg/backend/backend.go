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
	"net/url"
	"strings"
	"sync"

	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Backend interface {
	Call(context.Context, *url.URL, io.Reader) ([]byte, error)
	URL(string) *url.URL
	Logger() *zap.Logger
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
func ScatterGather(ctx context.Context, backends []Backend, u *url.URL, body io.Reader) ([][]byte, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	resps := make([][]byte, len(backends))
	errs := make([]error, len(backends))

	wg := sync.WaitGroup{}
	for i, backend := range backends {
		wg.Add(1)

		// yikes
		v := backend.URL(u.Path)
		v.Opaque = u.Opaque
		v.User = u.User
		v.RawQuery = u.RawQuery

		go func(j int, b Backend, w *url.URL) {
			resps[j], errs[j] = b.Call(ctx, w, body)
			wg.Done()
		}(i, backend, v)
	}
	wg.Wait()

	responses := make([][]byte, 0, len(backends))
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
		backends[0].Logger().Warn("Some requests failed",
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(combineErrors(errs)),
		)
	}

	return responses, nil
}
