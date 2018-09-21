/*
Package backend defines an interface and RPC methods for communication
with Graphite backends.

Example use:

    var b Backend
    metrics, err := Render(ctx, b, from, until, targets)

The package will transparently handle concurrent requests to multiple
backends:

    var bs []Backend
    metrics, err := Renders(ctx, bs, from, until, targets)
*/
package backend

import (
	"context"
	"io"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Backend interface {
	Call(context.Context, *url.URL, io.Reader) ([]byte, error)
	Logger() *zap.Logger
	Probe()
	URL(string) *url.URL
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
