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
	"strings"

	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Backend codifies the RPC calls a Graphite backend responds to.
type Backend interface {
	Find(context.Context, string) ([]types.Match, error)
	Info(context.Context, string) ([]types.Info, error)
	Render(context.Context, int32, int32, []string) ([]types.Metric, error)

	Contains([]string) bool // Reports whether a backend contains any of the given targets.
	Logger() *zap.Logger    // A logger used to communicate non-fatal warnings.
	Probe()                 // Probe updates internal state of the backend.
}

// TODO(gmagnusson): ^ Remove IsAbsent: IsAbsent[i] => Values[i] == NaN
// Doing math on NaN is expensive, but assuming that all functions will treat a
// default value of 0 intelligently is wrong (see multiplication). Thus math
// needs an if IsAbsent[i] check anyway, which is also expensive if we're
// worrying about those levels of performance in the first place.

// Renders makes Render calls to multiple backends.
func Renders(ctx context.Context, backends []Backend, from int32, until int32, targets []string) ([]types.Metric, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Metric, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		go func(b Backend) {
			msg, err := b.Render(ctx, from, until, targets)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([][]types.Metric, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if err := checkErrs(ctx, errs, len(backends), backends[0].Logger()); err != nil {
		return nil, err
	}

	return types.MergeMetrics(msgs), nil
}

// Infos makes Info calls to multiple backends.
func Infos(ctx context.Context, backends []Backend, metric string) ([]types.Info, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Info, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		go func(b Backend) {
			msg, err := b.Info(ctx, metric)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([][]types.Info, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if err := checkErrs(ctx, errs, len(backends), backends[0].Logger()); err != nil {
		return nil, err
	}

	return types.MergeInfos(msgs), nil
}

// Finds makes Find calls to multiple backends.
func Finds(ctx context.Context, backends []Backend, query string) ([]types.Match, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Match, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		go func(b Backend) {
			msg, err := b.Find(ctx, query)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([][]types.Match, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if err := checkErrs(ctx, errs, len(backends), backends[0].Logger()); err != nil {
		return nil, err
	}

	return types.MergeMatches(msgs), nil
}

// Filter filters the given backends by whether they Contain() the given targets.
func Filter(backends []Backend, targets []string) []Backend {
	bs := make([]Backend, 0)
	for _, b := range backends {
		if b.Contains(targets) {
			bs = append(bs, b)
		}
	}

	return bs
}

func checkErrs(ctx context.Context, errs []error, limit int, logger *zap.Logger) error {
	if len(errs) == 0 {
		return nil
	}

	if len(errs) >= limit {
		return errors.WithMessage(combineErrors(errs), "All backend requests failed")
	}

	logger.Warn("Some requests failed",
		zap.String("uuid", util.GetUUID(ctx)),
		zap.Error(combineErrors(errs)),
	)

	return nil
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
