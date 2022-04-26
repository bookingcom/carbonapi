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
	"github.com/bookingcom/carbonapi/cfg"

	"github.com/bookingcom/carbonapi/pkg/types"

	"go.uber.org/zap"
)

// Backend codifies the RPC calls a Graphite backend responds to.
type Backend interface {
	Find(context.Context, types.FindRequest) (types.Matches, error)
	Info(context.Context, types.InfoRequest) ([]types.Info, error)
	Render(context.Context, types.RenderRequest) ([]types.Metric, error)

	Contains([]string) bool // Reports whether a backend contains any of the given targets.
	Logger() *zap.Logger    // A logger used to communicate non-fatal warnings.
	GetServerAddress() string
}

// TODO(gmagnusson): ^ Remove IsAbsent: IsAbsent[i] => Values[i] == NaN
// Doing math on NaN is expensive, but assuming that all functions will treat a
// default value of 0 intelligently is wrong (see multiplication). Thus math
// needs an if IsAbsent[i] check anyway, which is also expensive if we're
// worrying about those levels of performance in the first place.

// Renders makes Render calls to multiple backends.
// replicaMatchMode indicates how data points of the metrics fetched from replicas
// will be checked and applied on the final metrics. replicaMismatchReportLimit limits
// the number of mismatched metrics reported in log for each render request.
func Renders(
	ctx context.Context,
	backends []Backend,
	request types.RenderRequest,
	replicaMismatchConfig cfg.RenderReplicaMismatchConfig,
	logger *zap.Logger) ([]types.Metric, types.MetricRenderStats, []error) {
	if len(backends) == 0 {
		return nil, types.MetricRenderStats{}, nil
	}

	msgCh := make(chan []types.Metric, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		request.IncCall()
		go func(b Backend) {
			msg, err := b.Render(ctx, request)
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

	metrics, stats := types.MergeMetrics(msgs, replicaMismatchConfig, logger)
	return metrics, stats, errs
}

// Infos makes Info calls to multiple backends.
func Infos(ctx context.Context, backends []Backend, request types.InfoRequest) ([]types.Info, []error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Info, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		request.IncCall()
		go func(b Backend) {
			msg, err := b.Info(ctx, request)
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

	return types.MergeInfos(msgs), errs
}

// Finds makes Find calls to multiple backends.
func Finds(ctx context.Context, backends []Backend, request types.FindRequest) (types.Matches, []error) {
	if len(backends) == 0 {
		return types.Matches{}, nil
	}

	msgCh := make(chan types.Matches, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		request.IncCall()
		go func(b Backend) {
			msg, err := b.Find(ctx, request)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([]types.Matches, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	return types.MergeMatches(msgs), errs
}

// Filter filters the given backends by whether they Contain() the given targets.
func Filter(backends []Backend, targets []string) []Backend {
	if bs := filter(backends, targets); len(bs) > 0 {
		return bs
	}
	return backends
}

func filter(backends []Backend, targets []string) []Backend {
	bs := make([]Backend, 0)
	for _, b := range backends {
		if b.Contains(targets) {
			bs = append(bs, b)
		}
	}

	return bs
}
