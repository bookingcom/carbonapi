package backend

import (
	"context"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bookingcom/carbonapi/pkg/types"

	"go.uber.org/zap"
)

// Renders makes Render calls to multiple backends.
// replicaMatchMode indicates how data points of the metrics fetched from replicas
// will be checked and applied on the final metrics. replicaMismatchReportLimit limits
// the number of mismatched metrics reported in log for each render request.
func Renders(ctx context.Context, backends []Backend, request types.RenderRequest,
	replicaMismatchConfig cfg.RenderReplicaMismatchConfig,
	lg *zap.Logger) ([]types.Metric, types.MetricRenderStats, []error) {
	if len(backends) == 0 {
		return nil, types.MetricRenderStats{}, nil
	}

	msgCh := make(chan []types.Metric, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		request.IncCall()
		backend.SendRender(ctx, request, msgCh, errCh)
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

	metrics, stats := types.MergeMetrics(msgs, replicaMismatchConfig, lg)
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
		backend.SendInfo(ctx, request, msgCh, errCh)
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
func Finds(ctx context.Context, backends []Backend, request types.FindRequest, durationHist *prometheus.HistogramVec) (types.Matches, []error) {
	if len(backends) == 0 {
		return types.Matches{}, nil
	}

	msgCh := make(chan types.Matches, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		request.IncCall()
		backend.SendFind(ctx, request, msgCh, errCh, durationHist)
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
func Filter(backends []Backend, targets []string) ([]Backend, bool) {
	if bs := filter(backends, targets); len(bs) > 0 {
		return bs, true
	}
	return backends, false
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
