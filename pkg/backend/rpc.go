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
	replicaMismatchConfig cfg.RenderReplicaMismatchConfig, lg *zap.Logger) ([]types.Metric, types.MetricRenderStats, []error) {
	if len(backends) == 0 {
		return nil, types.MetricRenderStats{}, nil
	}

	groups, membership := newRenderGroups(backends)
	for i, backend := range backends {
		g := groups[membership[i]]
		backend.SendRender(ctx, request, g.results, g.errors)
	}

	fetches := []renderFetch{{request: request, groups: groups}}
	collectRenderFetches(fetches, lg)
	retryUnreachable(ctx, fetches, lg)
	metrics, stats := types.MergeMetrics(fetches[0].messages, replicaMismatchConfig, lg)
	return metrics, stats, renderGroupErrors(groups)
}

// Infos makes Info calls to multiple backends. A store's connect failure
// counts as not-found when its cluster answered not-found (lookupGroupErrors).
func Infos(ctx context.Context, backends []Backend, request types.InfoRequest) ([]types.Info, []error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgs, errs := fanOut(backends, types.ErrInfoNotFound, func(b Backend, results chan []types.Info, errs chan error) {
		b.SendInfo(ctx, request, results, errs)
	})
	return types.MergeInfos(msgs), errs
}

// Finds makes Find calls to multiple backends. A store's connect failure
// counts as not-found when its cluster answered not-found (lookupGroupErrors).
func Finds(ctx context.Context, backends []Backend, request types.FindRequest, durationHist *prometheus.HistogramVec) (types.Matches, []error) {
	if len(backends) == 0 {
		return types.Matches{}, nil
	}

	msgs, errs := fanOut(backends, types.ErrMatchesNotFound, func(b Backend, results chan types.Matches, errs chan error) {
		b.SendFind(ctx, request, results, errs, durationHist)
	})
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
	var bs []Backend
	for _, b := range backends {
		if b.Contains(targets) {
			bs = append(bs, b)
		}
	}

	return bs
}
