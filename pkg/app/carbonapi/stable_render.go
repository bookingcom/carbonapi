package carbonapi

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	exprtypes "github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/tldcache"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

func literalRenderPath(path string) bool { return !strings.ContainsAny(path, "*?[]{}\\") }

func sendGroupedRenderRequests(app *App, ctx context.Context, paths []string, from, until int32,
	toLog *carbonapipb.AccessLogDetails, lg *zap.Logger) []RenderResponse {
	atomic.AddInt64(&toLog.ZipperRequests, int64(len(paths)))
	app.ms.UpstreamRequests.WithLabelValues("render").Add(float64(len(paths)))
	started := time.Now()
	requests := make([]types.RenderRequest, len(paths))
	choices := make([][]backend.Backend, len(paths))
	responses := make([]RenderResponse, len(paths))
	for i, path := range paths {
		requests[i] = types.NewRenderRequest([]string{path}, from, until)
		bs, miss := tldcache.FilterBackendByTopLevelDomain(app.TopLevelDomainCache, app.TopLevelDomainPrefixes, app.Backends, requests[i].Targets)
		if app.NotFoundWhenTLDCacheMiss && miss {
			responses[i].error = types.ErrNotFound(fmt.Sprintf("%s: %s", tldcache.TLDCacheMissErr, path))
			continue
		}
		filtered, pathCache := backend.Filter(bs, requests[i].Targets)
		if pathCache {
			app.ZipperMetrics.PathCacheFilteredRequests.Inc()
		}
		choices[i] = filtered
	}
	replies := backend.RendersGrouped(ctx, choices, requests, app.ZipperConfig.RenderReplicaMismatchConfig, lg)
	for i, reply := range replies {
		app.ms.UpstreamDuration.WithLabelValues("render").Observe(time.Since(started).Seconds())
		if responses[i].error != nil {
			continue
		}
		ms := app.ZipperMetrics
		ms.Renders.Add(float64(reply.Stats.DataPointCount))
		ms.RenderMismatches.Add(float64(reply.Stats.MismatchCount))
		ms.RenderFixedMismatches.Add(float64(reply.Stats.FixedMismatchCount))
		responses[i].error = errorsFanIn(reply.Errors, len(choices[i]))
		if responses[i].error == nil && reply.Stats.MismatchCount > reply.Stats.FixedMismatchCount {
			ms.RenderMismatchedResponses.Inc()
		}
		responses[i].data = make([]*exprtypes.MetricData, 0, len(reply.Metrics))
		for _, metric := range reply.Metrics {
			responses[i].data = append(responses[i].data, &exprtypes.MetricData{Metric: metric})
		}
	}
	return responses
}
