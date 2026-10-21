package carbonapi

import (
	"context"
	"sort"
	"time"

	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/util"
	"go.uber.org/zap"
)

// A fetch has one reader: the request handler. Backend workers only send to its
// buffered channel, so dispatching later targets cannot block earlier replies.
type renderMetricFetch struct {
	replies   chan RenderResponse
	responses []RenderResponse
	done      bool
}

func (f *renderMetricFetch) collect() []RenderResponse {
	if !f.done {
		f.responses = make([]RenderResponse, cap(f.replies))
		for i := range f.responses {
			f.responses[i] = <-f.replies
		}
		close(f.replies)
		f.replies = nil
		f.done = true
	}
	return f.responses
}

type renderPrefetch map[parser.MetricRequest]*renderMetricFetch
type renderPrefetchKey struct{}

// Prefetch independent top-level targets while preserving sequential evaluation,
// shared metric storage, error handling and response order. Glob expansion keeps
// its existing demand-driven path; it can produce many individual requests.
func (app *App) prefetchRenderTargets(ctx context.Context, expressions []parser.Expr,
	from, until int32, toLog *carbonapipb.AccessLogDetails, lg *zap.Logger) (context.Context, renderPrefetch) {
	if app.config.ResolveGlobs != 0 || len(expressions) < 2 {
		return ctx, nil
	}
	pending := make(renderPrefetch)
	for _, expression := range expressions {
		if expression == nil {
			continue
		}
		for _, m := range expression.Metrics() {
			request := m
			request.From += from
			request.Until += until
			if _, exists := pending[request]; exists {
				continue
			}
			pending[request] = app.enqueueRenderRequests(ctx, request, []string{m.Metric}, toLog, lg)
		}
	}
	return context.WithValue(ctx, renderPrefetchKey{}, pending), pending
}

func (p renderPrefetch) drain() {
	for _, fetch := range p {
		fetch.collect()
	}
}

func (app *App) enqueueRenderRequests(ctx context.Context, mfetch parser.MetricRequest,
	renderRequests []string, toLog *carbonapipb.AccessLogDetails, lgm *zap.Logger) *renderMetricFetch {
	renderRequestContext := ctx
	subrequestCount := len(renderRequests)
	Trace(lgm, "got sub-requests. sending them upstream", zap.Int("sub-requests", subrequestCount))
	stableBatch := app.stableRenderBatch && app.config.RenderBatchSize > 1
	batchLimit := 1
	if stableBatch {
		sort.Strings(renderRequests)
		batchLimit = max(1, min(app.config.RenderBatchSize, app.config.MaxConcurrentUpstreamRequests))
	}
	if subrequestCount > 1 {
		renderRequestContext = util.WithPriority(ctx, subrequestCount)
		if !stableBatch {
			renderRequestContext = util.WithRenderBatch(renderRequestContext, app.config.RenderBatchSize)
		}
	}
	app.ms.UpstreamSubRenderNum.Observe(float64(subrequestCount))
	rch := make(chan RenderResponse, len(renderRequests))
	for index := 0; index < len(renderRequests); {
		end := index + 1
		if stableBatch && literalRenderPath(renderRequests[index]) {
			for end < min(index+batchLimit, len(renderRequests)) && literalRenderPath(renderRequests[end]) && renderRequests[end] != renderRequests[end-1] {
				end++
			}
		}
		// This blocks when the queue fills up, which is fine as the below result read would block anyway.
		//
		// TODO: Maybe handle record drops when the queue is full.
		req := &RenderReq{
			Path:  renderRequests[index],
			From:  mfetch.From,
			Until: mfetch.Until,

			Ctx:       renderRequestContext,
			ToLog:     toLog,
			StartTime: time.Now(),

			Results: rch,
		}
		if end-index > 1 {
			req.Paths = renderRequests[index:end]
		}

		if subrequestCount > app.config.LargeReqSize {
			app.slowQ <- req
			app.ms.UpstreamEnqueuedRequests.WithLabelValues("slow").Add(float64(end - index))
			app.ms.UpstreamRequestsInQueue.WithLabelValues("slow").Add(float64(end - index))
		} else {
			app.fastQ <- req
			app.ms.UpstreamEnqueuedRequests.WithLabelValues("fast").Add(float64(end - index))
			app.ms.UpstreamRequestsInQueue.WithLabelValues("fast").Add(float64(end - index))
		}
		index = end
	}
	return &renderMetricFetch{replies: rch}
}
