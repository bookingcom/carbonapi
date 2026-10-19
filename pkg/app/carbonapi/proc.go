package carbonapi

import (
	"context"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"go.uber.org/zap"
)

// ProcessRequests processes the queued requests.
func ProcessRequests(app *App, lg *zap.Logger) {
	// semaphore does what semaphores do: It limits the number of concurrent requests.
	limiter := semaphore.NewWeighted(int64(app.config.MaxConcurrentUpstreamRequests))
	for i := 0; i < app.config.ProcWorkers; i++ {
		go func() {
			for {
				var req *RenderReq
				var label string

				// During processing we use two independent queues that share the semaphore:
				// fastQ includes regular requests while slowQ contains large requests.
				//
				// Large requests could stampede a queue for a long time if we only had a single one. This would prevent any new requests
				// to be processed. Two queues guarantee that we still process incoming requests while a large request is in progress.
				select {
				case req = <-app.fastQ: // "fast" (i.e. small) requests are prioritised
					label = "fast"
				default:
					select {
					case req = <-app.fastQ:
						label = "fast"
					case req = <-app.slowQ:
						label = "slow"
					}
				}

				weight := max(1, len(req.Paths))
				app.ms.UpstreamRequestsInQueue.WithLabelValues(label).Sub(float64(weight))

				select {
				case <-req.Ctx.Done():
					for range weight {
						req.Results <- RenderResponse{nil, req.Ctx.Err()}
					}
					continue
				default:
				}

				if err := limiter.Acquire(req.Ctx, int64(weight)); err != nil {
					for range weight {
						req.Results <- RenderResponse{nil, err}
					}
					continue
				}
				app.ms.UpstreamSemaphoreSaturation.Add(float64(weight))
				for range weight {
					app.ms.UpstreamTimeInQSec.WithLabelValues(label).Observe(time.Since(req.StartTime).Seconds())
				}

				go func(r *RenderReq) {
					if len(r.Paths) > 1 {
						for _, response := range sendGroupedRenderRequests(app, r.Ctx, r.Paths, r.From, r.Until, r.ToLog, lg) {
							r.Results <- response
						}
					} else {
						r.Results <- sendRenderRequest(app, r.Ctx, r.Path, r.From, r.Until, r.ToLog, lg)
					}
					limiter.Release(int64(weight))
					app.ms.UpstreamSemaphoreSaturation.Sub(float64(weight))
				}(req)
			}
		}()
	}
}

// RenderReq represents a render requests in the processing queue.
type RenderReq struct {
	Path  string
	Paths []string
	From  int32
	Until int32

	Ctx       context.Context
	ToLog     *carbonapipb.AccessLogDetails
	StartTime time.Time

	Results chan RenderResponse
}
