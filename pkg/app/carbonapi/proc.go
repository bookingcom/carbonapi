package carbonapi

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"go.uber.org/zap"
)

// ProcessRequests processes the queued requests.
func ProcessRequests(app *App, lg *zap.Logger) {
	// semaphore does what semaphores do: It limits the number of concurrent requests.
	semaphore := make(chan bool, app.config.MaxConcurrentUpstreamRequests)
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

				app.ms.UpstreamRequestsInQueue.WithLabelValues(label).Dec()

				select {
				case <-req.Ctx.Done():
					req.Results <- RenderResponse{nil, req.Ctx.Err()}
					continue
				default:
				}

				semaphore <- true
				app.ms.UpstreamSemaphoreSaturation.Inc()
				app.ms.UpstreamTimeInQSec.WithLabelValues(label).Observe(float64(time.Since(req.StartTime).Seconds()))

				go func(r *RenderReq) {
					r.Results <- sendRenderRequest(app, r.Ctx, r.Path, r.From, r.Until, r.ToLog, lg)

					<-semaphore
					app.ms.UpstreamSemaphoreSaturation.Dec()
				}(req)
			}
		}()
	}
}

// RenderReq represents a render requests in the processing queue.
type RenderReq struct {
	Path  string
	From  int32
	Until int32

	Ctx       context.Context
	ToLog     *carbonapipb.AccessLogDetails
	StartTime time.Time

	Results chan RenderResponse
}
