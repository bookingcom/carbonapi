package carbonapi

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
)

// ProcessRequests processes the queued requests.
func ProcessRequests(app *App) {
	// semaphore does what semaphores do: It limits the number of concurrent requests.
	semaphore := make(chan bool, app.config.MaxConcurrentUpstreamRequests)
	for i := 0; i < app.config.ProcWorkers; i++ {
		go func() {
			for {
				var req *renderReq
				var qLabel string

				// During processing we use two independent queues that share the semaphore:
				// fastQ includes regular requests while slowQ contains large requests.
				//
				// Large requests could stampede a queue for a long time if we only had a single one. This would prevent any new requests
				// to be processed. Two queues guarantee that we still process incoming requests while a large request is in progress.
				//
				// Both queues have equal chances to be processed, but since large requests have much more sub-requests, this
				// effectively increases priority of the smaller requests.
				select {
				case req = <-app.fastQ:
					qLabel = "fast"
				case req = <-app.slowQ:
					qLabel = "slow"
				}

				app.ms.UpstreamRequestsInQueue.WithLabelValues(qLabel).Dec()

				// The use of the atomic load here is a for future safety.
				// Currently, it's not strictly necessary.
				dl := atomic.LoadInt64(&req.DeadlineMicro)
				if dl != 0 && dl < time.Now().UnixMicro() {
					app.ms.UpstreamTimeInQSec.WithLabelValues(qLabel).Observe(float64(time.Now().Sub(req.StartTime).Seconds()))
					app.ms.UpstreamTimeouts.WithLabelValues(qLabel, "render")
					continue
				}

				semaphore <- true
				app.ms.UpstreamSemaphoreSaturation.Inc()
				app.ms.UpstreamTimeInQSec.WithLabelValues(qLabel).Observe(float64(time.Now().Sub(req.StartTime).Seconds()))

				go func(r *renderReq) {
					r.Results <- app.sendRenderRequest(r.Ctx, r.Path, r.From, r.Until, r.ToLog)

					<-semaphore
					app.ms.UpstreamSemaphoreSaturation.Dec()
				}(req)
			}
		}()
	}
}

// renderReq represents a render requests in the processing queue.
type renderReq struct {
	Path  string
	From  int32
	Until int32

	Ctx           context.Context
	ToLog         *carbonapipb.AccessLogDetails
	StartTime     time.Time
	DeadlineMicro int64

	Results chan renderResponse
}
