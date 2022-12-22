package backend

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// The backend to send requests to.
type Backend struct {
	BackendImpl

	// Request queues.
	// There's a separate one for each type, because the stampeding requests of one type should not
	// impact the other types of requests.
	// This, for example, removes the dependency between the find and render requests performance.
	// A big wave of render requests will not have direct impact on the find requests besides through the semphore.
	fastRenderQ chan *renderReq
	slowRenderQ chan *renderReq
	findQ       chan *findReq
	infoQ       chan *infoReq // the info requests are expected to be relatively rare

	// The size of the requests semaphore:
	// The maximum number of simultaneous requests.
	semaSize int

	requestsInQueue  *prometheus.GaugeVec
	saturation       prometheus.Gauge
	timeInQSec       *prometheus.HistogramVec
	enqueuedRequests *prometheus.CounterVec
	backendDuration  prometheus.ObserverVec
}

// The specific backend implementation.
//
// At the moment of writing, it can be one of:
// * net, the standard protocol
// * grpc, used for streaming gRPC
type BackendImpl interface {
	Find(context.Context, types.FindRequest) (types.Matches, error)
	Info(context.Context, types.InfoRequest) ([]types.Info, error)
	Render(context.Context, types.RenderRequest) ([]types.Metric, error)

	Contains([]string) bool // Reports whether a backend contains any of the given targets.
	Logger() *zap.Logger    // A logger used to communicate non-fatal warnings.
	GetServerAddress() string
}

type renderReq struct {
	types.RenderRequest

	Ctx       context.Context
	StartTime time.Time

	Results chan []types.Metric
	Errors  chan error
}

type findReq struct {
	types.FindRequest

	Ctx       context.Context
	StartTime time.Time

	Results chan types.Matches
	Errors  chan error
}

type infoReq struct {
	types.InfoRequest

	Ctx       context.Context
	StartTime time.Time

	Results chan []types.Info
	Errors  chan error
}

// Creates a new backend and starts processing the queues
func NewBackend(impl BackendImpl, qSize int, semaSize int,
	requestsInQueue *prometheus.GaugeVec,
	saturation prometheus.Gauge,
	timeInQSec *prometheus.HistogramVec,
	enqueuedRequests *prometheus.CounterVec,
	backendDuration prometheus.ObserverVec) Backend {
	b := Backend{
		BackendImpl: impl,

		fastRenderQ: make(chan *renderReq, qSize),
		slowRenderQ: make(chan *renderReq, qSize),
		findQ:       make(chan *findReq, qSize),
		infoQ:       make(chan *infoReq, qSize),
		semaSize:    semaSize,

		requestsInQueue:  requestsInQueue,
		saturation:       saturation,
		timeInQSec:       timeInQSec,
		enqueuedRequests: enqueuedRequests,
		backendDuration:  backendDuration,
	}

	b.Proc()

	return b
}

// Process the requests in the queue.
// Should not be called when async processing is disabled.
// Expects the metrics to be non-nil.
func (b *Backend) Proc() {
	semaphore := make(chan bool, b.semaSize)

	// The duplication below is the simplest solution at the moment without adding dynamic typing.
	// After https://github.com/golang/go/issues/48522 is closed, we can use generics to avoid duplication.
	// Without that issue resolved, the use of generics would produce too much boilerplate and would look much worse than
	// the solution below — I've tried.
	go func() {
		for {
			var r *renderReq
			select {
			case r = <-b.fastRenderQ:
			default:
				select {
				case r = <-b.fastRenderQ:
				case r = <-b.slowRenderQ:
				}
			}

			requestLabel := "render"
			b.requestsInQueue.WithLabelValues(requestLabel).Dec()
			select {
			case <-r.Ctx.Done():
				r.Errors <- r.Ctx.Err()
				continue
			default:
			}
			semaphore <- true
			b.saturation.Inc()
			b.timeInQSec.WithLabelValues(requestLabel).Observe(float64(time.Since(r.StartTime)))
			go func(req *renderReq) {
				t := prometheus.NewTimer(b.backendDuration.WithLabelValues("render"))
				res, err := b.BackendImpl.Render(req.Ctx, req.RenderRequest)
				t.ObserveDuration()

				if err != nil {
					req.Errors <- err
				} else {
					req.Results <- res
				}
				<-semaphore
				b.saturation.Dec()
			}(r)
		}
	}()
	go func() {
		for r := range b.findQ {
			requestLabel := "find"
			b.requestsInQueue.WithLabelValues(requestLabel).Dec()
			semaphore <- true
			b.saturation.Inc()
			b.timeInQSec.WithLabelValues(requestLabel).Observe(float64(time.Since(r.StartTime)))
			go func(req *findReq) {
				t := prometheus.NewTimer(b.backendDuration.WithLabelValues(requestLabel))
				res, err := b.BackendImpl.Find(req.Ctx, req.FindRequest)
				t.ObserveDuration()
				if err != nil {
					req.Errors <- err
				} else {
					req.Results <- res
				}
				<-semaphore
				b.saturation.Dec()
			}(r)
		}
	}()
	go func() {
		for r := range b.infoQ {
			b.requestsInQueue.WithLabelValues("info").Dec()
			semaphore <- true
			b.saturation.Inc()
			// not adding time in queue histogram for info requests to reduce the number of exposed metrics
			go func(req *infoReq) {
				// not adding duration histogram for info requests to reduce the number of exposed metrics
				res, err := b.BackendImpl.Info(req.Ctx, req.InfoRequest)
				if err != nil {
					req.Errors <- err
				} else {
					req.Results <- res
				}
				<-semaphore
				b.saturation.Dec()
			}(r)
		}
	}()
}

// The duplication below is the simplest solution at the moment without adding dynamic typing.
// After https://github.com/golang/go/issues/48522 is closed, we can use generics to avoid duplication.
// Without that issue resolved, the use of generics would produce too much boilerplate and would look much worse than
// the solution below — I've tried.

func (backend Backend) SendRender(ctx context.Context, request types.RenderRequest, msgCh chan []types.Metric, errCh chan error) {
	if util.GetPriority(ctx) < 10000 {
		backend.fastRenderQ <- &renderReq{
			RenderRequest: request,
			Ctx:           ctx,
			StartTime:     time.Now(),
			Results:       msgCh,
			Errors:        errCh,
		}
	} else {
		backend.slowRenderQ <- &renderReq{
			RenderRequest: request,
			Ctx:           ctx,
			StartTime:     time.Now(),
			Results:       msgCh,
			Errors:        errCh,
		}
	}
	backend.requestsInQueue.WithLabelValues("render").Inc()
	backend.enqueuedRequests.WithLabelValues("render").Inc()
}

func (backend Backend) SendFind(ctx context.Context, request types.FindRequest,
	msgCh chan types.Matches, errCh chan error, durationHist *prometheus.HistogramVec) {
	backend.findQ <- &findReq{
		FindRequest: request,
		Ctx:         ctx,
		StartTime:   time.Now(),
		Results:     msgCh,
		Errors:      errCh,
	}
	backend.requestsInQueue.WithLabelValues("find").Inc()
	backend.enqueuedRequests.WithLabelValues("find").Inc()
}

func (backend Backend) SendInfo(ctx context.Context, request types.InfoRequest, msgCh chan []types.Info, errCh chan error) {
	backend.infoQ <- &infoReq{
		InfoRequest: request,
		Ctx:         ctx,
		StartTime:   time.Now(),
		Results:     msgCh,
		Errors:      errCh,
	}
	backend.requestsInQueue.WithLabelValues("info").Inc()
	backend.enqueuedRequests.WithLabelValues("info").Inc()
}
