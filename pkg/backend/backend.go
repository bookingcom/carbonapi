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

	// Children of the vectors above for each request type, resolved once
	// instead of hashing the label values on every request.
	render, find, info requestMetrics

	// Shared by every metric this backend returns; it is never modified.
	sourceClusters []string
}

type requestMetrics struct {
	inQueue  prometheus.Gauge
	timeInQ  prometheus.Observer
	enqueued prometheus.Counter
	duration prometheus.Observer
}

func newRequestMetrics(b *Backend, label string) requestMetrics {
	var m requestMetrics
	if b.requestsInQueue != nil {
		m.inQueue = b.requestsInQueue.WithLabelValues(label)
	}
	if b.timeInQSec != nil {
		m.timeInQ = b.timeInQSec.WithLabelValues(label)
	}
	if b.enqueuedRequests != nil {
		m.enqueued = b.enqueuedRequests.WithLabelValues(label)
	}
	if b.backendDuration != nil {
		m.duration = b.backendDuration.WithLabelValues(label)
	}
	return m
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
	BackendInfo() (addr string, cluster string, dc string)
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
	b.render = newRequestMetrics(&b, "render")
	b.find = newRequestMetrics(&b, "find")
	b.info = newRequestMetrics(&b, "info")
	_, cluster, _ := impl.BackendInfo()
	b.sourceClusters = []string{cluster}

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

			b.render.inQueue.Dec()
			select {
			case <-r.Ctx.Done():
				r.Errors <- r.Ctx.Err()
				continue
			default:
			}
			semaphore <- true
			b.saturation.Inc()
			b.render.timeInQ.Observe(float64(time.Since(r.StartTime).Seconds()))
			go func(req *renderReq) {
				t := prometheus.NewTimer(b.render.duration)
				res, err := b.BackendImpl.Render(req.Ctx, req.RenderRequest)
				t.ObserveDuration()

				if err != nil {
					req.Errors <- err
				} else {
					b.addSourceMetaToMetrics(res)
					req.Results <- res
				}
				<-semaphore
				b.saturation.Dec()
			}(r)
		}
	}()
	go func() {
		for r := range b.findQ {
			b.find.inQueue.Dec()
			semaphore <- true
			b.saturation.Inc()
			b.find.timeInQ.Observe(float64(time.Since(r.StartTime).Seconds()))
			go func(req *findReq) {
				t := prometheus.NewTimer(b.find.duration)
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
			b.info.inQueue.Dec()
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
	backend.render.inQueue.Inc()
	backend.render.enqueued.Inc()
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
	backend.find.inQueue.Inc()
	backend.find.enqueued.Inc()
}

func (backend Backend) SendInfo(ctx context.Context, request types.InfoRequest, msgCh chan []types.Info, errCh chan error) {
	backend.infoQ <- &infoReq{
		InfoRequest: request,
		Ctx:         ctx,
		StartTime:   time.Now(),
		Results:     msgCh,
		Errors:      errCh,
	}
	backend.info.inQueue.Inc()
	backend.info.enqueued.Inc()
}

func (backend Backend) addSourceMetaToMetrics(metrics []types.Metric) {
	for i := range metrics {
		metrics[i].SourceClusters = backend.sourceClusters
	}
}
