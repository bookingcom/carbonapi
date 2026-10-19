package backend

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// The backend to send requests to.
//
// Backend values are copied a lot (filtered backend lists are built for every
// request), so everything except the implementation lives behind one pointer.
type Backend struct {
	BackendImpl
	*backendState
}

type backendState struct {
	// Request queues.
	// There's a separate one for each type, because the stampeding requests of one type should not
	// impact the other types of requests.
	// This, for example, removes the dependency between the find and render requests performance.
	// A big wave of render requests will not have direct impact on the find requests besides through the semphore.
	fastRenderQ *requestQueue[*renderReq]
	slowRenderQ *requestQueue[*renderReq]
	findQ       *requestQueue[*findReq]
	infoQ       *requestQueue[*infoReq] // the info requests are expected to be relatively rare

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

func newRequestMetrics(b *backendState, label string) requestMetrics {
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
	s := &backendState{
		fastRenderQ: newRequestQueue[*renderReq](qSize),
		slowRenderQ: newRequestQueue[*renderReq](qSize),
		findQ:       newRequestQueue[*findReq](qSize),
		infoQ:       newRequestQueue[*infoReq](qSize),
		semaSize:    semaSize,

		requestsInQueue:  requestsInQueue,
		saturation:       saturation,
		timeInQSec:       timeInQSec,
		enqueuedRequests: enqueuedRequests,
		backendDuration:  backendDuration,
	}
	s.render = newRequestMetrics(s, "render")
	s.find = newRequestMetrics(s, "find")
	s.info = newRequestMetrics(s, "info")
	_, cluster, _ := impl.BackendInfo()
	s.sourceClusters = []string{cluster}

	b := Backend{BackendImpl: impl, backendState: s}
	b.Proc()

	return b
}

// Process the requests in the queue.
// Should not be called when async processing is disabled.
// Expects the metrics to be non-nil.
func (b *Backend) Proc() {
	limiter := semaphore.NewWeighted(int64(b.semaSize))

	// The duplication below is the simplest solution at the moment without adding dynamic typing.
	// After https://github.com/golang/go/issues/48522 is closed, we can use generics to avoid duplication.
	// Without that issue resolved, the use of generics would produce too much boilerplate and would look much worse than
	// the solution below — I've tried.
	go func() {
		var pending *renderReq
		for {
			var r *renderReq
			if pending != nil {
				// A carried slow request must not hide newly arrived fast work.
				if util.GetPriority(pending.Ctx) >= 10000 {
					var ok bool
					if r, ok = b.fastRenderQ.tryRecv(); !ok {
						r, pending = pending, nil
					}
				} else {
					r, pending = pending, nil
				}
			} else {
				r = b.nextRender()
			}
			batch := []*renderReq{r}
			if pending == nil && r.Ctx.Err() == nil {
				batch, pending = b.collectRenderBatch(r)
			}
			weight := int64(len(batch))
			b.render.inQueue.Sub(float64(weight))
			if err := limiter.Acquire(r.Ctx, weight); err != nil {
				for _, req := range batch {
					req.Errors <- err
				}
				continue
			}
			b.saturation.Add(float64(weight))
			for _, req := range batch {
				b.render.timeInQ.Observe(time.Since(req.StartTime).Seconds())
			}
			go func(batch []*renderReq, weight int64) {
				defer limiter.Release(weight)
				defer b.saturation.Sub(float64(weight))
				b.renderBatch(batch)
			}(batch, weight)
		}
	}()
	go func() {
		for {
			r := b.findQ.recv()
			b.find.inQueue.Dec()
			if err := limiter.Acquire(r.Ctx, 1); err != nil {
				r.Errors <- err
				continue
			}
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
				limiter.Release(1)
				b.saturation.Dec()
			}(r)
		}
	}()
	go func() {
		for {
			r := b.infoQ.recv()
			b.info.inQueue.Dec()
			if err := limiter.Acquire(r.Ctx, 1); err != nil {
				r.Errors <- err
				continue
			}
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
				limiter.Release(1)
				b.saturation.Dec()
			}(r)
		}
	}()
}

// The duplication below is the simplest solution at the moment without adding dynamic typing.
// After https://github.com/golang/go/issues/48522 is closed, we can use generics to avoid duplication.
// Without that issue resolved, the use of generics would produce too much boilerplate and would look much worse than
// the solution below — I've tried.

// nextRender preserves the fast queue's priority without waiting to form a batch.
func (b Backend) nextRender() *renderReq {
	if b.fastRenderQ.direct != nil {
		select {
		case r := <-b.fastRenderQ.direct:
			return r
		default:
			select {
			case r := <-b.fastRenderQ.direct:
				return r
			case r := <-b.slowRenderQ.direct:
				return r
			}
		}
	}
	for {
		if r, ok := b.fastRenderQ.tryRecv(); ok {
			return r
		}
		select {
		case <-b.fastRenderQ.ready:
			if r, ok := b.fastRenderQ.tryRecv(); ok {
				return r
			}
		case <-b.slowRenderQ.ready:
			if r, ok := b.slowRenderQ.tryRecv(); ok {
				return r
			}
		}
	}
}

func (backend Backend) SendRender(ctx context.Context, request types.RenderRequest, msgCh chan []types.Metric, errCh chan error) {
	queue := backend.fastRenderQ
	if util.GetPriority(ctx) >= 10000 {
		queue = backend.slowRenderQ
	}
	r := &renderReq{RenderRequest: request, Ctx: ctx, StartTime: time.Now(), Results: msgCh, Errors: errCh}
	if queue.send(ctx, r) {
		backend.render.inQueue.Inc()
		backend.render.enqueued.Inc()
	} else {
		errCh <- ctx.Err()
	}
}

func (backend Backend) SendFind(ctx context.Context, request types.FindRequest,
	msgCh chan types.Matches, errCh chan error, durationHist *prometheus.HistogramVec) {
	backend.findQ.send(context.Background(), &findReq{
		FindRequest: request,
		Ctx:         ctx,
		StartTime:   time.Now(),
		Results:     msgCh,
		Errors:      errCh,
	})
	backend.find.inQueue.Inc()
	backend.find.enqueued.Inc()
}

func (backend Backend) SendInfo(ctx context.Context, request types.InfoRequest, msgCh chan []types.Info, errCh chan error) {
	backend.infoQ.send(context.Background(), &infoReq{
		InfoRequest: request,
		Ctx:         ctx,
		StartTime:   time.Now(),
		Results:     msgCh,
		Errors:      errCh,
	})
	backend.info.inQueue.Inc()
	backend.info.enqueued.Inc()
}

func (backend Backend) addSourceMetaToMetrics(metrics []types.Metric) {
	for i := range metrics {
		metrics[i].SourceClusters = backend.sourceClusters
	}
}
