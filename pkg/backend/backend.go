package backend

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	"github.com/bookingcom/carbonapi/pkg/types"
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
	renderQ chan *renderReq
	findQ   chan *findReq
	infoQ   chan *infoReq // the info requests are expected to be relatively rare

	// The size of the requests semaphore:
	// The maximum number of simultaneous requests.
	semaSize int

	requestsInQueue  *prometheus.GaugeVec
	saturation       prometheus.Gauge
	timeInQSec       *prometheus.HistogramVec
	enqueuedRequests *prometheus.CounterVec
}

// The specific backend implementation.
//
// At the moment of writing, it can be one of:
// * mock, used for tests
// * net, the standard protocol
// * grpc, used for streaming gRPC
type BackendImpl interface {
	Find(context.Context, types.FindRequest) (types.Matches, error)
	Info(context.Context, types.InfoRequest) ([]types.Info, error)
	Render(context.Context, types.RenderRequest) ([]types.Metric, error)

	Contains([]string) bool // Reports whether a backend contains any of the given targets.
	Logger() *zap.Logger    // A logger used to communicate non-fatal warnings.
	GetServerAddress() string
	GetCluster() string
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

// Creates a new backend and starts processing the queues if qSize > 0.
func NewBackend(impl BackendImpl, qSize int, semaSize int,
	requestsInQueue *prometheus.GaugeVec,
	saturation prometheus.Gauge,
	timeInQSec *prometheus.HistogramVec,
	enqueuedRequests *prometheus.CounterVec) Backend {
	b := Backend{
		BackendImpl: impl,

		renderQ:  make(chan *renderReq, qSize),
		findQ:    make(chan *findReq, qSize),
		infoQ:    make(chan *infoReq, qSize),
		semaSize: semaSize,

		requestsInQueue:  requestsInQueue,
		saturation:       saturation,
		timeInQSec:       timeInQSec,
		enqueuedRequests: enqueuedRequests,
	}

	// The channel capacity is used to indicate enabling of the new processing.
	// When it is indicated as 0, the backend can only be used as a proxy to the implementation.
	if qSize > 0 {
		b.Proc()
	}

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
		for r := range b.renderQ {
			b.requestsInQueue.WithLabelValues("render").Dec()
			semaphore <- true
			b.saturation.Inc()
			b.timeInQSec.WithLabelValues("render").Observe(float64(time.Now().Sub(r.StartTime)))
			go func(req *renderReq) {
				res, err := b.BackendImpl.Render(req.Ctx, req.RenderRequest)
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
			b.requestsInQueue.WithLabelValues("find").Dec()
			semaphore <- true
			b.saturation.Inc()
			b.timeInQSec.WithLabelValues("find").Observe(float64(time.Now().Sub(r.StartTime)))
			go func(req *findReq) {
				res, err := b.BackendImpl.Find(req.Ctx, req.FindRequest)
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
			b.timeInQSec.WithLabelValues("info").Observe(float64(time.Now().Sub(r.StartTime)))
			go func(req *infoReq) {
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
	if cap(backend.renderQ) > 0 {
		backend.renderQ <- &renderReq{
			RenderRequest: request,
			Ctx:           ctx,
			StartTime:     time.Now(),
			Results:       msgCh,
			Errors:        errCh,
		}
		backend.requestsInQueue.WithLabelValues("render").Inc()
		backend.enqueuedRequests.WithLabelValues("render").Inc()
	} else {
		// This branch is only necessary to satisfy the tests.
		// After we clean-up and rework the tests to have mock metrics everywhere,
		// this should be removed.
		// TODO: Remove after tests cleanup.
		go func(b Backend) {
			msg, err := b.Render(ctx, request)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}
}

func (backend Backend) SendFind(ctx context.Context, request types.FindRequest,
	msgCh chan types.Matches, errCh chan error, durationHist *prometheus.HistogramVec) {
	if cap(backend.findQ) > 0 {
		backend.findQ <- &findReq{
			FindRequest: request,
			Ctx:         ctx,
			StartTime:   time.Now(),
			Results:     msgCh,
			Errors:      errCh,
		}
		backend.requestsInQueue.WithLabelValues("find").Inc()
		backend.enqueuedRequests.WithLabelValues("find").Inc()
	} else {
		// This branch is only necessary to satisfy the tests.
		// After we clean-up and rework the tests to have mock metrics everywhere,
		// this should be removed.
		// TODO: Remove after tests cleanup.
		go func(b Backend) {
			var t *prometheus.Timer
			if durationHist != nil {
				t = prometheus.NewTimer(durationHist.WithLabelValues(b.GetCluster()))
			}
			defer func() {
				if t != nil {
					t.ObserveDuration()
				}
			}()

			msg, err := b.Find(ctx, request)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}
}

func (backend Backend) SendInfo(ctx context.Context, request types.InfoRequest, msgCh chan []types.Info, errCh chan error) {
	if cap(backend.infoQ) > 0 {
		backend.infoQ <- &infoReq{
			InfoRequest: request,
			Ctx:         ctx,
			StartTime:   time.Now(),
			Results:     msgCh,
			Errors:      errCh,
		}
		backend.requestsInQueue.WithLabelValues("info").Inc()
		backend.enqueuedRequests.WithLabelValues("info").Inc()
	} else {
		// This branch is only necessary to satisfy the tests.
		// After we clean-up and rework the tests to have mock metrics everywhere,
		// this should be removed.
		// TODO: Remove after tests cleanup.
		go func(b Backend) {
			msg, err := b.Info(ctx, request)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}
}

func NewMock(cfg mock.Config) Backend {
	return NewBackend(mock.New(cfg), 0, 0, nil, nil, nil, nil)
}
