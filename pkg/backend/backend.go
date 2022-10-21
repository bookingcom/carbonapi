package backend

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

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

// The backend to send requests to.
type Backend struct {
	BackendImpl

	// Request queues.
	// There's a separate one for each type, because the stampeding requests of one type should not
	// impact the other types of requests.
	// This, for example, removes the dependency between the find and render requests performance.
	// Huge amount of render requests will not have direct impact on the find requests besides the semphore.
	RenderQ chan *renderReq
	FindQ   chan *findReq
	InfoQ   chan *infoReq // the info requests are expected to be relatively rare

	// The size of the requests semaphore:
	// The maximum number of simultaneous requests.
	SemaSize int
}

// Creates a new backend and starts processing the queues if qSize > 0.
//
// Passing qSize == 0 disables async processing, and the backend only works as a proxy to the implementation functions.
// Using the channels won't work in that mode.
// TODO (redesign): Remove the option to have qSize == 0 and proxy processor when the redesign is finished.
func NewBackend(impl BackendImpl, qSize int, semaSize int,
	requestsInQueue *prometheus.GaugeVec,
	saturation *prometheus.Gauge,
	timeInQSec *prometheus.HistogramVec) Backend {
	b := Backend{
		BackendImpl: impl,
		RenderQ:     make(chan *renderReq, qSize),
		FindQ:       make(chan *findReq, qSize),
		InfoQ:       make(chan *infoReq, qSize),
		SemaSize:    semaSize,
	}

	if qSize > 0 {
		// Proc is the only function that uses the supplied metrics.
		// The metrics can be nil if it's not called.
		// Cannot be called w/ nil metrics.
		b.Proc(requestsInQueue, *saturation, timeInQSec)
	}

	return b
}

// Process the requests in the queue.
// Should not be called when async processing is disabled.
// Expects the metrics to be non-nil.
func (b *Backend) Proc(
	requestsInQueue *prometheus.GaugeVec,
	saturation prometheus.Gauge,
	timeInQSec *prometheus.HistogramVec) {

	semaphore := make(chan bool, b.SemaSize)

	go func() {
		for r := range b.RenderQ {
			requestsInQueue.WithLabelValues("render").Dec()
			semaphore <- true
			saturation.Inc()
			timeInQSec.WithLabelValues("render").Observe(float64(time.Now().Sub(r.StartTime)))
			go func(req *renderReq) {
				res, err := b.BackendImpl.Render(req.Ctx, req.RenderRequest)
				if err != nil {
					req.Errors <- err
				} else {
					req.Results <- res
				}
				<-semaphore
				saturation.Dec()
			}(r)
		}
	}()
	go func() {
		for r := range b.FindQ {
			requestsInQueue.WithLabelValues("find").Dec()
			semaphore <- true
			saturation.Inc()
			timeInQSec.WithLabelValues("find").Observe(float64(time.Now().Sub(r.StartTime)))
			go func(req *findReq) {
				res, err := b.BackendImpl.Find(req.Ctx, req.FindRequest)
				if err != nil {
					req.Errors <- err
				} else {
					req.Results <- res
				}
				<-semaphore
				saturation.Dec()
			}(r)
		}
	}()
	go func() {
		for r := range b.InfoQ {
			requestsInQueue.WithLabelValues("info").Dec()
			semaphore <- true
			saturation.Inc()
			timeInQSec.WithLabelValues("info").Observe(float64(time.Now().Sub(r.StartTime)))
			go func(req *infoReq) {
				res, err := b.BackendImpl.Info(req.Ctx, req.InfoRequest)
				if err != nil {
					req.Errors <- err
				} else {
					req.Results <- res
				}
				<-semaphore
				saturation.Dec()
			}(r)
		}
	}()
}

// The specific backend implementation.
//
// At the moment of writing can be one of:
// * mock, used for tests
// * net, used for standard comms
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

func NewMock(cfg mock.Config) Backend {
	return NewBackend(mock.New(cfg), 0, 0, nil, nil, nil)
}
