package prioritylimiter

import (
	"container/heap"
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	indexStateActive    = -1
	indexStateNew       = -2
	indexStateCancelled = -3
)

type request struct {
	priority int // less is more
	canEnter chan struct{}
	index    int
	uuid     string
}

type requests []*request

// Limiter does two things
// a) limits the number of concurrent requests going upstream
// b) prioritize the "waiting" requests
// For prioritization we are using two variables:
// "priority": that is request complexity, less complexity == more priority
// "uuid": for requests of equal comlexity, process them ordered by uuid in order do minimize the number of "active" requests
type Limiter struct {
	requests      requests
	limiter       chan struct{}
	wantToEnter   chan *request
	cancelRequest chan *request
	loopCount     uint32
	activeGauge   prometheus.Gauge
	waitingGauge  prometheus.Gauge
}

type LimiterOption func(*Limiter)

// New creates a new limiter that allows maximum "limit" requests to "Enter"
func New(limit int, options ...LimiterOption) *Limiter {
	ret := &Limiter{
		limiter:       make(chan struct{}, limit),
		wantToEnter:   make(chan *request),
		cancelRequest: make(chan *request),
		loopCount:     0,
	}
	for _, option := range options {
		option(ret)
	}

	go ret.loop()

	return ret
}

// WithMetrics adds prometheus metrics to the Limiter instanace
func WithMetrics(activeGauge, waitingGauge prometheus.Gauge) LimiterOption {
	return func(l *Limiter) {
		l.activeGauge = activeGauge
		l.waitingGauge = waitingGauge
	}
}

// Enter blocks this request until it's turn comes
func (l *Limiter) Enter(ctx context.Context, priority int, uuid string) error {
	canEnter := make(chan struct{})

	req := &request{
		priority: priority,
		canEnter: canEnter,
		uuid:     uuid,
		index:    indexStateNew,
	}

	l.wantToEnter <- req

	select {
	// Check first if the ctx is not closed
	case <-ctx.Done():
		l.cancelRequest <- req
		return ctx.Err()
	default:
		select {
		case <-ctx.Done():
			l.cancelRequest <- req
			return ctx.Err()
		case <-canEnter:
			return nil
		}
	}
}

// Leave marks a request as complete
func (l *Limiter) Leave() error {
	select {
	case <-l.limiter:
		// fallthrough
	default:
		// this should never happen, but let's not block forever if it does
		return errors.New("Unable to return value to limiter")
	}
	return nil
}

// Active returns the number of in progress requests
func (l *Limiter) Active() int {
	return len(l.limiter)
}

func (l *Limiter) loop() {
	for {
		if len(l.requests) == 0 {
			select {
			case req := <-l.wantToEnter:
				if req.index != indexStateCancelled {
					heap.Push(&l.requests, req)
				}
			case req := <-l.cancelRequest:
				index := req.index
				if index >= 0 {
					heap.Remove(&l.requests, index)
				}
				if index == indexStateActive {
					// If we are receiving a cancel request at this point,
					// it means Enter() returned with error, and the caller will not Leave()
					l.Leave()
				}
				req.index = indexStateCancelled
			}
		} else {
			select {
			case req := <-l.wantToEnter:
				if req.index != indexStateCancelled {
					heap.Push(&l.requests, req)
				}
			case req := <-l.cancelRequest:
				index := req.index
				if index >= 0 {
					heap.Remove(&l.requests, index)
				}
				if index == indexStateActive {
					// If we are receiving a cancel request at this point,
					// it means Enter() returned with error, and the caller will not Leave()
					l.Leave()
				}
				req.index = indexStateCancelled
			case l.limiter <- struct{}{}:
				req := heap.Pop(&l.requests).(*request)
				close(req.canEnter)
			}
		}
		atomic.AddUint32(&l.loopCount, 1)
		if l.activeGauge != nil {
			l.activeGauge.Set(float64(len(l.limiter)))
		}
		if l.waitingGauge != nil {
			l.waitingGauge.Set(float64(len(l.requests)))
		}
	}
}

// used in tests to ensure that loop() processed all the pending messages
func (l *Limiter) waitLoopCount(i int) {
	for {
		count := int(atomic.LoadUint32(&l.loopCount))
		if count >= i {
			return
		}
		time.Sleep(time.Millisecond * 50)
	}
}

func (r requests) Len() int {
	return len(r)
}

func (r requests) Less(i, j int) bool {
	if r[i].priority == r[j].priority {
		return r[i].uuid < r[j].uuid
	}
	return r[i].priority < r[j].priority
}

func (r requests) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
	r[i].index = i
	r[j].index = j
}

func (r *requests) Push(x interface{}) {
	req := x.(*request)
	idx := len(*r)
	req.index = idx
	*r = append(*r, req)
}

func (r *requests) Pop() interface{} {
	old := *r
	n := len(old)
	req := old[n-1]
	req.index = indexStateActive
	old[n-1] = nil // avoid memory leak
	*r = old[0 : n-1]
	return req
}
