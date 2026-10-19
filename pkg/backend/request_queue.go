package backend

import (
	"context"
	"sync"
)

// requestQueue keeps the configured bound without reserving a pointer slot for
// every possible request. Only queued requests need storage or GC scanning.
// Small and unbuffered queues retain native channel rendezvous semantics.
type requestQueue[T any] struct {
	direct chan T
	slots  chan struct{}
	ready  chan struct{}
	batch  chan struct{}
	mu     sync.Mutex
	values []T
	head   int
	n      int
}

func newRequestQueue[T any](capacity int) *requestQueue[T] {
	q := &requestQueue[T]{}
	if capacity <= 64 {
		q.direct = make(chan T, capacity)
	} else {
		// Zero-sized elements reserve no per-slot backing array.
		q.slots = make(chan struct{}, capacity)
		q.ready = make(chan struct{}, 1)
		q.batch = make(chan struct{}, 1)
	}
	return q
}

func (q *requestQueue[T]) send(ctx context.Context, value T) bool {
	if q.direct != nil {
		select {
		case q.direct <- value:
			return true
		case <-ctx.Done():
			return false
		}
	}
	select {
	case q.slots <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	q.mu.Lock()
	q.pushLocked(value)
	q.signal()
	q.mu.Unlock()
	return true
}

func (q *requestQueue[T]) pushLocked(value T) {
	if q.n == len(q.values) {
		values := make([]T, min(cap(q.slots), max(16, 2*len(q.values))))
		// Copy a wrapped ring into FIFO order.
		copied := copy(values, q.values[q.head:])
		copy(values[copied:], q.values[:q.head])
		q.values, q.head = values, 0
	}
	q.values[(q.head+q.n)%len(q.values)] = value
	q.n++
}

// sendMany publishes a complete contiguous group. Only one batch producer
// reserves slots at a time, so partial reservations cannot fill an empty queue
// while every producer waits for another slot. Single sends remain independent.
// Small native channels keep their original behavior and decline batching.
func (q *requestQueue[T]) sendMany(ctx context.Context, values []T) bool {
	if len(values) == 0 {
		return true
	}
	if len(values) == 1 {
		return q.send(ctx, values[0])
	}
	if q.direct != nil || len(values) > cap(q.slots) {
		return false
	}
	select {
	case q.batch <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	defer func() { <-q.batch }()
	reserved := 0
	for reserved < len(values) {
		select {
		case q.slots <- struct{}{}:
			reserved++
		case <-ctx.Done():
			for ; reserved > 0; reserved-- {
				<-q.slots
			}
			return false
		}
	}
	q.mu.Lock()
	for _, value := range values {
		q.pushLocked(value)
	}
	q.signal()
	q.mu.Unlock()
	return true
}

// signal is called with mu held. Receivers rearm readiness while work remains.
func (q *requestQueue[T]) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *requestQueue[T]) tryRecv() (T, bool) {
	if q.direct != nil {
		select {
		case value := <-q.direct:
			return value, true
		default:
			var zero T
			return zero, false
		}
	}
	q.mu.Lock()
	if q.n == 0 {
		q.mu.Unlock()
		var zero T
		return zero, false
	}
	value := q.values[q.head]
	var zero T
	q.values[q.head] = zero
	q.head = (q.head + 1) % len(q.values)
	q.n--
	if q.n == 0 {
		select {
		case <-q.ready:
		default:
		}
		q.head = 0
		if len(q.values) > 256 {
			q.values = nil
		}
	} else {
		q.signal()
	}
	q.mu.Unlock()
	<-q.slots
	return value, true
}

func (q *requestQueue[T]) recv() T {
	if q.direct != nil {
		return <-q.direct
	}
	for {
		if value, ok := q.tryRecv(); ok {
			return value
		}
		<-q.ready
	}
}

func (q *requestQueue[T]) length() int {
	if q.direct != nil {
		return len(q.direct)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.n
}
