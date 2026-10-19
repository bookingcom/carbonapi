package backend

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestRequestQueueMatchesChannel(t *testing.T) {
	for _, capacity := range []int{1, 64, 65, 256, 1024, 100000} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			q := newRequestQueue[*int](capacity)
			oracle := make(chan *int, capacity)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(17))
			// Fill, wrap, grow and drain repeatedly; compare every popped identity.
			for i := 0; i < 2*capacity+20000; i++ {
				if len(oracle) < capacity && (len(oracle) == 0 || rng.Intn(3) != 0) {
					value := new(int)
					*value = i
					oracle <- value
					if !q.send(ctx, value) {
						t.Fatal("unexpected rejection")
					}
				} else {
					want := <-oracle
					got, ok := q.tryRecv()
					if !ok || got != want {
						t.Fatal("FIFO differs from channel")
					}
				}
				if q.length() != len(oracle) {
					t.Fatal("length differs from channel")
				}
			}
			for len(oracle) > 0 {
				if q.recv() != <-oracle {
					t.Fatal("drain differs from channel")
				}
			}
			if _, ok := q.tryRecv(); ok {
				t.Fatal("duplicate delivery")
			}
			// An idle queue must not retain completed requests or a burst's backing array.
			for _, value := range q.values {
				if value != nil {
					t.Fatal("completed request retained")
				}
			}
			if len(q.values) > 256 {
				t.Fatal("burst storage retained")
			}
		})
	}
}

func TestRequestQueueBoundAndCancellation(t *testing.T) {
	for _, capacity := range []int{1, 65, 100000} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			q := newRequestQueue[int](capacity)
			for i := 0; i < capacity; i++ {
				q.send(context.Background(), i)
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan bool, 1)
			go func() { result <- q.send(ctx, -1) }()
			select {
			case <-result:
				t.Fatal("configured capacity exceeded")
			case <-time.After(10 * time.Millisecond):
			}
			cancel()
			select {
			case ok := <-result:
				if ok {
					t.Fatal("canceled blocked sender accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("blocked sender failed to cancel")
			}
			for i := 0; i < capacity; i++ {
				if q.recv() != i {
					t.Fatal("cancellation changed queue contents")
				}
			}
		})
	}
}

func TestRequestQueueUnbufferedRendezvous(t *testing.T) {
	q := newRequestQueue[int](0)
	done := make(chan bool, 1)
	go func() { done <- q.send(context.Background(), 42) }()
	select {
	case <-done:
		t.Fatal("unbuffered send completed without receiver")
	case <-time.After(10 * time.Millisecond):
	}
	if q.recv() != 42 || !<-done {
		t.Fatal("rendezvous failed")
	}
}

func TestRequestQueueConcurrentLiveness(t *testing.T) {
	for _, capacity := range []int{0, 1, 65, 100000} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			q := newRequestQueue[int](capacity)
			const producers, consumers, count = 8, 4, 2000
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			results := make(chan int, producers*count)
			var receive sync.WaitGroup
			for i := 0; i < consumers; i++ {
				receive.Add(1)
				go func() {
					defer receive.Done()
					for j := 0; j < producers*count/consumers; j++ {
						results <- q.recv()
					}
				}()
			}
			var send sync.WaitGroup
			for i := 0; i < producers; i++ {
				send.Add(1)
				go func(id int) {
					defer send.Done()
					for j := 0; j < count; j++ {
						if !q.send(ctx, id*count+j) {
							return
						}
					}
				}(i)
			}
			finished := make(chan struct{})
			go func() { send.Wait(); receive.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("queue deadlocked under concurrent load")
			}
			seen := make([]bool, producers*count)
			for i := 0; i < len(seen); i++ {
				n := <-results
				if n < 0 || n >= len(seen) || seen[n] {
					t.Fatal("lost, duplicated or corrupted request")
				}
				seen[n] = true
			}
		})
	}
}

func TestLazyRenderPriority(t *testing.T) {
	for _, capacity := range []int{1, 256, 100000} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			b := Backend{backendState: &backendState{
				fastRenderQ: newRequestQueue[*renderReq](capacity),
				slowRenderQ: newRequestQueue[*renderReq](capacity),
			}}
			fast, slow := new(renderReq), new(renderReq)
			b.slowRenderQ.send(context.Background(), slow)
			b.fastRenderQ.send(context.Background(), fast)
			if b.nextRender() != fast || b.nextRender() != slow {
				t.Fatal("fast render priority changed")
			}
			// A sleeping receiver must wake for either queue.
			for _, q := range []*requestQueue[*renderReq]{b.fastRenderQ, b.slowRenderQ} {
				result := make(chan *renderReq, 1)
				go func() { result <- b.nextRender() }()
				q.send(context.Background(), fast)
				select {
				case got := <-result:
					if got != fast {
						t.Fatal("unexpected render")
					}
				case <-time.After(time.Second):
					t.Fatal("render receiver failed to wake")
				}
			}
		})
	}
}

var benchmarkQueue any

func BenchmarkRequestQueueCreate(b *testing.B) {
	for _, lazy := range []bool{false, true} {
		b.Run(fmt.Sprint(lazy), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if lazy {
					benchmarkQueue = newRequestQueue[*renderReq](100000)
				} else {
					benchmarkQueue = make(chan *renderReq, 100000)
				}
			}
		})
	}
}

func BenchmarkRequestQueueRoundTrip(b *testing.B) {
	b.Run("channel", func(b *testing.B) {
		q := make(chan *renderReq, 100000)
		r := new(renderReq)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q <- r
			<-q
		}
	})
	b.Run("lazy", func(b *testing.B) {
		q := newRequestQueue[*renderReq](100000)
		r := new(renderReq)
		q.send(context.Background(), r)
		q.recv()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.send(context.Background(), r)
			q.recv()
		}
	})
}
