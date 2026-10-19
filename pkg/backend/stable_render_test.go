package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

func TestStableGroupFailurePreservesIndependentTargetResults(t *testing.T) {
	failure := errors.New("one broken target")
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		for _, path := range r.Targets {
			if path == "bad" {
				return nil, failure
			}
		}
		return coalesceMetrics(r.Targets), nil
	}, 4)
	b.Proc()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	paths := []string{"a", "b", "bad"}
	requests := make([]types.RenderRequest, len(paths))
	choices := make([][]Backend, len(paths))
	for i, path := range paths {
		requests[i] = types.NewRenderRequest([]string{path}, 1, 4)
		choices[i] = []Backend{b}
	}
	replies := RendersGrouped(ctx, choices, requests, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	for i, reply := range replies {
		if paths[i] == "bad" {
			if len(reply.Errors) != 1 || reply.Errors[0] != failure {
				t.Fatal("target failure changed", reply.Errors)
			}
		} else if len(reply.Errors) != 0 || len(reply.Metrics) != 1 || reply.Metrics[0].Name != paths[i] {
			t.Fatal("bad target changed a neighboring result", i, reply)
		}
	}
}

func TestAtomicBatchQueueProgressAndContiguity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q := newRequestQueue[int](128)
	var producers sync.WaitGroup
	for producer := 0; producer < 4; producer++ {
		producers.Add(1)
		go func(p int) {
			defer producers.Done()
			for batch := 0; batch < 4; batch++ {
				values := make([]int, 80)
				for i := range values {
					values[i] = (p*4+batch+1)*1000 + i
				}
				if !q.sendMany(ctx, values) {
					return
				}
			}
		}(producer)
	}
	producers.Add(1)
	go func() {
		defer producers.Done()
		for i := 0; i < 100; i++ {
			if !q.send(ctx, -i-1) {
				return
			}
		}
	}()
	done := make(chan error, 1)
	go func() {
		groups := map[int]bool{}
		singles := 0
		for received := 0; received < 4*4*80+100; {
			v := q.recv()
			received++
			if v < 0 {
				singles++
				continue
			}
			if v%1000 != 0 || groups[v/1000] {
				done <- fmt.Errorf("bad group start %d", v)
				return
			}
			groups[v/1000] = true
			for i := 1; i < 80; i++ {
				next := q.recv()
				received++
				if next != v+i {
					done <- fmt.Errorf("group interleaved: %d then %d", v, next)
					return
				}
			}
		}
		if len(groups) != 16 || singles != 100 {
			done <- fmt.Errorf("groups=%d singles=%d", len(groups), singles)
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("concurrent batch producers stalled")
	}
	producers.Wait()
}

func TestAtomicBatchCancellationReturnsReservedSlots(t *testing.T) {
	q := newRequestQueue[int](128)
	for i := 0; i < 120; i++ {
		q.send(context.Background(), i)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- q.sendMany(ctx, make([]int, 80)) }()
	deadline := time.Now().Add(time.Second)
	for len(q.slots) != 128 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(q.slots) != 128 {
		cancel()
		t.Fatal("batch did not reserve available slots")
	}
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancelled partial batch was published")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation stalled")
	}
	if q.length() != 120 || len(q.slots) != 120 {
		t.Fatal("cancelled reservation leaked slots")
	}
	for i := 0; i < 120; i++ {
		if q.recv() != i {
			t.Fatal("FIFO changed")
		}
	}
	if !q.sendMany(context.Background(), make([]int, 80)) {
		t.Fatal("batch gate leaked")
	}
}

func TestStableGroupedRenderRoutingErrorsAndCacheKeys(t *testing.T) {
	var mu sync.Mutex
	keys := map[int][]string{}
	newBackend := func(id int) Backend {
		b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
			mu.Lock()
			keys[id] = append(keys[id], strings.Join(r.Targets, ","))
			mu.Unlock()
			var paths []string
			for _, path := range r.Targets {
				if path != "missing" {
					paths = append(paths, path)
				}
			}
			return coalesceMetrics(paths), nil
		}, 4)
		b.Proc()
		return b
	}
	a, b := newBackend(0), newBackend(1)
	paths := []string{"a", "b", "c", "missing"}
	requests := make([]types.RenderRequest, len(paths))
	choices := make([][]Backend, len(paths))
	for i, path := range paths {
		requests[i] = types.NewRenderRequest([]string{path}, 1, 4)
		choices[i] = []Backend{a}
		if i%2 == 0 {
			choices[i] = append(choices[i], b)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 20 {
		replies := RendersGrouped(ctx, choices, requests, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
		for i, reply := range replies {
			if paths[i] == "missing" {
				if len(reply.Errors) != 1 || reply.Errors[0] != types.ErrMetricsNotFound {
					t.Fatal("missing-target error changed", reply.Errors)
				}
				continue
			}
			if len(reply.Errors) != 0 || len(reply.Metrics) != 1 || reply.Metrics[0].Name != paths[i] || math.Float64bits(reply.Metrics[0].Values[0]) != 1<<63 || math.Float64bits(reply.Metrics[0].Values[2]) != 0x7ff8000000000015 {
				t.Fatal("per-target result changed", i, reply)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for id, want := range map[int]string{0: "a,b,c,missing", 1: "a,c"} {
		if len(keys[id]) != 20 {
			t.Fatalf("backend %d made %d calls", id, len(keys[id]))
		}
		for _, key := range keys[id] {
			if key != want {
				t.Fatalf("unstable cache key or routing: %q != %q", key, want)
			}
		}
	}
}

func TestStableGroupedRenderConcurrentRootsDoNotMix(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		mu.Lock()
		keys = append(keys, strings.Join(r.Targets, ","))
		mu.Unlock()
		return coalesceMetrics(r.Targets), nil
	}, 128)
	b.Proc()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 32)
	for group := 0; group < 32; group++ {
		go func(g int) {
			requests := make([]types.RenderRequest, 16)
			choices := make([][]Backend, 16)
			for i := range requests {
				requests[i] = types.NewRenderRequest([]string{fmt.Sprintf("%02d.%02d", g, i)}, 1, 4)
				choices[i] = []Backend{b}
			}
			replies := RendersGrouped(ctx, choices, requests, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
			for _, reply := range replies {
				if len(reply.Errors) != 0 || len(reply.Metrics) != 1 {
					done <- fmt.Errorf("incomplete group %d", g)
					return
				}
			}
			done <- nil
		}(group)
	}
	for range 32 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("group fan-in stalled")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	sort.Strings(keys)
	if len(keys) != 32 {
		t.Fatalf("made %d calls for 32 roots", len(keys))
	}
	for g, key := range keys {
		var want []string
		for i := 0; i < 16; i++ {
			want = append(want, fmt.Sprintf("%02d.%02d", g, i))
		}
		if !reflect.DeepEqual(strings.Split(key, ","), want) {
			t.Fatal("independent root groups mixed", key)
		}
	}
}
