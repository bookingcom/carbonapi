package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	wire "github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type coalesceTestBackend struct {
	render func(context.Context, types.RenderRequest) ([]types.Metric, error)
}

func (coalesceTestBackend) SupportsTargetCoalescing() bool { return true }

func (b coalesceTestBackend) Render(c context.Context, r types.RenderRequest) ([]types.Metric, error) {
	return b.render(c, r)
}
func (b coalesceTestBackend) Find(context.Context, types.FindRequest) (types.Matches, error) {
	return types.Matches{}, nil
}
func (b coalesceTestBackend) Info(context.Context, types.InfoRequest) ([]types.Info, error) {
	return nil, nil
}
func (b coalesceTestBackend) Contains([]string) bool   { return true }
func (b coalesceTestBackend) Logger() *zap.Logger      { return zap.NewNop() }
func (b coalesceTestBackend) GetServerAddress() string { return "fixture" }
func (b coalesceTestBackend) BackendInfo() (string, string, string) {
	return "fixture", "fixture", "local"
}

// Leave processing stopped so each test deterministically fills the real queues.
func coalesceBackend(render func(context.Context, types.RenderRequest) ([]types.Metric, error), capacity int) Backend {
	s := &backendState{fastRenderQ: make(chan *renderReq, 256), slowRenderQ: make(chan *renderReq, 256), findQ: make(chan *findReq, 256), infoQ: make(chan *infoReq, 256), semaSize: capacity, sourceClusters: []string{"fixture"}, saturation: prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_saturation"})}
	metrics := func() requestMetrics {
		return requestMetrics{
			inQueue:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_queue"}),
			timeInQ:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_wait"}),
			enqueued: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_enqueued"}),
			duration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_duration"}),
		}
	}
	s.render, s.find, s.info = metrics(), metrics(), metrics()
	return Backend{BackendImpl: coalesceTestBackend{render: render}, backendState: s}
}

func coalesceRequest(ctx context.Context, path string) *renderReq {
	return &renderReq{RenderRequest: types.NewRenderRequest([]string{path}, 1, 4), Ctx: ctx, StartTime: time.Now(), Results: make(chan []types.Metric, 1), Errors: make(chan error, 1)}
}

func coalesceMetrics(paths []string) []types.Metric {
	ms := make([]types.Metric, len(paths))
	for i, p := range paths {
		ms[i] = types.Metric{Name: p, StartTime: 1, StopTime: 4, StepTime: 1, Values: []float64{math.Copysign(0, -1), 42, math.Float64frombits(0x7ff8000000000015)}, IsAbsent: []bool{false, false, true}}
	}
	return ms
}

func TestCollectRenderBatchBoundsAndIdentity(t *testing.T) {
	ctx := util.WithRenderBatch(context.Background(), 4)
	bUnsupported := coalesceBackend(nil, 4)
	bUnsupported.BackendImpl = struct{ BackendImpl }{bUnsupported.BackendImpl}
	bUnsupported.fastRenderQ <- coalesceRequest(ctx, "b")
	if batch, pending := bUnsupported.collectRenderBatch(coalesceRequest(ctx, "a")); len(batch) != 1 || pending != nil || len(bUnsupported.fastRenderQ) != 1 {
		t.Fatal("transport without the per-target contract was coalesced")
	}
	for _, tc := range []struct {
		name string
		next *renderReq
		join bool
	}{
		{"same", coalesceRequest(ctx, "b"), true},
		{"other_context", coalesceRequest(util.WithRenderBatch(context.Background(), 4), "b"), false},
		{"unmarked", coalesceRequest(context.Background(), "b"), false},
		{"duplicate", coalesceRequest(ctx, "a"), false},
		{"glob", coalesceRequest(ctx, "b.*"), false},
		{"escaped", coalesceRequest(ctx, "b\\x"), false},
		{"oversize", coalesceRequest(ctx, strings.Repeat("%", 3000)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := coalesceBackend(nil, 4)
			b.fastRenderQ <- tc.next
			batch, pending := b.collectRenderBatch(coalesceRequest(ctx, "a"))
			if (len(batch) == 2) != tc.join || (!tc.join && pending != tc.next) {
				t.Fatalf("batch=%d carry=%p", len(batch), pending)
			}
		})
	}
	b := coalesceBackend(nil, 2)
	for _, p := range []string{"b", "c", "d"} {
		b.fastRenderQ <- coalesceRequest(ctx, p)
	}
	batch, pending := b.collectRenderBatch(coalesceRequest(ctx, "a"))
	if len(batch) != 2 || pending != nil || len(b.fastRenderQ) != 2 {
		t.Fatal("batch exceeded backend metric capacity")
	}
	first, next := coalesceRequest(ctx, "a"), coalesceRequest(ctx, "b")
	next.Until++
	b = coalesceBackend(nil, 4)
	b.fastRenderQ <- next
	if batch, pending = b.collectRenderBatch(first); len(batch) != 1 || pending != next {
		t.Fatal("time windows combined")
	}
	ctx = util.WithRenderBatch(context.Background(), 32)
	b = coalesceBackend(nil, 32)
	first = coalesceRequest(ctx, strings.Repeat("%", 900)+"a")
	for _, suffix := range []string{"b", "c", "d"} {
		b.fastRenderQ <- coalesceRequest(ctx, strings.Repeat("%", 900)+suffix)
	}
	batch, _ = b.collectRenderBatch(first)
	q := url.Values{}
	for _, r := range batch {
		q.Add("target", r.Targets[0])
	}
	if len(batch) != 2 || len(q.Encode()) > maxRenderBatchURLBytes-128 {
		t.Fatal("encoded URL budget exceeded")
	}
}

func waitCoalesceQueues(t *testing.T, bs []Backend, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, b := range bs {
			ready = ready && len(b.fastRenderQ) == n
		}
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("requests did not reach backend queues")
}

func TestCoalescedReadsKeepReplicaErrorsAndMissingSeries(t *testing.T) {
	for _, scenario := range []string{"identical", "one_slow_failure", "missing", "majority"} {
		t.Run(scenario, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ctx := util.WithRenderBatch(parent, 4)
			paths := []string{"a", "b", "c", "d"}
			var calls [3]atomic.Int64
			var bs []Backend
			for replica := 0; replica < 3; replica++ {
				replica := replica
				bs = append(bs, coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
					calls[replica].Add(1)
					if scenario == "one_slow_failure" && replica == 2 {
						time.Sleep(30 * time.Millisecond)
						return nil, errors.New("unavailable replica")
					}
					ms := coalesceMetrics(r.Targets)
					if scenario == "missing" {
						out := ms[:0]
						for _, m := range ms {
							if m.Name != "b" {
								out = append(out, m)
							}
						}
						ms = out
					}
					if scenario == "majority" && replica == 2 {
						for i := range ms {
							ms[i].Values[1] = 999
						}
					}
					return ms, nil
				}, 4))
			}
			type result struct {
				metrics []types.Metric
				errs    []error
			}
			results := make([]chan result, len(paths))
			for i, path := range paths {
				results[i] = make(chan result, 1)
				go func(i int, path string) {
					ms, _, errs := Renders(ctx, bs, types.NewRenderRequest([]string{path}, 1, 4), cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeMajority}, zap.NewNop())
					results[i] <- result{ms, errs}
				}(i, path)
			}
			waitCoalesceQueues(t, bs, len(paths))
			for i := range bs {
				bs[i].Proc()
			}
			for i, path := range paths {
				select {
				case got := <-results[i]:
					if scenario == "missing" && path == "b" {
						if len(got.metrics) != 0 || len(got.errs) != 3 {
							t.Fatalf("missing path: %+v", got)
						}
						continue
					}
					if len(got.metrics) != 1 || got.metrics[0].Name != path || got.metrics[0].Values[1] != 42 || math.Float64bits(got.metrics[0].Values[0]) != 1<<63 {
						t.Fatalf("changed replica result: %+v", got)
					}
					wantErrs := 0
					if scenario == "one_slow_failure" {
						wantErrs = 1
					}
					if len(got.errs) != wantErrs {
						t.Fatalf("replica errors: %v", got.errs)
					}
				case <-parent.Done():
					t.Fatal("replica fan-in blocked")
				}
			}
			for i := range calls {
				if n := calls[i].Load(); n != 1 {
					t.Fatalf("replica %d made %d calls, want one batch", i, n)
				}
			}
		})
	}
}

func TestCoalescedRenameFallbackIsConcurrentAndBounded(t *testing.T) {
	var active, maximum, calls atomic.Int64
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		calls.Add(1)
		n := int64(len(r.Targets))
		v := active.Add(n)
		defer active.Add(-n)
		for old := maximum.Load(); v > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, v) {
				break
			}
		}
		if len(r.Targets) > 1 {
			return coalesceMetrics([]string{"renamed"}), nil
		}
		time.Sleep(time.Millisecond)
		return coalesceMetrics([]string{r.Targets[0] + ".alias"}), nil
	}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = util.WithRenderBatch(ctx, 4)
	var requests []*renderReq
	for _, p := range []string{"a", "b", "c", "d"} {
		r := coalesceRequest(ctx, p)
		requests = append(requests, r)
		b.fastRenderQ <- r
	}
	b.Proc()
	for _, r := range requests {
		select {
		case ms := <-r.Results:
			if len(ms) != 1 || ms[0].Name != r.Targets[0]+".alias" {
				t.Fatal("renamed result lost")
			}
		case err := <-r.Errors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("fallback blocked")
		}
	}
	if maximum.Load() > 4 || calls.Load() != 5 {
		t.Fatalf("unbounded fallback: peak%d calls%d", maximum.Load(), calls.Load())
	}
}

func TestCoalescedAdmissionCancelsAndRespectsMetricLimit(t *testing.T) {
	var active, maximum atomic.Int64
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		n := int64(len(r.Targets))
		v := active.Add(n)
		defer active.Add(-n)
		for old := maximum.Load(); v > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, v) {
				break
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}, 2)
	parent, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ctx := util.WithRenderBatch(parent, 32)
	var requests []*renderReq
	for i := 0; i < 20; i++ {
		r := coalesceRequest(ctx, fmt.Sprint(i))
		requests = append(requests, r)
		b.fastRenderQ <- r
	}
	b.Proc()
	for _, r := range requests {
		select {
		case err := <-r.Errors:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("cancelled admission or results blocked")
		}
	}
	if maximum.Load() > 2 {
		t.Fatal("batch multiplied backend metric concurrency")
	}
}

func TestCoalescedValuesDoNotAliasAcrossTargets(t *testing.T) {
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		return coalesceMetrics(r.Targets), nil
	}, 4)
	ctx := util.WithRenderBatch(context.Background(), 4)
	a, c := coalesceRequest(ctx, "a"), coalesceRequest(ctx, "c")
	b.fastRenderQ <- a
	b.fastRenderQ <- c
	b.Proc()
	first, second := <-a.Results, <-c.Results
	want := append([]float64(nil), second[0].Values...)
	first[0].Values[1] = 999
	if !reflect.DeepEqual(second[0].IsAbsent, []bool{false, false, true}) {
		t.Fatal("absence flags changed")
	}
	for i, v := range want {
		if math.Float64bits(second[0].Values[i]) != math.Float64bits(v) {
			t.Fatal("independent target values aliased")
		}
	}
}

func TestCoalescingKeepsEachTargetsSelectedBackends(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = util.WithRenderBatch(ctx, 4)
	var mu sync.Mutex
	var secondTargets []string
	first := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		return coalesceMetrics(r.Targets), nil
	}, 4)
	second := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		mu.Lock()
		secondTargets = append(secondTargets, r.Targets...)
		mu.Unlock()
		return coalesceMetrics(r.Targets), nil
	}, 4)
	done := make(chan error, 2)
	for _, path := range []string{"known", "cache.miss"} {
		path := path
		bs := []Backend{first}
		if path == "cache.miss" {
			bs = append(bs, second)
		}
		go func() {
			ms, _, errs := Renders(ctx, bs, types.NewRenderRequest([]string{path}, 1, 4), cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
			if len(errs) != 0 || len(ms) != 1 || ms[0].Name != path {
				done <- fmt.Errorf("routing %s: %v", path, errs)
				return
			}
			done <- nil
		}()
	}
	waitCoalesceQueues(t, []Backend{first}, 2)
	waitCoalesceQueues(t, []Backend{second}, 1)
	first.Proc()
	second.Proc()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("routing fan-in blocked")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(secondTargets, []string{"cache.miss"}) {
		t.Fatalf("per-target routing lost: %q", secondTargets)
	}
}

func TestCarriedSlowRequestsDoNotStarveFastQueue(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 100)
	release := make(chan struct{})
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		if util.GetPriority(ctx) >= 10000 {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return coalesceMetrics(r.Targets), nil
	}, 2)
	for i := 0; i < 100; i++ {
		ctx := util.WithRenderBatch(util.WithPriority(parent, 10000), 4)
		b.slowRenderQ <- coalesceRequest(ctx, fmt.Sprint(i))
	}
	b.Proc()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("slow queue did not start")
		}
	}
	r := coalesceRequest(parent, "fast")
	b.SendRender(r.Ctx, r.RenderRequest, r.Results, r.Errors)
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queued work did not resume")
	}
	release <- struct{}{}
	select {
	case ms := <-r.Results:
		if len(ms) != 1 || ms[0].Name != "fast" {
			t.Fatal("fast response lost")
		}
	case err := <-r.Errors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("carried slow requests starved the fast queue")
	}
}

func TestCoalescedHTTPRoundTrip(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("from") != "1" || r.URL.Query().Get("until") != "4" {
			t.Error("time window changed")
		}
		raw, err := wire.RenderEncoder(coalesceMetrics(r.URL.Query()["target"]))
		if err != nil {
			t.Error(err)
			http.Error(w, "encode", 500)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(raw)
	}))
	defer server.Close()
	be, err := bnet.New(bnet.Config{Address: server.URL, Client: server.Client(), Cluster: "fixture", Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	if (&bnet.GrpcBackend{NetBackend: be}).SupportsTargetCoalescing() {
		t.Fatal("gRPC inherited the HTTP missing-series contract")
	}
	b := coalesceBackend(nil, 4)
	b.BackendImpl = be
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx := util.WithRenderBatch(parent, 4)
	paths := []string{"metric.space name", "metric.amp&equals=value", "metric.slash/name", "metric.four"}
	var requests []*renderReq
	for _, path := range paths {
		r := coalesceRequest(ctx, path)
		requests = append(requests, r)
		b.fastRenderQ <- r
	}
	b.Proc()
	for i, r := range requests {
		select {
		case ms := <-r.Results:
			if len(ms) != 1 || ms[0].Name != paths[i] || math.Float64bits(ms[0].Values[0]) != 1<<63 || math.Float64bits(ms[0].Values[2]) != 0x7ff8000000000015 || !ms[0].IsAbsent[2] {
				t.Fatalf("wire result changed for %q", paths[i])
			}
		case err := <-r.Errors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("HTTP batch blocked")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("made %d wire requests, want one", calls.Load())
	}
}
