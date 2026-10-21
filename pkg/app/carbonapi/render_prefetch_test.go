package carbonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/expr/functions"
	"github.com/bookingcom/carbonapi/pkg/types"
	wire "github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

var prefetchTestFunctions sync.Once

func prefetchTestApp(t *testing.T, server string, concurrency int) *App {
	t.Helper()
	prefetchTestFunctions.Do(func() { functions.New(nil, zap.NewNop()) })
	c := cfg.DefaultAPIConfig()
	c.ResolveGlobs = 0
	c.MaxConcurrentUpstreamRequests = concurrency
	c.ProcWorkers = 4
	c.QueueSize = 2
	c.Timeouts.Global = 3 * time.Second
	z := cfg.DefaultZipperConfig()
	z.Backends = []string{server}
	z.ProtocolBackends, z.BackendsByCluster, z.BackendsByDC = nil, nil, nil
	z.BackendQueueSize = 2
	z.ConcurrencyLimitPerServer = concurrency
	app := &App{config: c, ZipperConfig: z, ms: newPrometheusMetrics(c), queryCache: cache.NullCache{}, findCache: cache.NullCache{}, TopLevelDomainCache: expirecache.New(0), fastQ: make(chan *RenderReq, 2), slowQ: make(chan *RenderReq, 2)}
	app.ZipperMetrics = NewZipperPrometheusMetrics(z)
	var err error
	app.Backends, err = InitBackends(z, app.ZipperMetrics, &app.ms, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ProcessRequests(app, zap.NewNop())
	return app
}

func prefetchTestRequest(app *App, targets []string) *httptest.ResponseRecorder {
	q := url.Values{"target": targets, "from": {"1700000000"}, "until": {"1700000003"}, "format": {"json"}, "noCache": {"1"}}
	r := httptest.NewRequest(http.MethodGet, "/render?"+q.Encode(), nil)
	w := httptest.NewRecorder()
	app.renderHandler(w, r, zap.NewNop())
	return w
}

func TestRenderPrefetchOverlapsFetchesAndPreservesOutput(t *testing.T) {
	var calls atomic.Int64
	var arrivals atomic.Int64
	both := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("target")
		calls.Add(1)
		if path == "m.a" || path == "m.b" {
			if arrivals.Add(1) == 2 {
				once.Do(func() { close(both) })
			}
			select {
			case <-both:
			case <-r.Context().Done():
				return
			}
		}
		if path == "m.missing" {
			http.NotFound(w, r)
			return
		}
		blob, err := wire.RenderEncoder([]types.Metric{{Name: path, StartTime: 1700000000, StopTime: 1700000003, StepTime: 1, Values: []float64{1, 2, 3}, IsAbsent: []bool{false, false, false}}})
		if err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(blob)
	}))
	defer server.Close()
	app := prefetchTestApp(t, server.URL, 2)
	targets := []string{"alias(scale(m.a,2),'first')", "alias(m.b,'second')", "alias(m.a,'again')", "fallbackSeries(m.missing,constantLine(7))", "alias(m.missing,'missing-again')"}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- prefetchTestRequest(app, targets) }()
	select {
	case w := <-done:
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var got []struct {
			Target     string      `json:"target"`
			Datapoints [][]float64 `json:"datapoints"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 4 || got[0].Target != "first" || got[1].Target != "second" || got[2].Target != "again" || got[3].Target != "7" {
			t.Fatalf("target order/fallback changed: %s", w.Body.String())
		}
		if got[0].Datapoints[0][0] != 2 || got[2].Datapoints[0][0] != 1 || got[3].Datapoints[0][0] != 7 {
			t.Fatalf("shared input mutated: %s", w.Body.String())
		}
		if calls.Load() != 3 {
			t.Fatalf("duplicate or missing fetches: %d", calls.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetches were serialized or request driver stalled")
	}
}

func TestRenderPrefetchCancelsUnusedFetchesBeforeReturning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("target") == "m.fail" {
			http.Error(w, "backend failed", http.StatusInternalServerError)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	app := prefetchTestApp(t, server.URL, 2)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- prefetchTestRequest(app, []string{"m.fail", "m.unused"}) }()
	select {
	case w := <-done:
		if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "500") {
			t.Fatalf("backend failure changed: %d %s", w.Code, w.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("unused fetch delayed error response or was not cancelled")
	}
}

func TestRenderPrefetchAlreadyCancelledRequestTerminates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
	defer server.Close()
	app := prefetchTestApp(t, server.URL, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest(http.MethodGet, "/render?target=m.a&target=m.b&from=1700000000&until=1700000003&format=json&noCache=1", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		app.renderHandler(httptest.NewRecorder(), r, zap.NewNop())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled request stalled with one upstream worker")
	}
}

func TestRenderPrefetchMatchesSequentialEvaluation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("target")
		if path == "m.missing" {
			http.NotFound(w, r)
			return
		}
		if path == "m.fail" {
			http.Error(w, "backend failed", 500)
			return
		}
		from, _ := strconv.Atoi(r.URL.Query().Get("from"))
		until, _ := strconv.Atoi(r.URL.Query().Get("until"))
		m := types.Metric{Name: path, StartTime: int32(from), StopTime: int32(until), StepTime: 1}
		for i := from; i < until; i++ {
			m.Values = append(m.Values, float64(i%13))
			m.IsAbsent = append(m.IsAbsent, i%7 == 0)
		}
		blob, err := wire.RenderEncoder([]types.Metric{m})
		if err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(blob)
	}))
	defer server.Close()
	parallel := prefetchTestApp(t, server.URL, 2)
	sequential := prefetchTestApp(t, server.URL, 2)
	// Literal paths require no find requests in either configuration. This
	// exercises the existing sequential handler against the overlapping path.
	sequential.config.ResolveGlobs = 1
	expressions := []string{"m.a", "alias(scale(m.a,2),'scaled')", "alias(m.b,'second')", "fallbackSeries(m.missing,constantLine(7))", "movingSum(m.a,2)", "timeShift(m.a,'-2s')", "sumSeries(m.a,m.b)", "m.missing", "m.fail"}
	rng := rand.New(rand.NewSource(20261021))
	for i := range 80 {
		targets := make([]string, 2+rng.Intn(4))
		for j := range targets {
			targets[j] = expressions[rng.Intn(len(expressions))]
		}
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			want := prefetchTestRequest(sequential, targets)
			got := prefetchTestRequest(parallel, targets)
			if got.Code != want.Code || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
				t.Fatalf("output changed for %v: sequential %d %s; prefetch %d %s", targets, want.Code, want.Body.String(), got.Code, got.Body.String())
			}
		})
	}
	for _, tc := range []struct {
		targets []string
		status  int
	}{
		{[]string{"m.fail", "alias("}, 500},
		{[]string{"alias(", "m.fail"}, 400},
		{[]string{"m.missing", "alias("}, 400},
	} {
		got := prefetchTestRequest(parallel, tc.targets)
		want := prefetchTestRequest(sequential, tc.targets)
		if got.Code != tc.status || got.Code != want.Code || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
			t.Fatalf("first target error changed for %v: %d %s / %d %s", tc.targets, got.Code, got.Body.String(), want.Code, want.Body.String())
		}
	}
}
