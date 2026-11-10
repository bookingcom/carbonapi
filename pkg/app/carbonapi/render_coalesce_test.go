package carbonapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/expr/functions"
	"github.com/bookingcom/carbonapi/pkg/types"
	wire "github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/dgryski/go-expirecache"
	pickle "github.com/lomik/og-rek"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

// Use the real HTTP backend, queue, evaluator and serializer so counting a fetch
// also proves duplicate requests did not independently decode/evaluate data.
func coalescingApp(t *testing.T, fetch func(http.ResponseWriter, *http.Request)) *App {
	t.Helper()
	functions.New(nil, zap.NewNop())
	server := httptest.NewServer(http.HandlerFunc(fetch))
	t.Cleanup(server.Close)
	config := cfg.DefaultAPIConfig()
	config.Timeouts.Global = 5 * time.Second
	config.ResolveGlobs = 0
	config.MaxConcurrentUpstreamRequests = 16
	config.ProcWorkers = 4
	z := cfg.DefaultZipperConfig()
	z.Backends = []string{server.URL}
	z.ProtocolBackends, z.BackendsByCluster, z.BackendsByDC = nil, nil, nil
	app := &App{config: config, ZipperConfig: z, ms: newPrometheusMetrics(config),
		queryCache: cache.NullCache{}, findCache: cache.NullCache{},
		TopLevelDomainCache: expirecache.New(0), defaultTimeZone: time.UTC,
		fastQ: make(chan *RenderReq, 64), slowQ: make(chan *RenderReq, 64)}
	app.ZipperMetrics = NewZipperPrometheusMetrics(z)
	var err error
	app.Backends, err = InitBackends(z, app.ZipperMetrics, &app.ms, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ProcessRequests(app, zap.NewNop())
	return app
}

func coalescingBody(w http.ResponseWriter, r *http.Request) {
	var metrics []types.Metric
	for _, target := range r.URL.Query()["target"] {
		metrics = append(metrics, types.Metric{Name: target, StartTime: 1000000001, StopTime: 1000000004, StepTime: 1,
			Values: []float64{1, 2, 3}, IsAbsent: []bool{false, true, false}})
	}
	body, _ := wire.RenderEncoder(metrics)
	w.Header().Set("Content-Type", contentTypeProtobuf)
	_, _ = w.Write(body)
}

func coalescingParams() url.Values {
	return url.Values{"target": {"scale(example.metric,2)"}, "from": {"1000000001"}, "until": {"1000000004"}, "format": {"json"}}
}

func coalescingRequest(app *App, ctx context.Context, params url.Values) <-chan *httptest.ResponseRecorder {
	ch := make(chan *httptest.ResponseRecorder, 1)
	r := httptest.NewRequest(http.MethodGet, "/render?"+params.Encode(), nil).WithContext(ctx)
	go func() {
		w := httptest.NewRecorder()
		app.renderHandler(w, r, zap.NewNop())
		ch <- w
	}()
	return ch
}

func coalescingReceive(t *testing.T, ch <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("render did not finish")
		return nil
	}
}

func coalescingWait(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("concurrent requests did not reach the expected state")
		}
		time.Sleep(time.Millisecond)
	}
}

func coalescingCount(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	_ = c.Write(m)
	return m.Counter.GetValue()
}

func TestRenderCoalesceConcurrentMissesAndLeaderCancellation(t *testing.T) {
	for _, cancelLeader := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "cancel leader"}[cancelLeader], func(t *testing.T) {
			var calls atomic.Int64
			release := make(chan struct{})
			app := coalescingApp(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				<-release
				coalescingBody(w, r)
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			leader := coalescingRequest(app, ctx, coalescingParams())
			coalescingWait(t, func() bool { return calls.Load() == 1 })
			var followers []<-chan *httptest.ResponseRecorder
			for range 15 {
				followers = append(followers, coalescingRequest(app, context.Background(), coalescingParams()))
			}
			coalescingWait(t, func() bool { return coalescingCount(app.ms.RenderCoalesced) == 15 })
			if cancelLeader {
				cancel()
				if got := coalescingReceive(t, leader); got.Code != 499 {
					t.Fatalf("cancelled leader: %d %s", got.Code, got.Body.String())
				}
			}
			close(release)
			var want []byte
			for _, response := range followers {
				got := coalescingReceive(t, response)
				if got.Code != 200 || !strings.Contains(got.Body.String(), "example.metric") {
					t.Fatalf("follower: %d %s", got.Code, got.Body.String())
				}
				if want == nil {
					want = got.Body.Bytes()
				} else if !bytes.Equal(got.Body.Bytes(), want) {
					t.Fatal("followers received different output")
				}
			}
			if !cancelLeader {
				if got := coalescingReceive(t, leader); got.Code != 200 || !bytes.Equal(got.Body.Bytes(), want) {
					t.Fatalf("leader output differs: %d %s", got.Code, got.Body.String())
				}
			}
			if calls.Load() != 1 || coalescingCount(app.ms.RenderLeaders) != 1 {
				t.Fatalf("got %d backend fetches, want one", calls.Load())
			}
		})
	}
}

func TestRenderCoalesceDifferentKeys(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	app := coalescingApp(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		coalescingBody(w, r)
	})
	var responses []<-chan *httptest.ResponseRecorder
	for _, change := range [][2]string{{}, {"target", "example.other"}, {"from", "1000000002"}, {"until", "1000000005"}, {"format", "raw"}, {"maxDataPoints", "1"}, {"tz", "UTC"}, {"cacheTimeout", "7"}, {"noCache", "1"}} {
		p := coalescingParams()
		if change[0] != "" {
			p.Set(change[0], change[1])
		}
		responses = append(responses, coalescingRequest(app, context.Background(), p))
	}
	coalescingWait(t, func() bool { return calls.Load() == int64(len(responses)) })
	close(release)
	for _, response := range responses {
		if got := coalescingReceive(t, response); got.Code != 200 {
			t.Fatalf("%d %s", got.Code, got.Body.String())
		}
	}
	if coalescingCount(app.ms.RenderCoalesced) != 0 {
		t.Fatal("different response-cache keys shared work")
	}
}

func TestRenderCoalesceErrorOnlySharedWithCurrentWaiters(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	app := coalescingApp(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		<-release
		if n == 1 {
			http.Error(w, "temporary backend error", 500)
		} else {
			coalescingBody(w, r)
		}
	})
	leader := coalescingRequest(app, context.Background(), coalescingParams())
	coalescingWait(t, func() bool { return calls.Load() == 1 })
	follower := coalescingRequest(app, context.Background(), coalescingParams())
	coalescingWait(t, func() bool { return coalescingCount(app.ms.RenderCoalesced) == 1 })
	close(release)
	a, b := coalescingReceive(t, leader), coalescingReceive(t, follower)
	if a.Code != 500 || b.Code != 500 || !bytes.Equal(a.Body.Bytes(), b.Body.Bytes()) {
		t.Fatalf("error was not shared: %d %q / %d %q", a.Code, a.Body.String(), b.Code, b.Body.String())
	}
	if got := coalescingReceive(t, coalescingRequest(app, context.Background(), coalescingParams())); got.Code != 200 || calls.Load() != 2 {
		t.Fatalf("completed error retained: %d, %d fetches", got.Code, calls.Load())
	}
}

func TestRenderCoalesceOutputParity(t *testing.T) {
	for _, format := range []string{"json", "raw", "csv", "pickle", "protobuf", "protobuf3"} {
		t.Run(format, func(t *testing.T) {
			var calls atomic.Int64
			release := make(chan struct{})
			app := coalescingApp(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) > 1 {
					<-release
				}
				coalescingBody(w, r)
			})
			p := coalescingParams()
			p.Set("format", format)
			p.Set("maxDataPoints", "1")
			p.Set("noCache", "1")
			want := coalescingReceive(t, coalescingRequest(app, context.Background(), p))
			p.Del("noCache")
			leader := coalescingRequest(app, context.Background(), p)
			coalescingWait(t, func() bool { return calls.Load() == 2 })
			follower := coalescingRequest(app, context.Background(), p)
			coalescingWait(t, func() bool { return coalescingCount(app.ms.RenderCoalesced) == 1 })
			close(release)
			got := coalescingReceive(t, leader)
			shared := coalescingReceive(t, follower)
			if got.Code != 200 || want.Code != 200 || shared.Code != 200 || !bytes.Equal(shared.Body.Bytes(), got.Body.Bytes()) {
				t.Fatalf("coalesced bytes differ: %d %q / %d %q", got.Code, got.Body.String(), shared.Code, shared.Body.String())
			}
			if format == "pickle" {
				// Existing pickle serialization iterates a Go map: independent
				// renders may order dictionary keys differently. The shared bytes
				// above must still match exactly; compare the baseline's values.
				a, errA := pickle.NewDecoder(bytes.NewReader(want.Body.Bytes())).Decode()
				b, errB := pickle.NewDecoder(bytes.NewReader(got.Body.Bytes())).Decode()
				if errA != nil || errB != nil || !reflect.DeepEqual(a, b) {
					t.Fatalf("pickle values changed: %v / %v", a, b)
				}
			} else if !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
				t.Fatalf("%s output changed: %q / %q", format, got.Body.String(), want.Body.String())
			}
			if got.Header().Get("Content-Type") != want.Header().Get("Content-Type") {
				t.Fatal("content type changed")
			}
		})
	}
}

func TestRenderCoalesceCacheKeyAndJSONP(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	app := coalescingApp(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		coalescingBody(w, r)
	})
	p := coalescingParams()
	p.Set("jsonp", "first")
	leader := coalescingRequest(app, context.Background(), p)
	coalescingWait(t, func() bool { return calls.Load() == 1 })
	p.Set("jsonp", "second")
	p.Set("_salt", "ignored")
	p.Set("_ts", "ignored")
	p.Set("_t", "ignored")
	r := httptest.NewRequest("GET", "/render?"+p.Encode(), nil)
	form, err := app.renderHandlerProcessForm(r, &carbonapipb.AccessLogDetails{}, zap.NewNop())
	if err != nil || form.cacheKey != coalescingParams().Encode() {
		t.Fatalf("response-cache key changed: %q / %v", form.cacheKey, err)
	}
	follower := coalescingRequest(app, context.Background(), p)
	coalescingWait(t, func() bool { return coalescingCount(app.ms.RenderCoalesced) == 1 })
	close(release)
	a, b := coalescingReceive(t, leader), coalescingReceive(t, follower)
	if a.Code != 200 || b.Code != 200 || !strings.HasPrefix(a.Body.String(), "first(") || !strings.HasPrefix(b.Body.String(), "second(") || calls.Load() != 1 {
		t.Fatalf("JSONP callbacks not independent: %q / %q", a.Body.String(), b.Body.String())
	}
}

func TestRenderCoalesceAbandonedFlightAndPanic(t *testing.T) {
	var g renderFlights
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	first := g.join("key", ctx, time.Second, func(ctx context.Context) renderResult {
		<-ctx.Done()
		close(stopped)
		return renderResult{}
	})
	cancel()
	_ = g.wait(ctx, "key", first.call)
	<-stopped
	next := g.join("key", context.Background(), time.Second, func(context.Context) renderResult { panic("test") })
	if !next.leader {
		t.Fatal("new caller joined an abandoned call")
	}
	got := g.wait(context.Background(), "key", next.call)
	if got.status != 500 || !strings.Contains(got.message, "test") {
		t.Fatalf("panic did not release waiters: %+v", got)
	}
}

type publishingRenderCache struct {
	mu      sync.Mutex
	body    []byte
	setting chan struct{}
	release chan struct{}
}

func (c *publishingRenderCache) Get(string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.body == nil {
		return nil, cache.ErrNotFound
	}
	return c.body, nil
}

func (c *publishingRenderCache) Set(_ string, body []byte, _ int32) error {
	close(c.setting)
	<-c.release
	c.mu.Lock()
	c.body = body
	c.mu.Unlock()
	return nil
}

func TestRenderCoalescePublishesCacheBeforeReleasingWaiters(t *testing.T) {
	var calls atomic.Int64
	app := coalescingApp(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		coalescingBody(w, r)
	})
	c := &publishingRenderCache{setting: make(chan struct{}), release: make(chan struct{})}
	app.queryCache = c
	leader := coalescingRequest(app, context.Background(), coalescingParams())
	<-c.setting
	follower := coalescingRequest(app, context.Background(), coalescingParams())
	coalescingWait(t, func() bool { return coalescingCount(app.ms.RenderCoalesced) == 1 })
	close(c.release)
	for _, response := range []<-chan *httptest.ResponseRecorder{leader, follower, coalescingRequest(app, context.Background(), coalescingParams())} {
		if got := coalescingReceive(t, response); got.Code != 200 {
			t.Fatalf("%d %s", got.Code, got.Body.String())
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("cache publication raced: %d backend fetches", calls.Load())
	}
}
