package carbonapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/expr/functions"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

type partialRenderBackend struct {
	backend.BackendImpl
	path  string
	err   error
	empty bool
	// Leave cluster empty for the strict, ungrouped-backend regression tests.
	cluster string
	dc      string
}

func (b *partialRenderBackend) Contains(paths []string) bool {
	for _, path := range paths {
		if path == b.path {
			return true
		}
	}
	return false
}
func (b *partialRenderBackend) Logger() *zap.Logger      { return zap.NewNop() }
func (b *partialRenderBackend) GetServerAddress() string { return b.path }
func (b *partialRenderBackend) BackendInfo() (string, string, string) {
	return b.path, b.cluster, b.dc
}
func (b *partialRenderBackend) Render(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
	if b.err != nil || b.empty {
		return nil, b.err
	}
	value := 99.0
	if b.path == "m.ko" {
		value = 1
	}
	m := types.Metric{Name: b.path, StartTime: r.From, StopTime: r.Until, StepTime: 60}
	for ts := r.From; ts < r.Until; ts += 60 {
		m.Values = append(m.Values, value)
		m.IsAbsent = append(m.IsAbsent, false)
	}
	return []types.Metric{m}, nil
}

func partialRenderApp(t *testing.T, stores ...*partialRenderBackend) *App {
	t.Helper()
	functions.New(nil, zap.NewNop())
	c := cfg.DefaultAPIConfig()
	c.ResolveGlobs = 0
	c.MaxConcurrentUpstreamRequests, c.ProcWorkers = 4, 4
	c.Timeouts.Global = 5 * time.Second
	z := cfg.DefaultZipperConfig()
	app := &App{config: c, ZipperConfig: z, ms: newPrometheusMetrics(c),
		queryCache: cache.NullCache{}, findCache: cache.NullCache{},
		TopLevelDomainCache: expirecache.New(0), defaultTimeZone: time.UTC,
		fastQ: make(chan *RenderReq, 64), slowQ: make(chan *RenderReq, 64)}
	app.ZipperMetrics = NewZipperPrometheusMetrics(z)
	for _, store := range stores {
		app.Backends = append(app.Backends, backend.NewBackend(store, 32, 16,
			app.ZipperMetrics.BackendRequestsInQueue, app.ZipperMetrics.BackendSemaphoreSaturation,
			app.ZipperMetrics.BackendTimeInQSec, app.ZipperMetrics.BackendEnqueuedRequests,
			app.ms.BackendDuration.MustCurryWith(map[string]string{"dc": "test", "cluster": "test"})))
	}
	ProcessRequests(app, zap.NewNop())
	return app
}

func partialRenderRequest(app *App, targets ...string) *httptest.ResponseRecorder {
	params := url.Values{"format": {"json"}, "from": {"1794392400"}, "until": {"1794393000"}, "target": targets}
	w := httptest.NewRecorder()
	app.renderHandler(w, httptest.NewRequest(http.MethodGet, "/render?"+params.Encode(), nil), zap.NewNop())
	return w
}

const partialRenderRatio = "divideSeries(transformNull(sumSeries(m.ko,scale(sumSeries(m.ko,m.ok),0)),0),transformNull(sumSeries(m.ko,m.ok),0))"

func assertPartialRenderValue(t *testing.T, w *httptest.ResponseRecorder, want float64) {
	t.Helper()
	var series []struct {
		Datapoints [][]float64 `json:"datapoints"`
	}
	if w.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &series); err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Datapoints) == 0 {
		t.Fatalf("expected one nonempty series: %s", w.Body.String())
	}
	for _, point := range series[0].Datapoints {
		if math.Abs(point[0]-want) > 1e-9 {
			t.Fatalf("got %v, want %v", point, want)
		}
	}
}

func TestRenderRequiredSeriesFailure(t *testing.T) {
	timeout := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	for _, tc := range []struct {
		name      string
		stores    []*partialRenderBackend
		wantError bool
	}{
		{"healthy", []*partialRenderBackend{{path: "m.ok"}}, false},
		{"timeout", []*partialRenderBackend{{path: "m.ok", err: timeout}}, true},
		{"server_error", []*partialRenderBackend{{path: "m.ok", err: errors.New("upstream HTTP 503")}}, true},
		{"timeout_and_two_not_found", []*partialRenderBackend{{path: "m.ok", err: timeout}, {path: "m.ok", err: types.ErrMetricsNotFound}, {path: "m.ok", err: types.ErrMetricsNotFound}}, true},
		{"timeout_and_empty_success", []*partialRenderBackend{{path: "m.ok", err: timeout}, {path: "m.ok", empty: true}}, true},
		{"healthy_replica_recovers_timeout", []*partialRenderBackend{{path: "m.ok", err: timeout}, {path: "m.ok"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := partialRenderApp(t, append(tc.stores, &partialRenderBackend{path: "m.ko"})...)
			w := partialRenderRequest(app, partialRenderRatio)
			if tc.wantError {
				if w.Code != http.StatusInternalServerError {
					t.Fatalf("HTTP %d, want 500: %s", w.Code, w.Body.String())
				}
			} else {
				assertPartialRenderValue(t, w, 0.01)
			}
			// The grouped path uses the same replica recovery rules.
			replies := sendGroupedRenderRequests(app, context.Background(), []string{"m.ok", "m.ko"}, 1794392400, 1794393000, &carbonapipb.AccessLogDetails{}, zap.NewNop())
			if (replies[0].error != nil) != tc.wantError || replies[1].error != nil {
				t.Fatalf("grouped replies: OK error %v, KO error %v", replies[0].error, replies[1].error)
			}
		})
	}
}

func TestRenderBurnRateRequiredSeriesFailure(t *testing.T) {
	// Full long/short burn-rate expressions with neutral leaf names. Preserve
	// the nested zero-fill, moving sums, aliases and error-budget calculation.
	const formula = `alias(diffSeries(alias(divideSeries(alias(movingSum(alias(transformNull(sumSeries(m.ko, scale(sumSeries(
    m.ko,
    m.ok
  ), 0)), 0), 'input_exp_error'), '%s'), 'calc_%s_error'), alias(movingSum(alias(transformNull(sumSeries(
    m.ko,
    m.ok
  ), 0), 'input_exp_total'), '%s'), 'calc_%s_total')), 'calc_%s_error_rate'), scale(alias(keepLastValue(offset(scale(transformNull(sumSeries(
    m.ko,
    m.ok
  )), 0), 0.01)), 'input_exp_error_budget'), %s)), '%s_window_trigger')`
	for _, tc := range []struct {
		name, window, label, budget string
		healthyValue                float64
	}{
		{"realtime_long", "45m", "long", "14.4", -0.134},
		{"realtime_short", "10m", "short", "14.4", -0.134},
		{"batched_long", "6h", "long", "6.0", -0.05},
		{"batched_short", "30m", "short", "6.0", -0.05},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := fmt.Sprintf(formula, tc.window, tc.label, tc.window, tc.label, tc.label, tc.budget, tc.label)
			app := partialRenderApp(t, &partialRenderBackend{path: "m.ok"}, &partialRenderBackend{path: "m.ko"})
			assertPartialRenderValue(t, partialRenderRequest(app, target), tc.healthyValue)
			app = partialRenderApp(t, &partialRenderBackend{path: "m.ok", err: errors.New("dial tcp: i/o timeout")}, &partialRenderBackend{path: "m.ko"})
			w := partialRenderRequest(app, target)
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("HTTP %d, want execution error: %s", w.Code, w.Body.String())
			}
		})
	}
}

type partialRenderCache struct {
	cache.NullCache
	writes chan []byte
}

func (c *partialRenderCache) Set(_ string, body []byte, _ int32) error {
	c.writes <- append([]byte(nil), body...)
	return nil
}

func TestRenderMissingSeriesAndCache(t *testing.T) {
	for _, tc := range []struct {
		name, target string
		missingKO    bool
		want         float64
		wantCache    bool
	}{
		{"complete", partialRenderRatio, false, 0.01, true},
		{"optional_error_series", partialRenderRatio, true, 0, false},
		{"fallback", "fallbackSeries(m.missing,constantLine(7))", false, 7, false},
		{"sum_with_missing", "sumSeries(m.ok,m.missing)", false, 99, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ko := &partialRenderBackend{path: "m.ko"}
			if tc.missingKO {
				ko.err = types.ErrMetricsNotFound
			}
			app := partialRenderApp(t, &partialRenderBackend{path: "m.ok"}, ko, &partialRenderBackend{path: "m.missing", err: types.ErrMetricsNotFound})
			c := &partialRenderCache{writes: make(chan []byte, 8)}
			app.queryCache = c
			assertPartialRenderValue(t, partialRenderRequest(app, tc.target), tc.want)
			select {
			case body := <-c.writes:
				if !tc.wantCache {
					t.Fatalf("partial result cached: %s", body)
				}
			default:
				if tc.wantCache {
					t.Fatal("complete result was not cached")
				}
			}
		})
	}
}

func TestRenderFailureDoesNotPublishCache(t *testing.T) {
	app := partialRenderApp(t, &partialRenderBackend{path: "m.ok", err: errors.New("dial tcp: i/o timeout")}, &partialRenderBackend{path: "m.ko"})
	c := &partialRenderCache{writes: make(chan []byte, 8)}
	app.queryCache = c
	// Even a successful target processed first must not publish a partial response.
	w := partialRenderRequest(app, "m.ko", partialRenderRatio)
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), `"datapoints"`) {
		t.Fatalf("expected execution error, got HTTP %d: %s", w.Code, w.Body.String())
	}
	select {
	case body := <-c.writes:
		t.Fatalf("failed render cached: %s", body)
	default:
	}
}

func TestErrorsFanInNotFound(t *testing.T) {
	for _, tc := range []struct {
		name     string
		errs     []error
		notFound bool
	}{
		{"all_not_found", []error{types.ErrMetricsNotFound, types.ErrMetricsNotFound}, true},
		{"wrapped_not_found", []error{fmt.Errorf("backend: %w", types.ErrMetricsNotFound)}, true},
		{"timeout_plus_not_found", []error{context.DeadlineExceeded, types.ErrMetricsNotFound, types.ErrMetricsNotFound}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := errorsFanIn(tc.errs, len(tc.errs))
			var notFound types.ErrNotFound
			if err == nil || errors.As(err, &notFound) != tc.notFound {
				t.Fatalf("error = %v, want NotFound %v", err, tc.notFound)
			}
		})
	}
}
