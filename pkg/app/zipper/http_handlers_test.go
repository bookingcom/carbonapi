package zipper

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	types "github.com/bookingcom/carbonapi/pkg/types"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

func newTestApp() (*App, *PrometheusMetrics, *zap.Logger) {
	lg, _ := zap.NewDevelopment()
	config := cfg.DefaultZipperConfig()

	ms := NewPrometheusMetrics(config)

	bs, err := InitBackends(config, ms, lg)
	if err != nil {
		lg.Fatal("Failed to initialize backends", zap.Error(err))
	}

	app := App{
		Config:              config,
		Metrics:             NewPrometheusMetrics(config),
		Backends:            bs,
		TopLevelDomainCache: expirecache.New(0),
	}

	return &app, ms, lg
}

// RENDER ENDPOINT

func TestRenderNoBackends(t *testing.T) {
	app, ms, lg := newTestApp()
	defer lg.Sync()

	var tt = []struct {
		path string
		code int
	}{
		{"/render", http.StatusBadRequest},
		{"/render?from=111", http.StatusBadRequest},
		{"/render?from=111&until=111", http.StatusBadRequest},
		{"/render?target=foo.bar&from=111&until=111", http.StatusOK},
	}

	for _, tst := range tt {
		w := httptest.NewRecorder()
		req, err := http.NewRequest("GET", tst.path, nil)
		if err != nil {
			t.Fatalf("error making request %v", err)
		}

		app.renderHandler(w, req, ms, lg)
		if w.Code != tst.code {
			t.Fatalf("got code %d expected %d", w.Code, tst.code)
		}
	}
}

func TestRenderSingleBackend(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusBadRequest)
	}

	req, err = http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	w = httptest.NewRecorder()
	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestRenderSingleGenericBackendError(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusInternalServerError)
	}
}

func TestRenderSingleNotFoundBackendError(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusNotFound {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusNotFound)
	}
}

func TestRenderMultipleBackends(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestRenderMultipleBackendsSomeErrors(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestRenderMultipleBackendsAllNotfoundErrors(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusNotFound {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusNotFound)
	}
}

func TestRenderMultipleBackendsAllMixedErrorsBelowThreshold(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusNotFound {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusNotFound)
	}
}

func TestRenderMultipleBackendsAllErrorsMajorityOther(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithGenericError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: renderWithNotFoundError,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=foo.bar&from=1110&until=1111", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.renderHandler(w, req, ms, lg)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusInternalServerError)
	}
}

// FIND ENDPOINT

func TestFindNoBackends(t *testing.T) {
	app, ms, lg := newTestApp()
	defer lg.Sync()

	var tt = []struct {
		path string
		code int
	}{
		{"/metrics/find/", http.StatusOK},
		{"/metrics/find/?from=111", http.StatusOK},
		{"/metrics/find/?query=a.b.c", http.StatusOK},
		{"/metrics/find/?query=a.b.c&format=json", http.StatusOK},
		// TODO (grzkv): We probably want BadRequest here
		{"/metrics/find/?query=a.b.c&format=badformat", http.StatusInternalServerError},
	}

	for _, tst := range tt {
		w := httptest.NewRecorder()
		req, err := http.NewRequest("GET", tst.path, nil)
		if err != nil {
			t.Fatalf("error making request %v", err)
		}

		app.findHandler(w, req, ms, lg)
		if w.Code != tst.code {
			t.Fatalf("got code %d expected %d for %s", w.Code, tst.code, tst.path)
		}
	}
}

func TestFindSingleBackend(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	// TODO (grzkv): This should be BadRequest
	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}

	req, err = http.NewRequest("GET", "/metrics/find?query=foo.bar&format=json", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	w = httptest.NewRecorder()
	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}

	if w.Body.String() != `[{"allowChildren":0,"context":{},"expandable":0,"id":"foo.bar","leaf":1,"text":"bar"}]` {
		t.Fatalf("unexpected body")
	}
}

func TestFindSingleBackendWithGenericError(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusInternalServerError)
	}
}

func TestFindSingleBackendWithNotfoundError(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestFindManyBackendsAllNotfound(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestFindManyBackendsAllErrorsNotFoundMajority(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestFindManyBackendsAllErrorsOthersMajority2(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusInternalServerError)
	}
}

func TestFindManyBackendsAllErrorsOthersMajoritySmallAmount(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusInternalServerError)
	}
}

func TestFindManyBackendsAllErrorsOthersMajority(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusInternalServerError)
	}
}

func TestFindManyBackendsSomeMixedErrors(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithNotfoundError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   findWithGenericError,
			Info:   info,
			Render: render,
		}),
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
	}

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics/find", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.findHandler(w, req, ms, lg)

	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d", w.Code, http.StatusOK)
	}
}

func TestInfoNoBackends(t *testing.T) {
	app, ms, lg := newTestApp()
	defer lg.Sync()

	var tt = []struct {
		path string
		code int
	}{
		{"/info", http.StatusBadRequest},
		{"/info?format=json", http.StatusBadRequest},
		{"/info?format=wrongformat", http.StatusBadRequest},
	}

	for _, tst := range tt {
		w := httptest.NewRecorder()
		req, err := http.NewRequest("GET", tst.path, nil)
		if err != nil {
			t.Fatalf("error making request %v", err)
		}

		app.infoHandler(w, req, ms, lg)
		if w.Code != tst.code {
			t.Fatalf("got code %d expected %d for %s", w.Code, tst.code, tst.path)
		}
	}
}
func TestInfoSingleBackend(t *testing.T) {

	app, ms, lg := newTestApp()
	defer lg.Sync()
	app.Backends = []backend.Backend{
		mock.New(mock.Config{
			Find:   find,
			Info:   info,
			Render: render,
		}),
	}

	var tests = []struct {
		path string
		code int
		body string
	}{
		{
			path: "/info",
			code: http.StatusBadRequest,
			body: "info: empty target\n",
		},
		{
			path: "/info?target=foo.bar",
			code: http.StatusOK,
			body: `{"http://127.0.0.1:8080":{"name":"foo.bar","aggregationMethod":"Average","maxRetention":157680000,"xFilesFactor":0.5,"retentions":[{"secondsPerPoint":60,"numberOfPoints":43200}]}}`,
		},
		{
			path: "/info?target=foo.bar&format=wrongformat",
			// TODO (grzkv) Should be BadRequest
			code: http.StatusInternalServerError,
			// TODO (grzkv) This is clearly wrong
			body: "error marshaling data\n",
		},
	}

	for _, tst := range tests {

		w := httptest.NewRecorder()
		req, err := http.NewRequest("GET", tst.path, nil)
		if err != nil {
			t.Fatalf("error making request %v", err)
		}

		app.infoHandler(w, req, ms, lg)

		if w.Code != tst.code {
			t.Fatalf("got code %d expected %d for %s", w.Code, tst.code, tst.path)
		}

		if w.Body.String() != tst.body {
			t.Fatalf("unexpected body: want %q got %q", tst.body, w.Body.String())
		}
	}
}

func TestLbCheckNoBackends(t *testing.T) {
	app, ms, lg := newTestApp()
	defer lg.Sync()
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/lb_check", nil)
	if err != nil {
		t.Fatalf("error making request %v", err)
	}

	app.lbCheckHandler(w, req, ms, lg)
	if w.Code != http.StatusOK {
		t.Fatalf("got code %d expected %d for %s", w.Code, http.StatusOK, "/lb_check")
	}

	if w.Body.String() != "Ok\n" {
		t.Fatalf("expected body Ok\\n, got %q", w.Body.String())
	}
}

// MOCK FUNCTIONS

func find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	return getMetricGlobResponse(request.Query), nil
}

func findWithGenericError(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	return getMetricGlobResponse(request.Query), errors.New("unexpected error")
}

func findWithNotfoundError(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	// we return this kind of error instead of generic ErrNotFound
	return getMetricGlobResponse(request.Query), types.ErrMatchesNotFound
}

func info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	return getMockInfoResponse(), nil
}

func getMockInfoResponse() []types.Info {
	return []types.Info{
		{
			Host:              "http://127.0.0.1:8080",
			Name:              "foo.bar",
			AggregationMethod: "Average",
			MaxRetention:      157680000,
			XFilesFactor:      0.5,
			Retentions: []types.Retention{
				{
					SecondsPerPoint: 60,
					NumberOfPoints:  43200,
				},
			},
		},
	}
}

func render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return []types.Metric{
		{
			Name:      "foo.bar",
			StartTime: 1510913280,
			StopTime:  1510913880,
			StepTime:  60,
			Values:    []float64{0, 1510913759, 1510913818},
			IsAbsent:  []bool{true, false, false},
		},
	}, nil
}

func renderWithNotFoundError(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return make([]types.Metric, 0), types.ErrMetricsNotFound
}

func renderWithGenericError(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return make([]types.Metric, 0), errors.New("some error")
}

func getMetricGlobResponse(metric string) types.Matches {
	match := types.Match{
		Path:   metric,
		IsLeaf: true,
	}

	switch metric {
	case "foo.bar*":
		return types.Matches{
			Name:    "foo.bar",
			Matches: []types.Match{match},
		}

	case "foo.bar":
		return types.Matches{
			Name:    "foo.bar",
			Matches: []types.Match{match},
		}

	case "foo.b*":
		return types.Matches{
			Name: "foo.b",
			Matches: []types.Match{
				match,
				{
					Path:   "foo.bat",
					IsLeaf: true,
				},
			},
		}
	}

	return types.Matches{}
}
