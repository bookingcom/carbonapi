package carbonapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bookingcom/carbonapi/cache"
	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	types "github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/json"
	"github.com/pkg/errors"

	"github.com/lomik/zapwriter"
)

// TODO (grzkv) Clean this
var testApp *App

func find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	return getMetricGlobResponse(request.Query), nil
}

func info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	return getMockInfoResponse(), nil
}

func getMockInfoResponse() []types.Info {
	return []types.Info{
		types.Info{
			Host:              "http://127.0.0.1:8080",
			Name:              "foo.bar",
			AggregationMethod: "Average",
			MaxRetention:      157680000,
			XFilesFactor:      0.5,
			Retentions: []types.Retention{
				types.Retention{
					SecondsPerPoint: 60,
					NumberOfPoints:  43200,
				},
			},
		},
	}
}

func render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return []types.Metric{
		types.Metric{
			Name:      "foo.bar",
			StartTime: 1510913280,
			StopTime:  1510913880,
			StepTime:  60,
			Values:    []float64{0, 1510913759, 1510913818},
			IsAbsent:  []bool{true, false, false},
		},
	}, nil
}

func renderErr(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return []types.Metric{
		types.Metric{},
	}, errors.New("error during render")
}

func renderErrNotFound(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return []types.Metric{
		types.Metric{},
	}, types.ErrMetricsNotFound
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
				types.Match{
					Path:   "foo.bat",
					IsLeaf: true,
				},
			},
		}
	}

	return types.Matches{}
}

func init() {
	testApp = setUpTestConfig()
}

func setUpTestConfig() *App {
	c := cfg.GetDefaultLoggerConfig()
	c.Level = "none"
	zapwriter.ApplyConfig([]zapwriter.Config{c})
	logger := zapwriter.Logger("main")

	config := cfg.DefaultAPIConfig()

	// TODO (grzkv): Should use New
	app := &App{
		config:            config,
		queryCache:        cache.NewMemcached("capi", ``),
		findCache:         cache.NewExpireCache(1000),
		prometheusMetrics: newPrometheusMetrics(config),
	}
	app.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: render,
	})

	app.config.ConcurrencyLimitPerServer = 1024

	setUpConfig(app, logger)
	initHandlers(app)

	return app
}

// TODO (grzkv) Enable this after we get rid of global state
// func newAppWithRenerErrs() *App {
// 	c := cfg.GetDefaultLoggerConfig()
// 	c.Level = "none"
// 	zapwriter.ApplyConfig([]zapwriter.Config{c})
// 	logger := zapwriter.Logger("main")
//
// 	config := cfg.DefaultAPIConfig()
//
// 	// TODO (grzkv): Should use New
// 	app := &App{
// 		config:            config,
// 		queryCache:        cache.NewMemcached("capi", ``),
// 		findCache:         cache.NewExpireCache(1000),
// 		prometheusMetrics: newPrometheusMetrics(config),
// 	}
// 	app.backend = mock.New(mock.Config{
// 		Find:   find,
// 		Info:   info,
// 		Render: renderErr,
// 	})
//
// 	app.config.ConcurrencyLimitPerServer = 1024
//
// 	setUpConfig(app, logger)
// 	initHandlers(app)
//
// 	return app
// }

func setUpRequest(t *testing.T, url string) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}

	return req
}

func TestRenderHandler(t *testing.T) {
	req := setUpRequest(t, "/render/?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1")
	rr := httptest.NewRecorder()

	// WARNING: Test results depend on the order of execution now. ENJOY THE GLOBAL STATE!!!
	// TODO (grzkv): Fix this
	testApp.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: render,
	})

	testApp.renderHandler(rr, req)

	expected := `[{"target":"foo.bar","datapoints":[[null,1510913280],[1510913759,1510913340],[1510913818,1510913400]]}]`

	if rr.Code != http.StatusOK {
		t.Error("HttpStatusCode should be 200 OK.")
	}
	if expected != rr.Body.String() {
		t.Error("Http response should be same.")
	}
}

func TestRenderHandlerErrs(t *testing.T) {
	req := setUpRequest(t, "/render/?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1")
	rr := httptest.NewRecorder()

	// WARNING: Test results depend on the order of execution now. ENJOY THE GLOBAL STATE!!!
	// TODO (grzkv): Fix this
	testApp.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: renderErr,
	})

	testApp.renderHandler(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("Expected status code %d, got %d", http.StatusInternalServerError, rr.Code)
	}
}

func TestRenderHandlerNotFoundErrs(t *testing.T) {
	req := setUpRequest(t, "/render/?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1")
	rr := httptest.NewRecorder()

	// WARNING: Test results depend on the order of execution now. ENJOY THE GLOBAL STATE!!!
	// TODO (grzkv): Fix this
	testApp.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: renderErrNotFound,
	})

	testApp.renderHandler(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, rr.Code)
	}
}
func TestFindHandler(t *testing.T) {
	req := setUpRequest(t, "/metrics/find/?query=foo.bar&format=json")
	rr := httptest.NewRecorder()
	testApp.findHandler(rr, req)

	body := rr.Body.String()
	// LOL this test is so fragile
	// TODO (grzkv): It can be made not-fragile by unmarshalling first
	// ...or using JSONEq, but this would be bloat
	expected := "[{\"allowChildren\":0,\"context\":{},\"expandable\":0,\"id\":\"foo.bar\",\"leaf\":1,\"text\":\"bar\"}]"
	if rr.Code != http.StatusOK {
		t.Error("HttpStatusCode should be 200 OK.")
	}
	if body != expected {
		t.Error("Http response should be same.")
	}
}

func TestFindHandlerCompleter(t *testing.T) {
	testMetrics := []string{"foo.b/", "foo.bar"}
	for _, testMetric := range testMetrics {
		req := setUpRequest(t, "/metrics/find/?query="+testMetric+"&format=completer")
		rr := httptest.NewRecorder()
		testApp.findHandler(rr, req)
		body := rr.Body.String()
		expectedValue, _ := findCompleter(getMetricGlobResponse(getCompleterQuery(testMetric)))
		if rr.Code != http.StatusOK {
			t.Error("HttpStatusCode should be 200 OK.")
		}
		if string(expectedValue) != body {
			t.Error("HTTP response should be same.")
		}
	}
}

func TestInfoHandler(t *testing.T) {
	req := setUpRequest(t, "/info/?target=foo.bar&format=json")
	rr := httptest.NewRecorder()
	testApp.infoHandler(rr, req)

	body := rr.Body.String()
	expected := getMockInfoResponse()
	expectedJSON, err := json.InfoEncoder(expected)
	if err != nil {
		t.Errorf("err should be nil, %v instead", err)
	}

	if rr.Code != http.StatusOK {
		t.Error("Http response should be same.")
	}
	// TODO (grzkv): Unmarshal for reliablility
	if string(expectedJSON) != body {
		t.Error("Http response should be same.")
	}
}
