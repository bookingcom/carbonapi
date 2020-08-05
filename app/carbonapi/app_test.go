package carbonapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bookingcom/carbonapi/blocker"
	"github.com/bookingcom/carbonapi/cache"
	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	types "github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/json"

	"github.com/lomik/zapwriter"
)

// TODO (grzkv) Clean this
var testApp *App
var testRouter http.Handler

func find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	return getMetricGlobResponse(request.Query), nil
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

func renderErr(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return []types.Metric{
		{},
	}, fmt.Errorf("error during render")
}

func renderErrNotFound(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return []types.Metric{
		{},
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
				{
					Path:   "foo.bat",
					IsLeaf: true,
				},
			},
		}
	}

	return types.Matches{}
}

func TestMain(m *testing.M) {
	testApp, testRouter = SetUpTestConfig()
	testServer := httptest.NewServer(testRouter)
	code := m.Run()
	testServer.Close()
	os.Exit(code)

}

func TestAppHandlers(t *testing.T) {
	t.Run("RenderHandler", renderHandler)
	t.Run("RenderHandlerErrors", renderHandlerErrs)
	t.Run("RenderHandlerNotFoundErrors", renderHandlerNotFoundErrs)
	t.Run("FindHandler", findHandler)
	t.Run("FindHandlerCompleter", findHandlerCompleter)
	t.Run("RenderHandlerNotFoundErrors", infoHandler)
}

func SetUpTestConfig() (*App, http.Handler) {
	c := cfg.GetDefaultLoggerConfig()
	c.Level = "none"
	zapwriter.ApplyConfig([]zapwriter.Config{c})
	logger := zapwriter.Logger("main")

	config := cfg.DefaultAPIConfig()

	// TODO (grzkv): Should use New
	app := &App{
		config:            config,
		queryCache:        cache.NewMemcached("capi", 50, ""),
		findCache:         cache.NewExpireCache(1000),
		prometheusMetrics: newPrometheusMetrics(config),
	}
	app.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: render,
	})

	app.config.ConcurrencyLimitPerServer = 1024

	app.requestBlocker = blocker.NewRequestBlocker(config.BlockHeaderFile, config.BlockHeaderUpdatePeriod, logger)

	setUpConfig(app, logger)
	handler := initHandlers(app)
	return app, handler
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

func renderHandler(t *testing.T) {
	req := httptest.NewRequest("GET",
		"/render?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1", nil)
	rr := httptest.NewRecorder()

	// WARNING: Test results depend on the order of execution now. ENJOY THE GLOBAL STATE!!!
	// TODO (grzkv): Fix this
	testApp.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: render,
	})

	testRouter.ServeHTTP(rr, req)

	expected := `[{"target":"foo.bar","datapoints":[[null,1510913280],[1510913759,1510913340],[1510913818,1510913400]]}]`

	if rr.Code != http.StatusOK {
		t.Error("HttpStatusCode should be 200 OK.")
	}
	if expected != rr.Body.String() {
		t.Error("Http response should be same.")
	}
}

func renderHandlerErrs(t *testing.T) {
	tests := []struct {
		req     string
		expCode int
	}{
		{
			req:     "/render/?target=foo.bar&from=-10minutes&format=json&noCache=1",
			expCode: http.StatusInternalServerError,
		},
		{
			req:     "/render/?target=sum(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1",
			expCode: http.StatusInternalServerError,
		},
		{
			req:     "/render/?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1",
			expCode: http.StatusInternalServerError,
		},
		{
			req:     "/render/?target=max(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1",
			expCode: http.StatusInternalServerError,
		},
	}

	for _, tst := range tests {
		t.Run(tst.req, func(t *testing.T) {
			req := httptest.NewRequest("GET", tst.req, nil)
			rr := httptest.NewRecorder()

			// WARNING: Test results depend on the order of execution now. ENJOY THE GLOBAL STATE!!!
			// TODO (grzkv): Fix this
			testApp.backend = mock.New(mock.Config{
				Find:   find,
				Info:   info,
				Render: renderErr,
			})

			testRouter.ServeHTTP(rr, req)

			if rr.Code != tst.expCode {
				t.Errorf("Expected status code %d, got %d", tst.expCode, rr.Code)
			}
		})
	}
}

func renderHandlerNotFoundErrs(t *testing.T) {
	req := httptest.NewRequest("GET",
		"/render/?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1", nil)
	rr := httptest.NewRecorder()

	// WARNING: Test results depend on the order of execution now. ENJOY THE GLOBAL STATE!!!
	// TODO (grzkv): Fix this
	testApp.backend = mock.New(mock.Config{
		Find:   find,
		Info:   info,
		Render: renderErrNotFound,
	})

	testRouter.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, rr.Code)
	}
}

func findHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/metrics/find/?query=foo.bar&format=json", nil)
	rr := httptest.NewRecorder()
	testRouter.ServeHTTP(rr, req)

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

func findHandlerCompleter(t *testing.T) {
	testMetrics := []string{"foo.b/", "foo.bar"}
	for _, testMetric := range testMetrics {
		req := httptest.NewRequest("GET", "/metrics/find/?query="+testMetric+"&format=completer", nil)
		rr := httptest.NewRecorder()
		testRouter.ServeHTTP(rr, req)
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

func infoHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/info/?target=foo.bar&format=json", nil)
	rr := httptest.NewRecorder()

	testRouter.ServeHTTP(rr, req)

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
