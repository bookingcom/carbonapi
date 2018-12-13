package carbonapi

import (
	"context"
	"encoding/json"
	"github.com/bookingcom/carbonapi/cache"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/expr/types"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"

	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
)

var testApp *App

type mockCarbonZipper struct {
}

func newMockCarbonZipper() *mockCarbonZipper {
	z := &mockCarbonZipper{}

	return z
}

func (z mockCarbonZipper) Find(ctx context.Context, metric string) (dataTypes.Matches, error) {
	return getMetricGlobResponse(metric), nil
}

func (z mockCarbonZipper) Info(ctx context.Context, metric string) (map[string]pb.InfoResponse, error) {
	response := getMockInfoResponse()

	return response, nil
}

func (z mockCarbonZipper) Render(ctx context.Context, metric string, from, until int32) ([]*types.MetricData, error) {
	var result []*types.MetricData
	multiFetchResponse := getMultiFetchResponse()
	result = append(result, &types.MetricData{FetchResponse: multiFetchResponse.Metrics[0]})
	return result, nil
}

func getMetricGlobResponse(metric string) dataTypes.Matches {
	match := dataTypes.Match{
		Path:   metric,
		IsLeaf: true,
	}

	switch metric {
	case "foo.bar*":
		return dataTypes.Matches{
			Name:    "foo.bar",
			Matches: []dataTypes.Match{match},
		}

	case "foo.bar":
		return dataTypes.Matches{
			Name:    "foo.bar",
			Matches: []dataTypes.Match{match},
		}

	case "foo.b*":
		return dataTypes.Matches{
			Name: "foo.b",
			Matches: []dataTypes.Match{
				match,
				dataTypes.Match{
					Path:   "foo.bat",
					IsLeaf: true,
				},
			},
		}
	}

	return dataTypes.Matches{}
}

func getMultiFetchResponse() pb.MultiFetchResponse {
	mfr := pb.FetchResponse{
		Name:      "foo.bar",
		StartTime: 1510913280,
		StopTime:  1510913880,
		StepTime:  60,
		Values:    []float64{0, 1510913759, 1510913818},
		IsAbsent:  []bool{true, false, false},
	}

	result := pb.MultiFetchResponse{Metrics: []pb.FetchResponse{mfr}}
	return result
}

func getMockInfoResponse() map[string]pb.InfoResponse {
	decoded := make(map[string]pb.InfoResponse)
	r := pb.Retention{
		SecondsPerPoint: 60,
		NumberOfPoints:  43200,
	}
	d := pb.InfoResponse{
		Name:              "foo.bar",
		AggregationMethod: "Average",
		MaxRetention:      157680000,
		XFilesFactor:      0.5,
		Retentions:        []pb.Retention{r},
	}
	decoded["http://127.0.0.1:8080"] = d
	return decoded
}

func init() {
	testApp = setUpTestConfig()
}

func setUpTestConfig() *App {
	c := cfg.DefaultLoggerConfig
	c.Level = "none"
	zapwriter.ApplyConfig([]zapwriter.Config{c})
	logger := zapwriter.Logger("main")
	app := App{config: cfg.API{},
		queryCache: cache.NewMemcached("capi", ``),
		findCache:  cache.NewExpireCache(1000),
	}
	app.config.Backends = []string{"http://127.0.0.1:8080"}
	app.config.ConcurrencyLimitPerServer = 1024
	setUpConfigUpstreams(logger, &app)
	setUpConfig(logger, newMockCarbonZipper(), &app)
	initHandlers(&app)
	return &app
}

func setUpRequest(t *testing.T, url string) (*http.Request, *httptest.ResponseRecorder) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	return req, rr
}

func TestRenderHandler(t *testing.T) {
	req, rr := setUpRequest(t, "/render/?target=fallbackSeries(foo.bar,foo.baz)&from=-10minutes&format=json&noCache=1")
	testApp.renderHandler(rr, req)

	expected := `[{"target":"foo.bar","datapoints":[[null,1510913280],[1510913759,1510913340],[1510913818,1510913400]]}]`

	// Check the status code is what we expect.
	r := assert.Equal(t, rr.Code, http.StatusOK, "HttpStatusCode should be 200 OK.")
	if !r {
		t.Error("HttpStatusCode should be 200 OK.")
	}
	r = assert.Equal(t, expected, rr.Body.String(), "Http response should be same.")
	if !r {
		t.Error("Http response should be same.")
	}
}

func TestFindHandler(t *testing.T) {
	req, rr := setUpRequest(t, "/metrics/find/?query=foo.bar&format=json")
	testApp.findHandler(rr, req)

	body := rr.Body.String()
	expected, _ := findTreejson(getMetricGlobResponse("foo.bar"))
	r := assert.Equal(t, rr.Code, http.StatusOK, "HttpStatusCode should be 200 OK.")
	if !r {
		t.Error("HttpStatusCode should be 200 OK.")
	}
	r = assert.Equal(t, string(expected), body, "Http response should be same.")
	if !r {
		t.Error("Http response should be same.")
	}
}

func TestFindHandlerCompleter(t *testing.T) {
	testMetrics := []string{"foo.b/", "foo.bar"}
	for _, testMetric := range testMetrics {
		req, rr := setUpRequest(t, "/metrics/find/?query="+testMetric+"&format=completer")
		testApp.findHandler(rr, req)
		body := rr.Body.String()
		expectedValue, _ := findCompleter(getMetricGlobResponse(getCompleterQuery(testMetric)))
		r := assert.Equal(t, rr.Code, http.StatusOK, "HttpStatusCode should be 200 OK.")
		if !r {
			t.Error("HttpStatusCode should be 200 OK.")
		}
		r = assert.Equal(t, string(expectedValue), body, "Http response should be same.")
		if !r {
			t.Error("Http response should be same.")
		}
	}
}

func TestInfoHandler(t *testing.T) {
	req, rr := setUpRequest(t, "/info/?target=foo.bar&format=json")
	testApp.infoHandler(rr, req)

	body := rr.Body.String()
	expected := getMockInfoResponse()
	expectedJson, err := json.Marshal(expected)
	r := assert.Nil(t, err)
	if !r {
		t.Errorf("err should be nil, %v instead", err)
	}

	r = assert.Equal(t, rr.Code, http.StatusOK, "HttpStatusCode should be 200 OK.")
	if !r {
		t.Error("Http response should be same.")
	}
	r = assert.Equal(t, string(expectedJson), body, "Http response should be same.")
	if !r {
		t.Error("Http response should be same.")
	}
}
