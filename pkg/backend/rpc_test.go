package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/go-graphite/carbonapi/pkg/backend/mock"
	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/protobuf/carbonapi_v2"

	"go.uber.org/zap"
)

func TestCarbonapiv2InfosCorrectMerge(t *testing.T) {
	mURL := func(path string) *url.URL { return new(url.URL) }
	backends := []Backend{
		mock.New(func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
			infos := carbonapi_v2.Infos{
				Hosts: []string{"host_A"},
				Infos: []*carbonapi_v2.Info{
					&carbonapi_v2.Info{
						Name:              "metric",
						AggregationMethod: "sum",
					},
				},
			}

			return infos.Marshal()
		}, mURL),
		mock.New(func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
			infos := carbonapi_v2.Infos{
				Hosts: []string{"host_B"},
				Infos: []*carbonapi_v2.Info{
					&carbonapi_v2.Info{
						Name:              "metric",
						AggregationMethod: "average",
					},
				},
			}

			return infos.Marshal()
		}, mURL),
	}

	got, err := Infos(context.Background(), backends, "metric")
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != len(backends) {
		t.Errorf("Expected %d responses, got %d", len(backends), len(got))
		return
	}

	if got[0].AggregationMethod == got[1].AggregationMethod {
		t.Error("Expected different aggregation methods")
	}
}

func TestCarbonapiv2InfosError(t *testing.T) {
	mURL := func(path string) *url.URL { return new(url.URL) }
	mCall := func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
		return nil, errors.New("No")
	}

	backends := []Backend{mock.New(mCall, mURL)}

	_, err := Infos(context.Background(), backends, "foo")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2Infos(t *testing.T) {
	mURL := func(path string) *url.URL { return new(url.URL) }
	var mCall func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error)

	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		j := i
		mCall = func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
			infos := carbonapi_v2.Infos{
				Hosts: []string{fmt.Sprintf("host_%d", j)},
				Infos: []*carbonapi_v2.Info{
					&carbonapi_v2.Info{
						Name: fmt.Sprintf("foo/%d", j),
					},
				},
			}

			return infos.Marshal()
		}
		b := mock.New(mCall, mURL)
		backends = append(backends, b)
	}

	got, err := Infos(context.Background(), backends, "foo")
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != N {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}
}

func TestCarbonapiv2FindsError(t *testing.T) {
	mURL := func(path string) *url.URL { return new(url.URL) }
	mCall := func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
		return nil, errors.New("No")
	}

	backends := []Backend{mock.New(mCall, mURL)}

	_, err := Finds(context.Background(), backends, "foo")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCarbonapiv2Finds(t *testing.T) {
	mURL := func(path string) *url.URL { return new(url.URL) }
	var mCall func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error)

	N := 10
	backends := make([]Backend, 0)
	for i := 0; i < 10; i++ {
		j := i
		mCall = func(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
			matches := carbonapi_v2.Matches{
				Matches: []carbonapi_v2.Match{
					carbonapi_v2.Match{
						Path:   fmt.Sprintf("foo/%d", j),
						IsLeaf: true,
					},
				},
			}

			return matches.Marshal()
		}
		b := mock.New(mCall, mURL)
		backends = append(backends, b)
	}

	got, err := Finds(context.Background(), backends, "foo")
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != N {
		t.Errorf("Expected %d responses, got %d", N, len(got))
		return
	}
}

func TestCarbonapiv2FindDecoder(t *testing.T) {
	input := carbonapi_v2.Matches{
		Matches: []carbonapi_v2.Match{
			carbonapi_v2.Match{
				Path:   "foo/bar",
				IsLeaf: true,
			},
		},
	}

	blob, err := input.Marshal()
	if err != nil {
		t.Error(err)
		return
	}

	got, err := carbonapiV2FindDecoder(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != 1 {
		t.Errorf("Expected 1 response, got %d", len(got))
		return
	}

	if got[0].Path != "foo/bar" || !got[0].IsLeaf {
		t.Error("Invalid match")
	}
}

func TestCarbonapiv2InfoDecoder(t *testing.T) {
	input := carbonapi_v2.Infos{
		Hosts: []string{"foo"},
		Infos: []*carbonapi_v2.Info{
			&carbonapi_v2.Info{
				Name: "A",
				Retentions: []carbonapi_v2.Retention{
					carbonapi_v2.Retention{
						SecondsPerPoint: 1,
						NumberOfPoints:  10,
					},
				},
			},
		},
	}

	blob, err := input.Marshal()
	if err != nil {
		t.Error(err)
		return
	}

	got, err := carbonapiV2InfoDecoder(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != 1 {
		t.Errorf("Expected 1 response, got %d", len(got))
		return
	}

	if got[0].Host != "foo" || got[0].Name != "A" {
		t.Error("Invalid info")
	}

	if len(got[0].Retentions) != 1 {
		t.Error("Invalid retention")
	}
}

func TestCarbonapiv2RenderDecoder(t *testing.T) {
	input := carbonapi_v2.Metrics{
		Metrics: []carbonapi_v2.Metric{
			carbonapi_v2.Metric{
				Name:      "A",
				StartTime: 1,
				StopTime:  2,
				StepTime:  3,
				Values:    []float64{0, 1},
				IsAbsent:  []bool{true, false},
			},
		},
	}

	blob, err := input.Marshal()
	if err != nil {
		t.Error(err)
		return
	}

	got, err := carbonapiV2RenderDecoder(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
		return
	}

	exp := types.Metric{
		Name:      "A",
		StartTime: 1,
		StopTime:  2,
		StepTime:  3,
		Values:    []float64{0, 1},
		IsAbsent:  []bool{true, false},
	}

	if !types.MetricsEqual(exp, got[0]) {
		t.Error("Metrics not equal")
	}
}

func TestCarbonapiv2RenderEncoder(t *testing.T) {
	u := &url.URL{}

	var from int32 = 100
	var until int32 = 200
	metrics := []string{"foo", "bar"}

	gotURL, gotReader := carbonapiV2RenderEncoder(u, from, until, metrics)
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["from"]; len(got) != 1 || got[0] != "100" {
		t.Errorf("Expected from=100, got %v", got)
	}

	if got := vals["until"]; len(got) != 1 || got[0] != "200" {
		t.Errorf("Expected until=200, got %v", got)
	}

	got := vals["target"]
	if len(got) != 2 || got[0] != "foo" || got[1] != "bar" {
		t.Errorf("Bad target: got %v, expected %v", got, metrics)
	}
}

func TestCarbonapiv2InfoEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL, gotReader := carbonapiV2InfoEncoder(u, "foo")
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["target"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}

func TestCarbonapiv2FindEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL, gotReader := carbonapiV2FindEncoder(u, "foo")
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["query"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}

func TestCheckErrs(t *testing.T) {
	ctx := context.Background()
	logger := zap.New(nil)

	if err := checkErrs(ctx, nil, 0, logger); err != nil {
		t.Error("Expected no error")
	}

	if err := checkErrs(ctx, []error{errors.New("no")}, 1, logger); err == nil {
		t.Error("Expected error")
	}

	if err := checkErrs(ctx, []error{errors.New("no")}, 0, logger); err == nil {
		t.Error("Expected error")
	}

	if err := checkErrs(ctx, []error{errors.New("no")}, 2, logger); err != nil {
		t.Error("Expected no error")
	}
}
