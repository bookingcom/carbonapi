package backend

import (
	"net/url"
	"testing"

	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/protobuf/carbonapi_v2"
)

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
