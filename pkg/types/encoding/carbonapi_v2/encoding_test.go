package carbonapi_v2

import (
	"testing"

	"github.com/go-graphite/carbonapi/pkg/types"
)

func TestResponseFindUnmarshal(t *testing.T) {
	input := Matches{
		Matches: []Match{
			Match{
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

	got, err := FindDecoder(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if len(got.Matches) != 1 {
		t.Errorf("Expected 1 response, got %d", len(got.Matches))
		return
	}

	if got.Matches[0].Path != "foo/bar" || !got.Matches[0].IsLeaf {
		t.Error("Invalid match")
	}
}

func TestResponseInfoUnmarshal(t *testing.T) {
	input := Infos{
		Hosts: []string{"foo"},
		Infos: []Info{
			Info{
				Name: "A",
				Retentions: []Retention{
					Retention{
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

	got, err := InfoDecoder(blob)
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

func TestResponseRenderUnmarshal(t *testing.T) {
	input := Metrics{
		Metrics: []Metric{
			Metric{
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

	got, err := RenderDecoder(blob)
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
