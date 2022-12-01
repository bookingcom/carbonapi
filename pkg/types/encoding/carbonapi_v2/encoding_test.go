package carbonapi_v2

import (
	"testing"

	"github.com/bookingcom/carbonapi/pkg/types"

	"github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func TestIsInfoResponse(t *testing.T) {
	var blob []byte
	var ok bool
	var err error

	info := carbonapi_v2_pb.InfoResponse{
		Name:              "foo",
		AggregationMethod: "bar",
		MaxRetention:      10,
		XFilesFactor:      1.0,
	}
	blob, err = info.MarshalVT()
	if err != nil {
		t.Error(err)
		return
	}

	ok, err = IsInfoResponse(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if !ok {
		t.Error("Expected single response")
		return
	}

	sInfo := &carbonapi_v2_pb.ServerInfoResponse{
		Server: "localhost",
		Info:   &info,
	}

	blob, err = sInfo.MarshalVT()
	if err != nil {
		t.Error(err)
		return
	}

	ok, err = IsInfoResponse(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if ok {
		t.Error("Expected multiple responses")
		return
	}

	zInfo := carbonapi_v2_pb.ZipperInfoResponse{
		Responses: []*carbonapi_v2_pb.ServerInfoResponse{
			sInfo,
		},
	}
	blob, err = zInfo.MarshalVT()
	if err != nil {
		t.Error(err)
		return
	}

	ok, err = IsInfoResponse(blob)
	if err != nil {
		t.Error(err)
		return
	}

	if ok {
		t.Error("Expected multiple responses")
		return
	}
}

func TestResponseFindUnmarshal(t *testing.T) {
	input := carbonapi_v2_pb.GlobResponse{
		Matches: []*carbonapi_v2_pb.GlobMatch{
			{
				Path:   "foo/bar",
				IsLeaf: true,
			},
		},
	}

	blob, err := input.MarshalVT()
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
	input := carbonapi_v2_pb.ZipperInfoResponse{
		Responses: []*carbonapi_v2_pb.ServerInfoResponse{
			{
				Server: "foo",
				Info: &carbonapi_v2_pb.InfoResponse{
					Name: "A",
					Retentions: []*carbonapi_v2_pb.Retention{
						{
							SecondsPerPoint: 1,
							NumberOfPoints:  10,
						},
					},
				},
			},
		},
	}

	blob, err := input.MarshalVT()
	if err != nil {
		t.Error(err)
		return
	}

	got, err := MultiInfoDecoder(blob)
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
	input := carbonapi_v2_pb.MultiFetchResponse{
		Metrics: []*carbonapi_v2_pb.FetchResponse{
			{
				Name:      "A",
				StartTime: 1,
				StopTime:  2,
				StepTime:  3,
				Values:    []float64{0, 1},
				IsAbsent:  []bool{true, false},
			},
		},
	}

	blob, err := input.MarshalVT()
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
