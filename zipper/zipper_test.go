package zipper

import (
	"fmt"
	"testing"

	pb3 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

func TestMergeResponses(t *testing.T) {
	z := &Zipper{
		logger: zap.New(nil),
	}
	stats := &Stats{}

	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0},
					IsAbsent: []bool{true},
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1},
				IsAbsent: []bool{false},
			},
		},
	}

	responses := make([]ServerResponse, len(input))
	for i, resp := range input {
		blob, err := resp.Marshal()
		if err != nil {
			t.Fatal(err)
		}

		responses[i] = ServerResponse{
			server:   fmt.Sprintf("server_%d", i),
			response: blob,
		}
	}

	_, got := z.mergeResponses(responses, stats)

	if !got.Equal(expected) {
		t.Fatalf("Response mismatch\nExp: %+v\nGot: %+v\n", expected, got)
	}
}
