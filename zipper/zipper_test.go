package zipper

import (
	"fmt"
	"testing"

	pb3 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

func TestMergeResponsesBasic(t *testing.T) {
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

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes1(t *testing.T) {
	// lower resolution metric first
	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
					StepTime: 2,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0, 1},
					IsAbsent: []bool{true, false},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1, 0},
					IsAbsent: []bool{false, true},
					StepTime: 1,
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
				StepTime: 1,
			},
		},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes2(t *testing.T) {
	// lower resolution metric first
	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
					StepTime: 2,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1, 0},
					IsAbsent: []bool{false, true},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0, 1},
					IsAbsent: []bool{true, false},
					StepTime: 1,
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
				StepTime: 1,
			},
		},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes3(t *testing.T) {
	// (0, 1) metric first
	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0, 1},
					IsAbsent: []bool{true, false},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
					StepTime: 2,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1, 0},
					IsAbsent: []bool{false, true},
					StepTime: 1,
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
				StepTime: 1,
			},
		},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes4(t *testing.T) {
	// (0, 1) metric first
	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0, 1},
					IsAbsent: []bool{true, false},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1, 0},
					IsAbsent: []bool{false, true},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
					StepTime: 2,
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
				StepTime: 1,
			},
		},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes5(t *testing.T) {
	// (1, 0) metric first
	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1, 0},
					IsAbsent: []bool{false, true},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
					StepTime: 2,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0, 1},
					IsAbsent: []bool{true, false},
					StepTime: 1,
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
				StepTime: 1,
			},
		},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes6(t *testing.T) {
	// (1, 0) metric first
	input := []pb3.MultiFetchResponse{
		pb3.MultiFetchResponse{
			Metrics: []pb3.FetchResponse{
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1, 0},
					IsAbsent: []bool{false, true},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{0, 1},
					IsAbsent: []bool{true, false},
					StepTime: 1,
				},
				pb3.FetchResponse{
					Name:     "metric",
					Values:   []float64{1},
					IsAbsent: []bool{false},
					StepTime: 2,
				},
			},
		},
	}

	expected := pb3.MultiFetchResponse{
		Metrics: []pb3.FetchResponse{
			pb3.FetchResponse{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
				StepTime: 1,
			},
		},
	}

	doTest(t, input, expected)
}

func doTest(t *testing.T, input []pb3.MultiFetchResponse, expected pb3.MultiFetchResponse) {
	z := &Zipper{
		logger: zap.New(nil),
	}
	stats := &Stats{}

	got, err := getTestResponse(z, stats, input)
	if err != nil {
		t.Error(err)
	}

	if !got.Equal(expected) {
		t.Errorf("Response mismatch\nExp: %+v\nGot: %+v\n", expected, *got)
	}
}

func getTestResponse(z *Zipper, stats *Stats, input []pb3.MultiFetchResponse) (*pb3.MultiFetchResponse, error) {
	responses := make([]ServerResponse, len(input))
	for i, resp := range input {
		blob, err := resp.Marshal()
		if err != nil {
			return nil, err
		}

		responses[i] = ServerResponse{
			server:   fmt.Sprintf("server_%d", i),
			response: blob,
		}
	}

	_, got := z.mergeResponses(responses, stats)

	return got, nil
}
