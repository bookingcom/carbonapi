package helper

import (
	"math"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/google/go-cmp/cmp"
)

func TestNormalize(t *testing.T) {
	now32 := int32(time.Now().Unix())
	input := []struct {
		name  string
		in    []*types.MetricData
		want  []*types.MetricData
		start int32
		end   int32
		step  int32
	}{
		{
			"nulls",
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
			},
			now32,
			now32 + 4,
			1,
		},
	}

	for _, test := range input {
		got, start, end, step, err := Normalize(test.in)
		if err != nil {
			t.Errorf("error: %w", err)
		}
		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Errorf("Normalize() mismatch (-want +got):\n%s", diff)
		}
		if start != test.start {
			t.Errorf("Normalize() mismatch for start:\n%d -- %d", start, test.start)
		}
		if end != test.end {
			t.Errorf("Normalize() mismatch for end:\n%d -- %d", end, test.end)
		}
		if step != test.step {
			t.Errorf("Normalize() mismatch for step:\n%d -- %d", step, test.step)
		}

	}
}
