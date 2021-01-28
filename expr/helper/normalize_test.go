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
		name string
		in   []*types.MetricData
		want []*types.MetricData
	}{
		{
			"none",
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
			},
		},
		{
			"simple",
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{1, 2, 3, 4, 5}, 1, now32),
				types.MakeMetricData("metricB", []float64{1, 2, 3, 4, 5}, 1, now32),
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{1, 2, 3, 4, 5}, 1, now32),
				types.MakeMetricData("metricB", []float64{1, 2, 3, 4, 5}, 1, now32),
			},
		},
		{
			"simple",
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{1, 2, 3, 4, 5}, 1, now32),
				types.MakeMetricData("metricB", []float64{1, 2, 3, 4, 5}, 1, now32),
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{1, 2, 3, 4, 5}, 1, now32),
				types.MakeMetricData("metricB", []float64{1, 2, 3, 4, 5}, 1, now32),
			},
		},
		{
			"different_steps",
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{1, 2, 3, 4, 5, 6, 7}, 3, now32),
				types.MakeMetricData("metricB", []float64{1, 2, 3}, 7, now32),
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{4}, 21, now32),
				types.MakeMetricData("metricB", []float64{2}, 21, now32),
			},
		},
	}

	for _, test := range input {

		got, _, _, _, err := Normalize(test.in)
		if err != nil {
			t.Errorf("error: %w", err)
		}
		if len(got) != len(test.want) {
			t.Errorf("Normalize() mismatch for number of metrics. Want: %d. Got: %d", len(test.want), len(got))
		}
		for idx := range test.want {
			if diff := cmp.Diff(test.want[idx].Values, got[idx].Values); diff != "" {
				t.Errorf("Normalize() mismatch for %s Values (-want +got):\n%s", test.name, diff)
			}
			if diff := cmp.Diff(test.want[idx].IsAbsent, got[idx].IsAbsent); diff != "" {
				t.Errorf("Normalize() mismatch for %s IsAbsent (-want +got):\n%s ", test.name, diff)
			}

		}

	}
}
