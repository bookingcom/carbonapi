package helper

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/expr/types"
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
			t.Error(fmt.Errorf("error: %w", err))
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

func TestNormalizeBorrowedMatchesDetached(t *testing.T) {
	for _, steps := range [][]int32{{1, 1}, {2, 3}, {6, 3}, {3, 6}} {
		t.Run(fmt.Sprint(steps), func(t *testing.T) {
			args := []*types.MetricData{
				types.MakeMetricData("a", []float64{1, math.NaN(), 3, 4, 5, 6, 7}, steps[0], 120),
				types.MakeMetricData("b", []float64{2, 4, math.NaN(), 8}, steps[1], 126),
			}
			for _, a := range args {
				a.ValuesPerPoint = 7
			}
			want, ws, we, wt, err := Normalize(args)
			if err != nil {
				t.Fatal(err)
			}
			got, gs, ge, gt, err := normalize(args, false)
			if err != nil || gs != ws || ge != we || gt != wt {
				t.Fatalf("range = %d/%d/%d, %v; want %d/%d/%d", gs, ge, gt, err, ws, we, wt)
			}
			for i := range got {
				if !reflect.DeepEqual(got[i].Metric, want[i].Metric) || got[i].ValuesPerPoint != want[i].ValuesPerPoint {
					t.Fatalf("series %d differs from detached normalization", i)
				}
				borrowed := &got[i].Values[0] == &args[i].Values[0]
				if borrowed != (steps[i] == gt) {
					t.Fatalf("series %d: borrowed = %v at step %d -> %d", i, borrowed, steps[i], gt)
				}
				want[i].Values[0] = -100
				want[i].IsAbsent[0] = true
				if args[i].Values[0] < 0 || args[i].IsAbsent[0] {
					t.Fatal("Normalize lost its detached-copy behavior")
				}
			}

			before := types.MarshalJSON(args)
			for _, first := range []bool{false, true} {
				for _, any := range []bool{false, true} {
					result, err := AggregateSeries("sum", args, first, any, func(v []float64) (float64, bool) {
						// Aggregate functions may reorder their scratch values.
						v[0]++
						return v[0], false
					})
					if err != nil {
						t.Fatal(err)
					}
					result[0].Values[0] = -200
				}
			}
			if string(types.MarshalJSON(args)) != string(before) {
				t.Fatal("aggregation or its output modified shared inputs")
			}
		})
	}
}
