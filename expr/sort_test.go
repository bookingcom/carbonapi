package expr

import (
	"reflect"
	"testing"

	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

func TestSplitByDotsIgnoringBraces(t *testing.T) {
	tests := []struct {
		str    string
		result []string
	}{
		{
			"a.*.[c-d].{b-*,c*}.[e].{j,k}.l.m.{*.Status,*}.JVM.Memory.Heap.*",
			[]string{"a", "*", "[c-d]", "{b-*,c*}", "[e]", "{j,k}", "l", "m", "{*.Status,*}", "JVM", "Memory", "Heap", "*"},
		},
	}
	for i, test := range tests {
		res := SplitByDotsIgnoringBraces(test.str)
		if !reflect.DeepEqual(res, test.result) {
			t.Errorf("[%d] Expected %q but have %q", i, test.result, res)
		}
	}
}

func TestSortMetrics(t *testing.T) {
	const (
		gold   = "a.gold.c.d"
		silver = "a.silver.c.d"
		bronze = "a.bronze.c.d"
		first  = "a.first.c.d"
		second = "a.second.c.d"
		third  = "a.third.c.d"
		fourth = "a.fourth.c.d"
	)
	tests := []struct {
		metrics []*types.MetricData
		mfetch  parser.MetricRequest
		sorted  []*types.MetricData
	}{
		{
			[]*types.MetricData{
				// Note that these lines lexically sorted
				types.MakeMetricData(bronze, []float64{}, 1, 0),
				types.MakeMetricData(first, []float64{}, 1, 0),
				types.MakeMetricData(fourth, []float64{}, 1, 0),
				types.MakeMetricData(gold, []float64{}, 1, 0),
				types.MakeMetricData(second, []float64{}, 1, 0),
				types.MakeMetricData(silver, []float64{}, 1, 0),
				types.MakeMetricData(third, []float64{}, 1, 0),
			},
			parser.MetricRequest{
				Metric: "a.{first,second,third,fourth}.c.d",
				From:   0,
				Until:  1,
			},
			[]*types.MetricData{
				// First part : These are in the brace appearance order
				types.MakeMetricData(first, []float64{}, 1, 0),
				types.MakeMetricData(second, []float64{}, 1, 0),
				types.MakeMetricData(third, []float64{}, 1, 0),
				types.MakeMetricData(fourth, []float64{}, 1, 0),
				//Second part: These are in the slice order as above and come after
				types.MakeMetricData(bronze, []float64{}, 1, 0),
				types.MakeMetricData(gold, []float64{}, 1, 0),
				types.MakeMetricData(silver, []float64{}, 1, 0),
			},
		},
		{
			[]*types.MetricData{
				// Note that source now it's in random order
				types.MakeMetricData(third, []float64{}, 1, 0),
				types.MakeMetricData(silver, []float64{}, 1, 0),
				types.MakeMetricData(first, []float64{}, 1, 0),
				types.MakeMetricData(gold, []float64{}, 1, 0),
				types.MakeMetricData(second, []float64{}, 1, 0),
				types.MakeMetricData(bronze, []float64{}, 1, 0),
				types.MakeMetricData(fourth, []float64{}, 1, 0),
			},
			parser.MetricRequest{
				Metric: "a.*.c.d",
				From:   0,
				Until:  1,
			},
			[]*types.MetricData{
				// And result is sorted by glob in 2nd field
				types.MakeMetricData(bronze, []float64{}, 1, 0),
				types.MakeMetricData(first, []float64{}, 1, 0),
				types.MakeMetricData(fourth, []float64{}, 1, 0),
				types.MakeMetricData(gold, []float64{}, 1, 0),
				types.MakeMetricData(second, []float64{}, 1, 0),
				types.MakeMetricData(silver, []float64{}, 1, 0),
				types.MakeMetricData(third, []float64{}, 1, 0),
			},
		},
		{
			[]*types.MetricData{
				// Just picking 3 random metric
				types.MakeMetricData(first, []float64{}, 1, 0),
				types.MakeMetricData(fourth, []float64{}, 1, 0),
				types.MakeMetricData(bronze, []float64{}, 1, 0),
			},
			parser.MetricRequest{
				Metric: "a.{*.first,*}.c.d",
				From:   0,
				Until:  1,
			},
			[]*types.MetricData{
				// Sorted by glob in 2nd field, also but should be no error
				// despite query have 4 dots and metric has 3
				types.MakeMetricData(bronze, []float64{}, 1, 0),
				types.MakeMetricData(first, []float64{}, 1, 0),
				types.MakeMetricData(fourth, []float64{}, 1, 0),
			},
		},
	}
	for i, test := range tests {
		if len(test.metrics) != len(test.sorted) {
			t.Skipf("Error in test %d : length mismatch %d vs. %d", i, len(test.metrics), len(test.sorted))
		}
		SortMetrics(test.metrics, test.mfetch)
		for i := range test.metrics {
			if test.metrics[i].Name != test.sorted[i].Name {
				t.Errorf("[%d] Expected %q but have %q", i, test.sorted[i].Name, test.metrics[i].Name)
			}
		}
	}
}
