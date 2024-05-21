package sumSeriesWithWildcards

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/metadata"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	th "github.com/bookingcom/carbonapi/tests"
)

func init() {
	md := New("")
	evaluator := th.EvaluatorFromFunc(md[0].F)
	metadata.SetEvaluator(evaluator)
	helper.SetEvaluator(evaluator)
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
}

func TestSumSeriesWithWildcards(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"sumSeriesWithWildcards(empty,0)",
			map[parser.MetricRequest][]*types.MetricData{
				{"empty", 0, 1}: {
					types.MakeMetricData("metric0", []float64{}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeriesWithWildcards()",
				[]float64{}, 1, now32)},
		},
		{
			"sumSeriesWithWildcards(*,0)",
			map[parser.MetricRequest][]*types.MetricData{
				{"*", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m2", []float64{5, 4, 3, 2, 1}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeriesWithWildcards()",
				[]float64{6, 6, 6, 6, 6}, 1, now32)},
		},
		{
			"sumSeriesWithWildcards(m1.*.*,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"m1.*.*", 0, 1}: {
					types.MakeMetricData("m1.n1.o1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o2", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n1.o2", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o3", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o4", []float64{1, 2, 3, 4, 5}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("sumSeriesWithWildcards(m1.n1)", []float64{2, 4, 6, 8, 10}, 1, now32),
				types.MakeMetricData("sumSeriesWithWildcards(m1.n2)", []float64{4, 8, 12, 16, 20}, 1, now32),
			},
		},
		{
			"sumSeriesWithWildcards(m1.*.*,1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"m1.*.*", 0, 1}: {
					types.MakeMetricData("m1.n1.o1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o2", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n1.o2", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o3", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m1.n2.o4", []float64{1, 2, 3, 4, 5}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("sumSeriesWithWildcards(m1)", []float64{6, 12, 18, 24, 30}, 1, now32),
			},
		},
	}

	for _, tt := range tests {
		copy := tt
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &copy)
		})
	}
}
