package multiplySeriesWithWildcards

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

func TestMultiplySeriesWithWildcards(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"multiplySeriesWithWildcards(empty,0)",
			map[parser.MetricRequest][]*types.MetricData{
				{"empty", 0, 1}: {
					types.MakeMetricData("metric0", []float64{}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("multiplySeriesWithWildcards()",
				[]float64{}, 1, now32)},
		},
		{
			"multiplySeriesWithWildcards(*,0)",
			map[parser.MetricRequest][]*types.MetricData{
				{"*", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("m2", []float64{5, 4, 3, 2, 1}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("multiplySeriesWithWildcards()",
				[]float64{5, 8, 9, 8, 5}, 1, now32)},
		},
		{
			"multiplySeriesWithWildcards(m1.*.*,2)",
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
				types.MakeMetricData("multiplySeriesWithWildcards(m1.n1)", []float64{1, 4, 9, 16, 25}, 1, now32),
				types.MakeMetricData("multiplySeriesWithWildcards(m1.n2)", []float64{1, 16, 81, 256, 625}, 1, now32),
			},
		},
		{
			"multiplySeriesWithWildcards(m1.*.*,1,2)",
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
				types.MakeMetricData("multiplySeriesWithWildcards(m1)", []float64{1, 64, 729, 4096, 15625}, 1, now32),
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
