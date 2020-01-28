package moving

import (
	"math"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/metadata"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	th "github.com/bookingcom/carbonapi/tests"
)

func init() {
	md := New("")
	evaluator := th.EvaluatorFromFunc(md[0].F)
	metadata.SetEvaluator(evaluator)
	helper.SetEvaluator(evaluator)
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F)
	}
}

func makeTestItem(expr string, now32 int32, metricName string, input []float64, result []float64) *th.EvalTestItem {
	return &th.EvalTestItem{
		expr,
		map[parser.MetricRequest][]*types.MetricData{
			{metricName, 0, 1}: {types.MakeMetricData(metricName, input, 1, now32)},
		},
		[]*types.MetricData{
			types.MakeMetricData(expr, result, 1, now32),
		},
	}
}

func TestMovingSum(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []*th.EvalTestItem{
		// movingSum window size 1 -> should be the same
		makeTestItem(
			"movingSum(metric1,1)", now32, "metric1",
			[]float64{
				1, 2, 3, 4, 5, 6, 7, 8, 9,
			},
			[]float64{
				1, 2, 3, 4, 5, 6, 7, 8, 9,
			},
		),
		// movingSum window size 2 -> sum of the previous 2 values,
		// the first is skipped as it's NaN + 1
		makeTestItem(
			"movingSum(metric1,2)", now32, "metric1",
			[]float64{
				1, 2, 3, 4, 5, 6, 7, 8, 9,
			},
			[]float64{
				math.NaN(), 3, 5, 7, 9, 11, 13, 15, 17,
			},
		),
		// movingSum window size 2 with missing data -> sum of the previous 2 values,
		// missing + 1 are skipped as it's NaN
		makeTestItem(
			"movingSum(metric1,2)", now32, "metric1",
			[]float64{
				math.NaN(), math.NaN(), 9, 8, 7, 6,
			},
			[]float64{
				math.NaN(), math.NaN(), math.NaN(), 17, 15, 13,
			},
		),
		// movingSum window size 2 with missing data in between,
		// missing in between treated as zeros
		makeTestItem(
			"movingSum(metric1,2)", now32, "metric1",
			[]float64{
				1, 2, math.NaN(), 3, math.NaN(), math.NaN(), 4,
			},
			[]float64{
				math.NaN(), 3, 2, 3, 3, 0, 4,
			},
		),
	}

	for _, tt := range tests {
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, tt)
		})
	}
}
