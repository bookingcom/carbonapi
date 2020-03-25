package integralByInterval

import (
	"testing"

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

func TestFunction(t *testing.T) {
	tests := []th.EvalTestItem{
		{
			Target: "integralByInterval(10s,'10s')",
			M: map[parser.MetricRequest][]*types.MetricData{
				{Metric: "10s", From: 0, Until: 1}: {
					types.MakeMetricData(
						"10s", []float64{1, 0, 2, 3, 4, 5, 0, 7, 8, 9, 10}, 2, 0,
					),
				},
			},
			Want: []*types.MetricData{
				types.MakeMetricData(
					"integralByInterval(10s,'10s')", []float64{1, 1, 3, 6, 10, 5, 5, 12, 20, 29, 10}, 2, 0,
				),
			},
		},
	}

	for _, tt := range tests {
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &tt)
		})
	}

}
