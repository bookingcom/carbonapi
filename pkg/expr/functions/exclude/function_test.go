package exclude

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

func TestExclude(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"exclude(metric1,\"(Foo|Baz)\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricFoo", []float64{1, 1, 1, 1, 1}, 1, now32),
					types.MakeMetricData("metricBar", []float64{2, 2, 2, 2, 2}, 1, now32),
					types.MakeMetricData("metricBaz", []float64{3, 3, 3, 3, 3}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("metricBar", // NOTE(dgryski): not sure if this matches graphite
				[]float64{2, 2, 2, 2, 2}, 1, now32)},
		},
	}

	for _, tt := range tests {
		tt := tt
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &tt)
		})
	}

}
