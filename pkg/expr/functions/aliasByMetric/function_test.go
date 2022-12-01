package aliasByMetric

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

func TestAliasByMetric(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"aliasByMetric(metric1.foo.bar.baz)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.bar.baz", 0, 1}: {types.MakeMetricData("metric1.foo.bar.baz", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("baz", []float64{1, 2, 3, 4, 5}, 1, now32)},
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
