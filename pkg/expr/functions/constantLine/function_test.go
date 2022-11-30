package constantLine

import (
	"context"
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

func TestConstantLine(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"constantLine(42.42)",
			map[parser.MetricRequest][]*types.MetricData{
				{"42.42", 0, 1}: {types.MakeMetricData("constantLine", []float64{12.3, 12.3}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("42.42",
				[]float64{42.42, 42.42, 42.42}, 1, now32)},
		},
	}

	for _, tt := range tests {
		testName := tt.Target
		evaluator := metadata.GetEvaluator()
		originalMetrics := th.DeepClone(tt.M)
		exp, _, err := parser.ParseExpr(tt.Target)
		if err != nil {
			t.Error(err)
		}
		ctx := context.Background()
		g, err := evaluator.EvalExpr(ctx, exp, 1, now32, tt.M, th.NoopGetTargetData)

		if err != nil {
			t.Errorf("failed to eval %s: %+v", testName, err)
			return
		}
		if len(g) != len(tt.Want) {
			t.Errorf("%s returned a different number of metrics, actual %v, Want %v", testName, len(g), len(tt.Want))
			return
		}
		th.DeepEqual(t, testName, originalMetrics, tt.M)

		for i, want := range tt.Want {
			actual := g[i]
			if actual == nil {
				t.Errorf("returned no value %v", tt.Target)
				return
			}
			if actual.StepTime == 0 {
				t.Errorf("missing Step for %+v", g)
			}
			if actual.Name != want.Name {
				t.Errorf("bad Name for %s metric %d: got %s, Want %s", testName, i, actual.Name, want.Name)
			}
			if !th.NearlyEqualMetrics(actual, want) {
				t.Errorf("different values for %s metric %s: got %v, Want %v", testName, actual.Name, actual.Values, want.Values)
				return
			}
		}
	}
}
