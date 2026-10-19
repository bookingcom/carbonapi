package expr

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

// The oracle evaluates the child normally, then supplies it as a shared named
// input to its parent. That invokes the original copying path in the very same
// functions, independently of the parent/child ownership optimization.
func TestPointOwnershipMatchesCopyingPipeline(t *testing.T) {
	operations := []string{"scale(%s,2.5)", "offset(%s,-1)", "removeBelowValue(%s,0)", "removeAboveValue(%s,2)", "keepLastValue(%s,2)", "keepLastValue(%s)", "transformNull(%s,-2)", "transformNull(%s,default=3)", "removeBelowPercentile(%s,50)"}
	ctx := context.Background()
	eval := func(target string, data map[parser.MetricRequest][]*types.MetricData) []*types.MetricData {
		e, rest, err := parser.ParseExpr(target)
		if err != nil || rest != "" {
			t.Fatalf("parse %s: %v %s", target, err, rest)
		}
		result, err := EvalExpr(ctx, e, 100, 107, data, noopGetTargetData)
		if err != nil {
			t.Fatalf("eval %s: %v", target, err)
		}
		return result
	}
	equal := func(a, b []*types.MetricData) {
		t.Helper()
		if len(a) != len(b) {
			t.Fatalf("series: %d != %d", len(a), len(b))
		}
		for i := range a {
			x, y := a[i], b[i]
			if x.Name != y.Name || x.StepTime != y.StepTime || x.StartTime != y.StartTime || x.StopTime != y.StopTime || len(x.Values) != len(y.Values) {
				t.Fatal("series metadata differs")
			}
			for j, v := range x.Values {
				if math.Float64bits(v) != math.Float64bits(y.Values[j]) || x.IsAbsent[j] != y.IsAbsent[j] {
					t.Fatalf("point %d: %v/%v != %v/%v", j, v, x.IsAbsent[j], y.Values[j], y.IsAbsent[j])
				}
			}
		}
	}
	fresh := func() *types.MetricData {
		return types.New("a", []float64{math.NaN(), math.Copysign(0, -1), -1, 2, math.Inf(1), 3, math.NaN()}, []bool{false, true, false, false, false, true, true}, 1, 100)
	}
	for _, outer := range operations {
		for _, inner := range operations {
			target := fmt.Sprintf(outer, fmt.Sprintf(inner, "a"))
			t.Run(target, func(t *testing.T) {
				input := fresh()
				data := map[parser.MetricRequest][]*types.MetricData{{Metric: "a", From: 100, Until: 107}: {input}}
				child := eval(fmt.Sprintf(inner, "a"), data)
				expected := eval(fmt.Sprintf(outer, "scratch"), map[parser.MetricRequest][]*types.MetricData{{Metric: "scratch", From: 100, Until: 107}: child})
				actual := eval(target, data)
				equal(actual, expected)
				equal([]*types.MetricData{input}, []*types.MetricData{fresh()})
				actual[0].Values[0] = 19
				actual[0].IsAbsent[0] = true
				equal([]*types.MetricData{input}, []*types.MetricData{fresh()})
			})
		}
	}
}
