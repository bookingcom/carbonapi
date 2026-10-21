package groupByNode

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/expr/functions/averageSeries"
	"github.com/bookingcom/carbonapi/pkg/expr/functions/diffSeries"
	"github.com/bookingcom/carbonapi/pkg/expr/functions/minMax"
	"github.com/bookingcom/carbonapi/pkg/expr/functions/stddevSeries"
	"github.com/bookingcom/carbonapi/pkg/expr/functions/sum"
	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/metadata"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	th "github.com/bookingcom/carbonapi/tests"
)

func init() {
	s := sum.New("")
	for _, m := range s {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
	md := New("")
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
	mm := minMax.New("")
	for _, m := range mm {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
	as := averageSeries.New("")
	for _, m := range as {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
	stds := stddevSeries.New("")
	for _, m := range stds {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
	ds := diffSeries.New("")
	for _, m := range ds {
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
	evaluator := th.EvaluatorFromFuncWithMetadata(metadata.FunctionMD.Functions)
	metadata.SetEvaluator(evaluator)
	helper.SetEvaluator(evaluator)
}

type callbackEvaluator func(parser.Expr) ([]*types.MetricData, error)

func (f callbackEvaluator) EvalExpr(_ context.Context, e parser.Expr, _, _ int32, _ map[parser.MetricRequest][]*types.MetricData, _ interfaces.GetTargetData) ([]*types.MetricData, error) {
	return f(e)
}

func TestGroupByNodeEmptyCallbackResult(t *testing.T) {
	for _, target := range []string{
		`groupByNode(metric.*,1,"sum")`,
		`groupByNodes(metric.*,"sum",1)`,
	} {
		t.Run(target, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				result []*types.MetricData
				err    error
			}{
				{name: "nil"},
				{name: "empty", result: []*types.MetricData{}},
				{name: "cancelled", result: []*types.MetricData{}, err: context.Canceled},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, mixed := range []bool{false, true} {
						e, _, err := parser.ParseExpr(target)
						if err != nil {
							t.Fatal(err)
						}
						args := []*types.MetricData{types.MakeMetricData("metric.empty", []float64{1}, 1, 0)}
						var want []*types.MetricData
						if mixed {
							args = append(args, types.MakeMetricData("metric.present", []float64{2}, 1, 0))
							want = []*types.MetricData{types.MakeMetricData("present", []float64{2}, 1, 0)}
						}
						f := &groupByNode{}
						f.SetEvaluator(callbackEvaluator(func(e parser.Expr) ([]*types.MetricData, error) {
							if e.Args()[0].Target() == "empty" {
								return tc.result, tc.err
							}
							return []*types.MetricData{types.MakeMetricData("sumSeries(present)", []float64{2}, 1, 0)}, nil
						}))
						values := map[parser.MetricRequest][]*types.MetricData{{"metric.*", 0, 1}: args}
						got, err := f.Do(context.Background(), e, 0, 1, values, th.NoopGetTargetData)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("mixed=%v: got %#v, want %#v", mixed, got, want)
						}
					}
				})
			}
		})
	}
}

func TestGroupByNodeEmptyInput(t *testing.T) {
	for _, target := range []string{`groupByNode(metric.*,1,"sum")`, `groupByNodes(metric.*,"sum",1)`} {
		for _, input := range [][]*types.MetricData{nil, {}} {
			t.Run(target, func(t *testing.T) {
				e, _, err := parser.ParseExpr(target)
				if err != nil {
					t.Fatal(err)
				}
				values := map[parser.MetricRequest][]*types.MetricData{{"metric.*", 0, 1}: input}
				got, err := metadata.GetEvaluator().EvalExpr(context.Background(), e, 0, 1, values, th.NoopGetTargetData)
				if err != nil || len(got) != 0 {
					t.Fatalf("got %#v, %v; want no series and no error", got, err)
				}
			})
		}
	}
}

func TestGroupByNodeAllNull(t *testing.T) {
	for _, target := range []string{`groupByNode(metric.*,1,"sum")`, `groupByNodes(metric.*,"sum",1)`} {
		t.Run(target, func(t *testing.T) {
			th.TestMultiReturnEvalExpr(t, &th.MultiReturnEvalTestItem{
				Target: target,
				M: map[parser.MetricRequest][]*types.MetricData{
					{"metric.*", 0, 1}: {types.MakeMetricData("metric.empty", []float64{math.NaN()}, 1, 0)},
				},
				Name: "all-null group remains present",
				Results: map[string][]*types.MetricData{
					"empty": {types.MakeMetricData("empty", []float64{math.NaN()}, 1, 0)},
				},
			})
		})
	}
}

func TestGroupByNode(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.MultiReturnEvalTestItem{
		{
			"groupByNode(metric1.foo.*.*,3,\"avg\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 22, 3, 24, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"groupByNodeAvg",
			map[string][]*types.MetricData{
				"baz": {types.MakeMetricData("baz", []float64{6, 17, 8, 19, 10}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"diff\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 22, 3, 24, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"groupByNodeDiff",
			map[string][]*types.MetricData{
				"baz": {types.MakeMetricData("baz", []float64{-10, 10, -10, 10, -10}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"stddev\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 22, 3, 24, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 2, 13, 4, 5}, 1, now32),
				},
			},
			"groupByNodeStddev",
			map[string][]*types.MetricData{
				"baz": {types.MakeMetricData("baz", []float64{5, 10, 5, 10, 0}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"max\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 22, 3, 24, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"groupByNodeMax",
			map[string][]*types.MetricData{
				"baz": {types.MakeMetricData("baz", []float64{11, 22, 13, 24, 15}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"min\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 22, 3, 24, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"groupByNodeMin",
			map[string][]*types.MetricData{
				"baz": {types.MakeMetricData("baz", []float64{1, 12, 3, 14, 5}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"sum\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.qux", []float64{6, 7, 8, 9, 10}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.qux", []float64{7, 8, 9, 10, 11}, 1, now32),
				},
			},
			"groupByNode",
			map[string][]*types.MetricData{
				"baz": {types.MakeMetricData("baz", []float64{12, 14, 16, 18, 20}, 1, now32)},
				"qux": {types.MakeMetricData("qux", []float64{13, 15, 17, 19, 21}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"sum\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.01", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.10", []float64{6, 7, 8, 9, 10}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.01", []float64{11, 12, 13, 14, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.10", []float64{7, 8, 9, 10, 11}, 1, now32),
				},
			},
			"groupByNode_names_with_int",
			map[string][]*types.MetricData{
				"01": {types.MakeMetricData("01", []float64{12, 14, 16, 18, 20}, 1, now32)},
				"10": {types.MakeMetricData("10", []float64{13, 15, 17, 19, 21}, 1, now32)},
			},
		},
		{
			"groupByNode(metric1.foo.*.*,3,\"sum\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.127_0_0_1:2003", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.127_0_0_1:2004", []float64{6, 7, 8, 9, 10}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.127_0_0_1:2003", []float64{11, 12, 13, 14, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.127_0_0_1:2004", []float64{7, 8, 9, 10, 11}, 1, now32),
				},
			},
			"groupByNode_names_with_colons",
			map[string][]*types.MetricData{
				"127_0_0_1:2003": {types.MakeMetricData("127_0_0_1:2003", []float64{12, 14, 16, 18, 20}, 1, now32)},
				"127_0_0_1:2004": {types.MakeMetricData("127_0_0_1:2004", []float64{13, 15, 17, 19, 21}, 1, now32)},
			},
		},
		{
			"groupByNodes(metric1.foo.*.*,\"sum\",0,1,3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.qux", []float64{6, 7, 8, 9, 10}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.qux", []float64{7, 8, 9, 10, 11}, 1, now32),
				},
			},
			"groupByNodes",
			map[string][]*types.MetricData{
				"metric1.foo.baz": {types.MakeMetricData("metric1.foo.baz", []float64{12, 14, 16, 18, 20}, 1, now32)},
				"metric1.foo.qux": {types.MakeMetricData("metric1.foo.qux", []float64{13, 15, 17, 19, 21}, 1, now32)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestMultiReturnEvalExpr(t, &tt)
		})
	}

}
