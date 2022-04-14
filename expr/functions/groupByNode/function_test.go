package groupByNode

import (
	"go.uber.org/zap"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/expr/functions/averageSeries"
	"github.com/bookingcom/carbonapi/expr/functions/diffSeries"
	"github.com/bookingcom/carbonapi/expr/functions/minMax"
	"github.com/bookingcom/carbonapi/expr/functions/stddevSeries"
	"github.com/bookingcom/carbonapi/expr/functions/sum"
	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/metadata"
	"github.com/bookingcom/carbonapi/expr/types"
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
