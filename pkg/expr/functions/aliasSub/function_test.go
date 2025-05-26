package aliasSub

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

func TestAliasByNode(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"aliasSub(metric1.foo.bar.baz, \"foo\", \"replaced\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.bar.baz", 0, 1}: {types.MakeMetricData("metric1.foo.bar.baz", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric1.replaced.bar.baz",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"aliasSub(metric1.TCP100,\"^.*TCP(\\d+)\",\"$1\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.TCP100", 0, 1}: {types.MakeMetricData("metric1.TCP100", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("100",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"aliasSub(metric1.TCP100,\"^.*TCP(\\d+)\", \"\\1\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.TCP100", 0, 1}: {types.MakeMetricData("metric1.TCP100", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("100",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"aliasSub(metric1.foo.bar.baz, \"foo\", \"replaced\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.bar.baz", 0, 1}: {types.MakeMetricData("metric1.foo.bar.baz", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric1.replaced.bar.baz",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		// https://github.com/go-graphite/carbonapi/issues/290
		{
			//"aliasSub(*, '.dns.([^.]+).zone.', '\\1 diff to sql')",
			"aliasSub(*, 'dns.([^.]*).zone.', '\\1 diff to sql ')",
			map[parser.MetricRequest][]*types.MetricData{
				{"*", 0, 1}: {types.MakeMetricData("diffSeries(dns.snake.sql_updated, dns.snake.zone_updated)", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(dns.snake.sql_updated, snake diff to sql updated)",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
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
