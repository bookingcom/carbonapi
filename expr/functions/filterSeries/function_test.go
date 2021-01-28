package filterSeries

import (
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

func TestFilterSeriesMultiReturn(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.MultiReturnEvalTestItem{
		{
			"filterSeries(metric1.foo.*.baz,\"max\", \">\",20)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.baz", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{15, 22, 13, 24, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"filterSeries",
			map[string][]*types.MetricData{
				"metric1.foo.bar1.baz": {types.MakeMetricData("metric1.foo.bar1.baz", []float64{15, 22, 13, 24, 15}, 1, now32)},
			},
		},
		{
			"filterSeries(metric1.foo.*.baz,\"min\", \"<\",13)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.baz", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar3.baz", []float64{15, 22, 13, 24, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar4.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"filterSeries",
			map[string][]*types.MetricData{
				"metric1.foo.bar4.baz": {types.MakeMetricData("metric1.foo.bar4.baz", []float64{11, 12, 13, 14, 15}, 1, now32)},
			},
		},
		{
			"filterSeries(metric1.foo.*.baz,\"average\", \"<\",14)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.baz", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar5.baz", []float64{15, 22, 13, 24, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar6.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"filterSeries",
			map[string][]*types.MetricData{
				"metric1.foo.bar6.baz": {types.MakeMetricData("metric1.foo.bar6.baz", []float64{11, 12, 13, 14, 15}, 1, now32)},
			},
		},
		{
			"filterSeries(metric1.foo.*.baz,\"last\", \"=\",15)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.baz", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar5.baz", []float64{15, 22, 13, 24, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar6.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"filterSeries",
			map[string][]*types.MetricData{
				"metric1.foo.bar5.baz": {types.MakeMetricData("metric1.foo.bar5.baz", []float64{15, 22, 13, 24, 15}, 1, now32)},
				"metric1.foo.bar6.baz": {types.MakeMetricData("metric1.foo.bar6.baz", []float64{11, 12, 13, 14, 15}, 1, now32)},
			},
		},
		{
			"filterSeries(metric1.foo.*.baz,\"sum\", \"=\",89)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.baz", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar5.baz", []float64{15, 22, 13, 24, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar6.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
				},
			},
			"filterSeries",
			map[string][]*types.MetricData{
				"metric1.foo.bar5.baz": {types.MakeMetricData("metric1.foo.bar5.baz", []float64{15, 22, 13, 24, 15}, 1, now32)},
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
