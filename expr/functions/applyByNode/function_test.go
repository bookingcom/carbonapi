package applyByNode

import (
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/expr/functions/divideSeries"
	"github.com/bookingcom/carbonapi/expr/functions/sum"
	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/metadata"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	th "github.com/bookingcom/carbonapi/tests"
)

func init() {
	md := New("")
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F)
	}
	md = divideSeries.New("")
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F)
	}
	md = sum.New("")
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F)
	}
	evaluator := th.EvaluatorFromFuncWithMetadata(metadata.FunctionMD.Functions)
	metadata.SetEvaluator(evaluator)
	helper.SetEvaluator(evaluator)

}

func TestApplyByNode(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"applyByNode(servers.s*.disk.bytes_free, 1, 'divideSeries(%.disk.bytes_used,sumSeries(%.disk.bytes_*)))', '%.disk.pct_used')",
			map[parser.MetricRequest][]*types.MetricData{
				{"servers.s*.disk.bytes_free", 0, 1}: {
					types.MakeMetricData("servers.s1.disk.bytes_free", []float64{90, 80, 70}, 1, now32),
					types.MakeMetricData("servers.s2.disk.bytes_free", []float64{99, 97, 98}, 1, now32),
				},
				{"servers.s1.disk.bytes_*", 0, 1}: {
					types.MakeMetricData("servers.s1.disk.bytes_free", []float64{90, 80, 70}, 1, now32),
					types.MakeMetricData("servers.s1.disk.bytes_used", []float64{10, 20, 30}, 1, now32),
				},
				{"servers.s2.disk.bytes_*", 0, 1}: {
					types.MakeMetricData("servers.s2.disk.bytes_free", []float64{99, 98, 97}, 1, now32),
					types.MakeMetricData("servers.s2.disk.bytes_used", []float64{1, 2, 3}, 1, now32),
				},
				{"servers.s1.disk.bytes_used", 0, 1}: {
					types.MakeMetricData("servers.s1.disk.bytes_used", []float64{10, 20, 30}, 1, now32),
				},
				{"servers.s2.disk.bytes_used", 0, 1}: {
					types.MakeMetricData("servers.s2.disk.bytes_used", []float64{1, 2, 3}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("servers.s1.disk.pct_used", []float64{0.1, 0.2, 0.3}, 1, now32),
				types.MakeMetricData("servers.s2.disk.pct_used", []float64{0.01, 0.02, 0.03}, 1, now32),
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
