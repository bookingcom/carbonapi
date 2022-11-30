package sum

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

func TestSum(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"sumSeries(empty)",
			map[parser.MetricRequest][]*types.MetricData{
				{"empty", 0, 1}: {
					types.MakeMetricData("metric0", []float64{}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(empty)",
				[]float64{}, 1, now32)},
		},
		{
			"sumSeries(m1,m2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"m1", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4, 5}, 1, now32),
				},
				{"m2", 0, 1}: {
					types.MakeMetricData("m2", []float64{5, 4, 3, 2, 1}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(m1,m2)",
				[]float64{6, 6, 6, 6, 6}, 1, now32)},
		},
		{
			"sumSeries(m1,m2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"m1", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4, 5}, 1, now32),
				},
				{"m2", 0, 1}: {
					types.MakeMetricData("m2", []float64{5, 4, 3, 2, 1}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(m1,m2)",
				[]float64{6, 6, 6, 6, 6}, 1, now32)},
		},
		{
			"sumSeries(different,resolution)",
			map[parser.MetricRequest][]*types.MetricData{
				{"different", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4}, 2, now32),
				},
				{"resolution", 0, 1}: {
					types.MakeMetricData("m2", []float64{1, 2, 3, 4, 5, 6, 7, 8}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(different,resolution)",
				[]float64{2.5, 5.5, 8.5, 11.5}, 2, now32)},
		},
		{
			"sumSeries(different,length)",
			map[parser.MetricRequest][]*types.MetricData{
				{"different", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4}, 1, now32),
				},
				{"length", 0, 1}: {
					types.MakeMetricData("m2", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(different,length)",
				[]float64{2, 4, 6, 8, 5, 6, 7, 8, 9, 10}, 2, now32)},
		},
		{
			"sumSeries(different,resolution_and_length)",
			map[parser.MetricRequest][]*types.MetricData{
				{"different", 0, 1}: {
					types.MakeMetricData("m1", []float64{1, 2, 3, 4}, 2, now32),
				},
				{"resolution_and_length", 0, 1}: {
					types.MakeMetricData("m2", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(different,resolution_and_length)",
				[]float64{2.5, 5.5, 8.5, 11.5, 9.5}, 2, now32)},
		},
	}

	for _, tt := range tests {
		copy := tt
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &copy)
		})
	}
}
