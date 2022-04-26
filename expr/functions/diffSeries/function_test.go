package diffSeries

import (
	"go.uber.org/zap"
	"testing"
	"time"

	"math"

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
		metadata.RegisterFunction(m.Name, m.F, zap.NewNop())
	}
}

func TestDiffSeries(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"diffSeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(metric1,metric2)",
				[]float64{-1, math.NaN(), math.NaN(), 3, 4, 6}, 1, now32)},
		},
		{
			"diffSeries(metric1,metric2,metric3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{5, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{3, math.NaN(), 3, math.NaN(), 0, 7}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{1, math.NaN(), 3, math.NaN(), 0, 4}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(metric1,metric2,metric3)",
				[]float64{1, math.NaN(), math.NaN(), 3, 4, 1}, 1, now32)},
		},
		{
			"diffSeries(metric1,metric2,metric3,metric4)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{5, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{3, math.NaN(), 3, math.NaN(), 0, 7}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{1, math.NaN(), 3, math.NaN(), 0, 4}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(metric1,metric2,metric3)",
				[]float64{1, math.NaN(), math.NaN(), 3, 4, 1}, 1, now32)},
		},
		{
			"diffSeries(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(metric*)",
				[]float64{-1, math.NaN(), math.NaN(), 3, 4, 6}, 1, now32)},
		},
		{
			"diffSeries(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, math.NaN(), 3, 4, math.NaN()}, 1, now32),
					types.MakeMetricData("metric2", []float64{5, math.NaN(), 6}, 2, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(metric*)",
				[]float64{-3.5, 3, -2}, 1, now32)},
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
