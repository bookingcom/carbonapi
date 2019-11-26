package timeLag

import (
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
		metadata.RegisterFunction(m.Name, m.F)
	}
}

func TestTimeLagMultiReturn(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.MultiReturnEvalTestItem{
		{
			"timeLag(metric[12],metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric[12]", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
				{"metric1", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
				},
				{"metric2", 0, 1}: {
					types.MakeMetricData("metric2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
			},
			"timeLag",
			map[string][]*types.MetricData{
				"timeLag(metric1,metric2)": {types.MakeMetricData("timeLag(metric1,metric2)", []float64{math.NaN(), 1, 2, 2, 3}, 1, now32)},
				"timeLag(metric2,metric2)": {types.MakeMetricData("timeLag(metric2,metric2)", []float64{0, 0, 0, 0, 0}, 1, now32)},
			},
		},
	}

	for _, tt := range tests {
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestMultiReturnEvalExpr(t, &tt)
		})
	}

}

func TestTimeLag(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"timeLag(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 5, 12}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("timeLag(metric1,metric2)",
				[]float64{math.NaN(), math.NaN(), math.NaN(), 1, 2, 0}, 1, now32)},
		},
		{
			"timeLag(metric[12])",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric[12]", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 5, 12}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("timeLag(metric[12])",
				[]float64{math.NaN(), math.NaN(), math.NaN(), 1, 2, 0}, 1, now32)},
		},
	}

	for _, tt := range tests {
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &tt)
		})
	}

}
