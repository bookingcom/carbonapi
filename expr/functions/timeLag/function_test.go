package timeLag

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

func TestTimeLagSeriesMultiReturn(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.MultiReturnEvalTestItem{
		{
			"timeLagSeries(metric[12],metric2)",
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
			"timeLagSeries",
			map[string][]*types.MetricData{
				"timeLagSeries(metric1,metric2)": {types.MakeMetricData("timeLagSeries(metric1,metric2)", []float64{math.NaN(), 1, 2, 2, 3}, 1, now32)},
				"timeLagSeries(metric2,metric2)": {types.MakeMetricData("timeLagSeries(metric2,metric2)", []float64{0, 0, 0, 0, 0}, 1, now32)},
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

func TestTimeLagSeries(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"timeLagSeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 5, 12}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("timeLagSeries(metric1,metric2)",
				[]float64{math.NaN(), math.NaN(), math.NaN(), 1, 2, 0}, 1, now32)},
		},
		{
			"timeLagSeries(metric[12])",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric[12]", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 5, 12}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("timeLagSeries(metric[12])",
				[]float64{math.NaN(), math.NaN(), math.NaN(), 1, 2, 0}, 1, now32)},
		},
		{
			"timeLagSeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 1, 1, 1, 1, 1}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, 2, 2, 2, 2, 2}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("timeLagSeries(metric1,metric2)",
				[]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32)},
		},
		{
			"timeLagSeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 2, 3}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, 3, 4, 5, 6, 7}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("timeLagSeries(metric1,metric2)",
				[]float64{math.NaN(), 1, 1, 1, 4, 4}, 1, now32)},
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

func TestTimeLagSeriesLists(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.MultiReturnEvalTestItem{
		{
			"timeLagSeriesLists(consumer.*,producer.*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"consumer.*", 0, 1}: {
					types.MakeMetricData("consumer.1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("consumer.2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
				{"consumer.1", 0, 1}: {
					types.MakeMetricData("consumer.1", []float64{1, 2, 3, 4, 5}, 1, now32),
				},
				{"consumer.2", 0, 1}: {
					types.MakeMetricData("consumer.2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
				{"producer.*", 0, 1}: {
					types.MakeMetricData("producer.1", []float64{1, 2, 4, 4, 6}, 1, now32),
					types.MakeMetricData("producer.2", []float64{2, 4, 7, 8, 11}, 1, now32),
				},
				{"producer.1", 0, 1}: {
					types.MakeMetricData("producer.1", []float64{1, 2, 4, 4, 6}, 1, now32),
				},
				{"producer.2", 0, 1}: {
					types.MakeMetricData("producer.2", []float64{2, 4, 7, 8, 11}, 1, now32),
				},
			},
			"timeLagSeriesLists",
			map[string][]*types.MetricData{
				"timeLagSeries(consumer.1,producer.1)": {types.MakeMetricData("timeLagSeries(consumer.1,producer.1)", []float64{0, 0, 1, 0, 1}, 1, now32)},
				"timeLagSeries(consumer.2,producer.2)": {types.MakeMetricData("timeLagSeries(consumer.2,producer.2)", []float64{0, 0, 1, 0, 1}, 1, now32)},
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
