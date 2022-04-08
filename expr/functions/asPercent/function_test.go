package asPercent

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
		metadata.RegisterFunction(m.Name, m.F, nil)
	}
}

func TestAsPercent(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"asPercent(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("asPercent(metric1,metric2)",
				[]float64{50, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 200}, 1, now32)},
		},
		{
			"asPercent(metricA*,metricB*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metricA*", 0, 1}: {
					types.MakeMetricData("metricA1", []float64{1, 20, 10}, 1, now32),
					types.MakeMetricData("metricA2", []float64{1, 10, 20}, 1, now32),
				},
				{"metricB*", 0, 1}: {
					types.MakeMetricData("metricB1", []float64{4, 4, 8}, 1, now32),
					types.MakeMetricData("metricB2", []float64{4, 16, 2}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("asPercent(metricA1,metricB1)",
				[]float64{25, 500, 125}, 1, now32),
				types.MakeMetricData("asPercent(metricA2,metricB2)",
					[]float64{25, 62.5, 1000}, 1, now32)},
		},
		{
			"asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total)",
			map[parser.MetricRequest][]*types.MetricData{
				{"Server{1,2}.memory.used", 0, 1}: {
					types.MakeMetricData("Server1.memory.used", []float64{1, 20, 10}, 1, now32),
					types.MakeMetricData("Server2.memory.used", []float64{1, 10, 20}, 1, now32),
				},
				{"Server{1,3}.memory.total", 0, 1}: {
					types.MakeMetricData("Server1.memory.total", []float64{4, 4, 8}, 1, now32),
					types.MakeMetricData("Server3.memory.total", []float64{4, 16, 2}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("asPercent(Server1.memory.used,Server1.memory.total)", []float64{25, 500, 125}, 1, now32),
				types.MakeMetricData("asPercent(Server2.memory.used,Server3.memory.total)", []float64{25, 62.5, 1000}, 1, now32),
			},
		},
		{
			"asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)",
			map[parser.MetricRequest][]*types.MetricData{
				{"Server{1,2}.memory.used", 0, 1}: {
					types.MakeMetricData("Server1.memory.used", []float64{1, 20, 10}, 1, now32),
					types.MakeMetricData("Server2.memory.used", []float64{1, 10, 20}, 1, now32),
				},
				{"Server{1,3}.memory.total", 0, 1}: {
					types.MakeMetricData("Server1.memory.total", []float64{4, 4, 8}, 1, now32),
					types.MakeMetricData("Server3.memory.total", []float64{4, 16, 2}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("asPercent(Server1.memory.used,Server1.memory.total)", []float64{25, 500, 125}, 1, now32),
				types.MakeMetricData("asPercent(Server2.memory.used,MISSING)", []float64{math.NaN(), math.NaN(), math.NaN()}, 1, now32),
				types.MakeMetricData("asPercent(MISSING,Server3.memory.total)", []float64{math.NaN(), math.NaN(), math.NaN()}, 1, now32),
			},
		},
		{
			"asPercent(not_found,Server{1,3}.memory.total)",
			map[parser.MetricRequest][]*types.MetricData{
				{"not_found", 0, 1}: {},
				{"Server{1,3}.memory.total", 0, 1}: {
					types.MakeMetricData("Server1.memory.total", []float64{4, 4, 8}, 1, now32),
					types.MakeMetricData("Server3.memory.total", []float64{4, 16, 2}, 1, now32),
				},
			},
			[]*types.MetricData{},
		},
		{
			"asPercent(test-db*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"test-db*", 0, 1}: {
					types.MakeMetricData("test-db1", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1, now32),
					types.MakeMetricData("test-db2", []float64{math.NaN(), 2, math.NaN(), 4, math.NaN(), 6, math.NaN(), 8, math.NaN(), 10, math.NaN(), 12, math.NaN(), 14, math.NaN(), 16, math.NaN(), 18, math.NaN(), 20}, 1, now32),
					types.MakeMetricData("test-db3", []float64{1, 2, math.NaN(), math.NaN(), math.NaN(), 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, math.NaN(), math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("test-db4", []float64{1, 2, 3, 4, math.NaN(), 6, math.NaN(), math.NaN(), 9, 10, 11, math.NaN(), 13, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 18, 19, 20}, 1, now32),
					types.MakeMetricData("test-db5", []float64{1, 2, math.NaN(), math.NaN(), math.NaN(), 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, math.NaN(), math.NaN()}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("asPercent(test-db1,sumSeries(test-db*))", []float64{25.0, 20.0, 50.0, 33.33333333333, 100.0, 20.0, 33.33333333333, 25.0, 25.0, 20.0, 25.0, 25.0, 25.0, 25.0, 33.33333333333, 25.0, 33.33333333333, 25.0, 50.0, 33.33333333333}, 1, now32),
				types.MakeMetricData("asPercent(test-db2,sumSeries(test-db*))", []float64{math.NaN(), 20.0, math.NaN(), 33.3333333333, math.NaN(), 20.0, math.NaN(), 25.0, math.NaN(), 20.0, math.NaN(), 25.0, math.NaN(), 25.0, math.NaN(), 25.0, math.NaN(), 25.0, math.NaN(), 33.33333333333}, 1, now32),
				types.MakeMetricData("asPercent(test-db3,sumSeries(test-db*))", []float64{25.0, 20.0, math.NaN(), math.NaN(), math.NaN(), 20.0, 33.33333333333, 25.0, 25.0, 20.0, 25.0, 25.0, 25.0, 25.0, 33.3333333333, 25.0, 33.3333333333, math.NaN(), math.NaN(), math.NaN()}, 1, now32),
				types.MakeMetricData("asPercent(test-db4,sumSeries(test-db*))", []float64{25.0, 20.0, 50.0, 33.33333333333, math.NaN(), 20.0, math.NaN(), math.NaN(), 25.0, 20.0, 25.0, math.NaN(), 25.0, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 25.0, 50.0, 33.33333333333}, 1, now32),
				types.MakeMetricData("asPercent(test-db5,sumSeries(test-db*))", []float64{25.0, 20.0, math.NaN(), math.NaN(), math.NaN(), 20.0, 33.33333333333, 25.0, 25.0, 20.0, 25.0, 25.0, 25.0, 25.0, 33.33333333333, 25.0, 33.33333333333, 25.0, math.NaN(), math.NaN()}, 1, now32),
			}},
		{
			"asPercent(test-db*, 10)",
			map[parser.MetricRequest][]*types.MetricData{
				{"test-db*", 0, 1}: {
					types.MakeMetricData("test-db1", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1, now32),
					types.MakeMetricData("test-db2", []float64{math.NaN(), 2, math.NaN(), 4, math.NaN(), 6, math.NaN(), 8, math.NaN(), 10, math.NaN(), 12, math.NaN(), 14, math.NaN(), 16, math.NaN(), 18, math.NaN(), 20}, 1, now32),
					types.MakeMetricData("test-db3", []float64{1, 2, math.NaN(), math.NaN(), math.NaN(), 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, math.NaN(), math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("test-db4", []float64{1, 2, 3, 4, math.NaN(), 6, math.NaN(), math.NaN(), 9, 10, 11, math.NaN(), 13, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 18, 19, 20}, 1, now32),
					types.MakeMetricData("test-db5", []float64{1, 2, math.NaN(), math.NaN(), math.NaN(), 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, math.NaN(), math.NaN()}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("asPercent(test-db1,10.00)", []float64{10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 190.0, 200.0}, 1, now32),
				types.MakeMetricData("asPercent(test-db2,10.00)", []float64{math.NaN(), 20.0, math.NaN(), 40.0, math.NaN(), 60.0, math.NaN(), 80.0, math.NaN(), 100.0, math.NaN(), 120.0, math.NaN(), 140.0, math.NaN(), 160.0, math.NaN(), 180.0, math.NaN(), 200.0}, 1, now32),
				types.MakeMetricData("asPercent(test-db3,10.00)", []float64{10.0, 20.0, math.NaN(), math.NaN(), math.NaN(), 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, math.NaN(), math.NaN(), math.NaN()}, 1, now32),
				types.MakeMetricData("asPercent(test-db4,10.00)", []float64{10.0, 20.0, 30.0, 40.0, math.NaN(), 60.0, math.NaN(), math.NaN(), 90.0, 100.0, 110.0, math.NaN(), 130.0, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 180.0, 190.0, 200.0}, 1, now32),
				types.MakeMetricData("asPercent(test-db5,10.00)", []float64{10.0, 20.0, math.NaN(), math.NaN(), math.NaN(), 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, math.NaN(), math.NaN()}, 1, now32),
			}},
		{
			"asPercent(test-db*, single)",
			map[parser.MetricRequest][]*types.MetricData{
				{"test-db*", 0, 1}: {
					types.MakeMetricData("test-db1", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1, now32),
					types.MakeMetricData("test-db2", []float64{math.NaN(), 2, math.NaN(), 4, math.NaN(), 6, math.NaN(), 8, math.NaN(), 10, math.NaN(), 12, math.NaN(), 14, math.NaN(), 16, math.NaN(), 18, math.NaN(), 20}, 1, now32),
					types.MakeMetricData("test-db3", []float64{1, 2, math.NaN(), math.NaN(), math.NaN(), 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, math.NaN(), math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("test-db4", []float64{1, 2, 3, 4, math.NaN(), 6, math.NaN(), math.NaN(), 9, 10, 11, math.NaN(), 13, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 18, 19, 20}, 1, now32),
					types.MakeMetricData("test-db5", []float64{1, 2, math.NaN(), math.NaN(), math.NaN(), 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, math.NaN(), math.NaN()}, 1, now32),
				},
				{"single", 0, 1}: {
					types.MakeMetricData("test-db1", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("asPercent(test-db1,single)", []float64{100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0}, 1, now32),
				types.MakeMetricData("asPercent(test-db2,single)", []float64{math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0, math.NaN(), 100.0}, 1, now32),
				types.MakeMetricData("asPercent(test-db3,single)", []float64{100.0, 100.0, math.NaN(), math.NaN(), math.NaN(), 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, math.NaN(), math.NaN(), math.NaN()}, 1, now32),
				types.MakeMetricData("asPercent(test-db4,single)", []float64{100.0, 100.0, 100.0, 100.0, math.NaN(), 100.0, math.NaN(), math.NaN(), 100.0, 100.0, 100.0, math.NaN(), 100.0, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 100.0, 100.0, 100.0}, 1, now32),
				types.MakeMetricData("asPercent(test-db5,single)", []float64{100.0, 100.0, math.NaN(), math.NaN(), math.NaN(), 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, math.NaN(), math.NaN()}, 1, now32),
			}},
	}

	for _, tt := range tests {
		tt := tt
		testName := tt.Target
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &tt)
		})
	}

}
