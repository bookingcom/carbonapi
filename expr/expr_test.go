package expr

import (
	"context"
	"math"
	"testing"
	"time"
	"unicode"

	"fmt"

	"github.com/bookingcom/carbonapi/expr/functions"
	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
	th "github.com/bookingcom/carbonapi/tests"
)

func init() {
	functions.New(make(map[string]string))
}

func TestGetBuckets(t *testing.T) {
	tests := []struct {
		start       int32
		stop        int32
		bucketSize  int32
		wantBuckets int32
	}{
		{13, 18, 5, 1},
		{13, 17, 5, 1},
		{13, 19, 5, 2},
	}

	for _, test := range tests {
		buckets := helper.GetBuckets(test.start, test.stop, test.bucketSize)
		if buckets != test.wantBuckets {
			t.Errorf("TestGetBuckets failed!\n%v\ngot buckets %d",
				test,
				buckets,
			)
		}
	}
}

func TestAlignToBucketSize(t *testing.T) {
	tests := []struct {
		inputStart int32
		inputStop  int32
		bucketSize int32
		wantStart  int32
		wantStop   int32
	}{
		{
			13, 18, 5,
			10, 20,
		},
		{
			13, 17, 5,
			10, 20,
		},
		{
			13, 19, 5,
			10, 20,
		},
	}

	for _, test := range tests {
		start, stop := helper.AlignToBucketSize(test.inputStart, test.inputStop, test.bucketSize)
		if start != test.wantStart || stop != test.wantStop {
			t.Errorf("TestAlignToBucketSize failed!\n%v\ngot start %d stop %d",
				test,
				start,
				stop,
			)
		}
	}
}

func TestAlignToInterval(t *testing.T) {
	tests := []struct {
		inputStart int32
		inputStop  int32
		bucketSize int32
		wantStart  int32
	}{
		{
			91111, 92222, 5,
			91111,
		},
		{
			91111, 92222, 60,
			91080,
		},
		{
			91111, 92222, 3600,
			90000,
		},
		{
			91111, 92222, 86400,
			86400,
		},
	}

	for _, test := range tests {
		start := helper.AlignStartToInterval(test.inputStart, test.inputStop, test.bucketSize)
		if start != test.wantStart {
			t.Errorf("TestAlignToInterval failed!\n%v\ngot start %d",
				test,
				start,
			)
		}
	}
}

type evalExprTestCase struct {
	metric        string
	request       string
	metricRequest parser.MetricRequest
	values        []float64
	isAbsent      []bool
	stepTime      int32
	from          int32
	until         int32
}

func noopGetTargetData(ctx context.Context, exp parser.Expr, from, until int32, metricMap map[parser.MetricRequest][]*types.MetricData) (error, int) {
	return nil, 0
}

func TestEvalExpr(t *testing.T) {
	tests := map[string]evalExprTestCase{
		"EvalExp with summarize": {
			metric:  "metric1",
			request: "summarize(metric1,'1min')",
			metricRequest: parser.MetricRequest{
				Metric: "metric1",
				From:   1437127020,
				Until:  1437127140,
			},
			values:   []float64{343, 407, 385},
			isAbsent: []bool{false, false, false},
			stepTime: 60,
			from:     1437127020,
			until:    1437127140,
		},
		"metric name starts with digit": {
			metric:  "1metric",
			request: "1metric",
			metricRequest: parser.MetricRequest{
				Metric: "1metric",
				From:   1437127020,
				Until:  1437127140,
			},
			values:   []float64{343, 407, 385},
			isAbsent: []bool{false, false, false},
			stepTime: 60,
			from:     1437127020,
			until:    1437127140,
		},
		"metric unicode name starts with digit": {
			metric:  "1Метрика",
			request: "1Метрика",
			metricRequest: parser.MetricRequest{
				Metric: "1Метрика",
				From:   1437127020,
				Until:  1437127140,
			},
			values:   []float64{343, 407, 385},
			isAbsent: []bool{false, false, false},
			stepTime: 60,
			from:     1437127020,
			until:    1437127140,
		},
	}
	ctx := context.Background()
	parser.RangeTables = append(parser.RangeTables, unicode.Cyrillic)
	for name, test := range tests {
		t.Run(fmt.Sprintf("%s: %s", "TestEvalExpr", name), func(t *testing.T) {
			exp, e, err := parser.ParseExpr(test.request)
			if err != nil || e != "" {
				t.Errorf("error='%v', leftovers='%v'", err, e)
			}

			metricMap := make(map[parser.MetricRequest][]*types.MetricData)
			request := parser.MetricRequest{
				Metric: test.metric,
				From:   test.from,
				Until:  test.until,
			}

			data := types.MetricData{
				Metric: dataTypes.Metric{
					Name:      request.Metric,
					StartTime: request.From,
					StopTime:  request.Until,
					StepTime:  test.stepTime,
					Values:    test.values,
					IsAbsent:  test.isAbsent,
				},
			}

			metricMap[request] = []*types.MetricData{
				&data,
			}

			EvalExpr(ctx, exp, int32(request.From), int32(request.Until), metricMap, noopGetTargetData)
		})
	}
}

func TestEvalExpression(t *testing.T) {

	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			"metric",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric", 0, 1}: {types.MakeMetricData("metric", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric", []float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"metric*",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, 3, 4, 5, 6}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
				types.MakeMetricData("metric2", []float64{2, 3, 4, 5, 6}, 1, now32),
			},
		},
		{
			"sum(metric1,metric2,metric3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5, math.NaN()}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, 3, math.NaN(), 5, 6, math.NaN()}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{3, 4, 5, 6, math.NaN(), math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(metric1,metric2,metric3)", []float64{6, 9, 8, 15, 11, math.NaN()}, 1, now32)},
		},
		{
			"sum(metric1,metric2,metric3,metric4)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5, math.NaN()}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, 3, math.NaN(), 5, 6, math.NaN()}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{3, 4, 5, 6, math.NaN(), math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("sumSeries(metric1,metric2,metric3)", []float64{6, 9, 8, 15, 11, math.NaN()}, 1, now32)},
		},
		{
			"lowPass(metric1,40)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("lowPass(metric1,40)", []float64{0, 1, math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 8, 9}, 1, now32)},
		},
		{
			"percentileOfSeries(metric1,4)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("percentileOfSeries(metric1,4)", []float64{1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8, math.NaN()}, 1, now32)},
		},
		{
			"percentileOfSeries(metric1.foo.*.*,50)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 2, 3, 4, math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.qux", []float64{6, 7, 8, 9, 10, math.NaN()}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15, math.NaN()}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.qux", []float64{7, 8, 9, 10, 11, math.NaN()}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("percentileOfSeries(metric1.foo.*.*,50)", []float64{7, 8, 9, 10, 11, math.NaN()}, 1, now32)},
		},
		{
			"percentileOfSeries(metric1.foo.*.*,50,interpolate=true)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 2, 3, 4, math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.qux", []float64{6, 7, 8, 9, 10, math.NaN()}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15, math.NaN()}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.qux", []float64{7, 8, 9, 10, 11, math.NaN()}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("percentileOfSeries(metric1.foo.*.*,50,interpolate=true)", []float64{6.5, 7.5, 8.5, 9.5, 11, math.NaN()}, 1, now32)},
		},
		{
			"nPercentile(metric1,50)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{2, 4, 6, 10, 14, 20, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("nPercentile(metric1,50)", []float64{8, 8, 8, 8, 8, 8, 8}, 1, now32)},
		},
		{
			"nonNegativeDerivative(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{2, 4, 6, 10, 14, 20}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("nonNegativeDerivative(metric1)", []float64{math.NaN(), 2, 2, 4, 4, 6}, 1, now32)},
		},
		{
			"nonNegativeDerivative(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{2, 4, 6, 1, 4, math.NaN(), 8}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("nonNegativeDerivative(metric1)", []float64{math.NaN(), 2, 2, math.NaN(), 3, math.NaN(), math.NaN()}, 1, now32)},
		},
		{
			"nonNegativeDerivative(metric1,32)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{2, 4, 0, 10, 1, math.NaN(), 8, 40, 37}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("nonNegativeDerivative(metric1,32)", []float64{math.NaN(), 2, 29, 10, 24, math.NaN(), math.NaN(), 32, math.NaN()}, 1, now32)},
		},
		{
			"nonNegativeDerivative(metric1,minValue=1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{2, 4, 2, 10, 1, math.NaN(), 8, 40, 37}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("nonNegativeDerivative(metric1,minValue=1)", []float64{math.NaN(), 2, 1, 8, 0, math.NaN(), math.NaN(), 32, 36}, 1, now32)},
		},
		{
			"perSecond(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{27, 19, math.NaN(), 10, 1, 100, 1.5, 10.20}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("perSecond(metric1)", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 99, math.NaN(), 8.7}, 1, now32)},
		},
		{
			"perSecond(metric1,32)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{math.NaN(), 1, 2, 3, 4, 30, 0, 32, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("perSecond(metric1,32)", []float64{math.NaN(), math.NaN(), 1, 1, 1, 26, 3, 32, math.NaN()}, 1, now32)},
		},
		{
			"perSecond(metric1,minValue=1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{math.NaN(), 1, 2, 3, 4, 30, 3, 32, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("perSecond(metric1,minValue=1)", []float64{math.NaN(), math.NaN(), 1, 1, 1, 26, 2, 29, math.NaN()}, 1, now32)},
		},
		{
			"movingAverage(metric1,4)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingAverage(metric1,4)", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1, 1.25, 1.5, 1.75, 2.5, 3.5, 4, 5}, 1, now32)},
		},
		{
			"movingSum(metric1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingSum(metric1,2)", []float64{math.NaN(), math.NaN(), 3, 5, 7, 9}, 1, now32)},
		},
		{
			"movingMin(metric1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 2, 1, 0}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingMin(metric1,2)", []float64{math.NaN(), math.NaN(), 1, 2, 2, 1}, 1, now32)},
		},
		{
			"movingMax(metric1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 2, 1, 0}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingMax(metric1,2)", []float64{math.NaN(), math.NaN(), 2, 3, 3, 2}, 1, now32)},
		},
		{
			"movingMedian(metric1,4)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingMedian(metric1,4)", []float64{math.NaN(), math.NaN(), math.NaN(), 1, 1, 1.5, 2, 2, 3, 4, 5, 6}, 1, now32)},
		},
		{
			"movingMedian(metric1,5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8, 1, 2, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingMedian(metric1,5)", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1, 1, 2, 2, 2, 4, 4, 6, 6, 4, 2}, 1, now32)},
		},
		{
			"movingMedian(metric1,\"1s\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", -1, 1}: {types.MakeMetricData("metric1", []float64{1, 1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8, 1, 2, 0}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingMedian(metric1,\"1s\")", []float64{1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8, 1, 2, 0}, 1, now32)},
		},
		{
			"movingMedian(metric1,\"3s\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", -3, 1}: {types.MakeMetricData("metric1", []float64{0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 4, 6, 4, 6, 8, 1, 2}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("movingMedian(metric1,\"3s\")", []float64{0, 1, 1, 1, 1, 2, 2, 2, 4, 4, 6, 6, 6, 2}, 1, now32)},
		},
		{

			"pearson(metric1,metric2,6)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{43, 21, 25, 42, 57, 59}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{99, 65, 79, 75, 87, 81}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("pearson(metric1,metric2,6)", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 0.5298089018901744}, 1, now32)},
		},
		{
			"scale(metric1,2.5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, math.NaN(), 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("scale(metric1,2.5)", []float64{2.5, 5.0, math.NaN(), 10.0, 12.5}, 1, now32)},
		},
		{
			"scaleToSeconds(metric1,5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{60, 120, math.NaN(), 120, 120}, 60, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("scaleToSeconds(metric1,5)", []float64{5, 10, math.NaN(), 10, 10}, 1, now32)},
		},
		{
			"pow(metric1,3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{5, 1, math.NaN(), 0, 12, 125, 10.4, 1.1}, 60, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("pow(metric1,3)", []float64{125, 1, math.NaN(), 0, 1728, 1953125, 1124.864, 1.331}, 1, now32)},
		},
		{
			"keepLastValue(metric1,3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{math.NaN(), 2, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("keepLastValue(metric1,3)", []float64{math.NaN(), 2, 2, 2, 2, math.NaN(), 4, 5}, 1, now32)},
		},
		{
			"keepLastValue(metric1)",

			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{math.NaN(), 2, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("keepLastValue(metric1)", []float64{math.NaN(), 2, 2, 2, 2, 2, 4, 5}, 1, now32)},
		},
		{
			"keepLastValue(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4, 5}, 1, now32),
					types.MakeMetricData("metric2", []float64{math.NaN(), 2, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4, 5}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("keepLastValue(metric1)", []float64{1, 1, 1, 1, 1, 1, 4, 5}, 1, now32),
				types.MakeMetricData("keepLastValue(metric2)", []float64{math.NaN(), 2, 2, 2, 2, 2, 4, 5}, 1, now32),
			},
		},
		{
			"legendValue(metric1,\"avg\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric1 (avg: 3.000000)",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"legendValue(metric1,\"sum\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric1 (sum: 15.000000)",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"legendValue(metric1,\"total\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric1 (total: 15.000000)",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"legendValue(metric1,\"sum\",\"avg\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("metric1 (sum: 15.000000) (avg: 3.000000)",
				[]float64{1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"mapSeries(servers.*.cpu.*, 1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"servers.*.cpu.*", 0, 1}: {
					types.MakeMetricData("servers.server1.cpu.valid", []float64{1, 2, 3}, 1, now32),
					types.MakeMetricData("servers.server2.cpu.valid", []float64{6, 7, 8}, 1, now32),
					types.MakeMetricData("servers.server1.cpu.total", []float64{1, 2, 4}, 1, now32),
					types.MakeMetricData("servers.server2.cpu.total", []float64{5, 7, 8}, 1, now32),
					types.MakeMetricData("servers.server3.cpu.valid", []float64{8, 10, 11}, 1, now32),
					types.MakeMetricData("servers.server3.cpu.total", []float64{9, 10, 11}, 1, now32),
					types.MakeMetricData("servers.server4.cpu.valid", []float64{11, 13, 14}, 1, now32),
					types.MakeMetricData("servers.server4.cpu.total", []float64{12, 13, 14}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("servers.server1.cpu.valid", []float64{1, 2, 3}, 1, now32),
				types.MakeMetricData("servers.server1.cpu.total", []float64{1, 2, 4}, 1, now32),
				types.MakeMetricData("servers.server2.cpu.valid", []float64{6, 7, 8}, 1, now32),
				types.MakeMetricData("servers.server2.cpu.total", []float64{5, 7, 8}, 1, now32),
				types.MakeMetricData("servers.server3.cpu.valid", []float64{8, 10, 11}, 1, now32),
				types.MakeMetricData("servers.server3.cpu.total", []float64{9, 10, 11}, 1, now32),
				types.MakeMetricData("servers.server4.cpu.valid", []float64{11, 13, 14}, 1, now32),
				types.MakeMetricData("servers.server4.cpu.total", []float64{12, 13, 14}, 1, now32),
			},
		},
		{
			"maxSeries(metric1,metric2,metric3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), 2, 3, 4, 5}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 5, 6}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{3, math.NaN(), 4, 5, 6, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("maxSeries(metric1,metric2,metric3)",
				[]float64{3, math.NaN(), 4, 5, 6, 6}, 1, now32)},
		},
		{
			"minSeries(metric1,metric2,metric3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), 2, 3, 4, 5}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 5, 6}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{3, math.NaN(), 4, 5, 6, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("minSeries(metric1,metric2,metric3)",
				[]float64{1, math.NaN(), 2, 3, 4, 5}, 1, now32)},
		},
		{
			"divideSeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("divideSeries(metric1,metric2)",
				[]float64{0.5, math.NaN(), math.NaN(), math.NaN(), math.NaN(), 2}, 1, now32)},
		},
		{
			"multiplySeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("multiplySeries(metric1,metric2)",
				[]float64{2, math.NaN(), math.NaN(), math.NaN(), 0, 72}, 1, now32)},
		},
		{
			"diffSeriesLists(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("diffSeries(metric1,metric2)",
				[]float64{-1, math.NaN(), math.NaN(), math.NaN(), 4, 6}, 1, now32)},
		},

		{
			"multiplySeries(metric1,metric2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("multiplySeries(metric1,metric2)",
				[]float64{2, math.NaN(), math.NaN(), math.NaN(), 0, 72}, 1, now32)},
		},
		{
			"multiplySeries(metric1,metric2,metric3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, math.NaN(), 3, math.NaN(), 0, 6}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{3, math.NaN(), 4, math.NaN(), 7, 8}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("multiplySeries(metric1,metric2,metric3)",
				[]float64{6, math.NaN(), math.NaN(), math.NaN(), 0, 576}, 1, now32)},
		},
		{
			"rangeOfSeries(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{math.NaN(), math.NaN(), math.NaN(), 3, 4, 12, -10}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, math.NaN(), math.NaN(), 15, 0, 6, 10}, 1, now32),
					types.MakeMetricData("metric3", []float64{1, 2, math.NaN(), 4, 5, 6, 7}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("rangeOfSeries(metric*)",
				[]float64{1, math.NaN(), math.NaN(), 12, 5, 6, 20}, 1, now32)},
		},
		{
			"transformNull(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("transformNull(metric1)",
				[]float64{1, 0, 0, 3, 4, 12}, 1, now32)},
		},
		{
			"reduceSeries(mapSeries(devops.service.*.filter.received.*.count,2), \"asPercent\", 5,\"valid\",\"total\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"devops.service.*.filter.received.*.count", 0, 1}: {
					types.MakeMetricData("devops.service.server1.filter.received.valid.count", []float64{2, 4, 8}, 1, now32),
					types.MakeMetricData("devops.service.server1.filter.received.total.count", []float64{8, 2, 4}, 1, now32),
					types.MakeMetricData("devops.service.server2.filter.received.valid.count", []float64{3, 9, 12}, 1, now32),
					types.MakeMetricData("devops.service.server2.filter.received.total.count", []float64{12, 9, 3}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("devops.service.server1.filter.received.reduce.asPercent.count", []float64{25, 200, 200}, 1, now32),
				types.MakeMetricData("devops.service.server2.filter.received.reduce.asPercent.count", []float64{25, 100, 400}, 1, now32),
			},
		},
		{
			"reduceSeries(mapSeries(devops.service.*.filter.received.*.count,2), \"asPercent\", 5,\"valid\",\"total\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"devops.service.*.filter.received.*.count", 0, 1}: {
					types.MakeMetricData("devops.service.server1.filter.received.total.count", []float64{8, 2, 4}, 1, now32),
					types.MakeMetricData("devops.service.server2.filter.received.valid.count", []float64{3, 9, 12}, 1, now32),
					types.MakeMetricData("devops.service.server2.filter.received.total.count", []float64{12, 9, 3}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("devops.service.server2.filter.received.reduce.asPercent.count", []float64{25, 100, 400}, 1, now32),
			},
		},
		{
			"transformNull(metric1,default=5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("transformNull(metric1,5)",
				[]float64{1, 5, 5, 3, 4, 12}, 1, now32)},
		},
		{
			"highestMax(metric1,1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricA", []float64{1, 1, 3, 3, 12, 11}, 1, now32),
					types.MakeMetricData("metricB", []float64{1, 1, 3, 3, 4, 1}, 1, now32),
					types.MakeMetricData("metricC", []float64{1, 1, 3, 3, 4, 10}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("metricA", // NOTE(dgryski): not sure if this matches graphite
				[]float64{1, 1, 3, 3, 12, 11}, 1, now32)},
		},
		{
			"lowestCurrent(metric1,1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricA", []float64{1, 1, 3, 3, 4, 12}, 1, now32),
					types.MakeMetricData("metricB", []float64{1, 1, 3, 3, 4, 1}, 1, now32),
					types.MakeMetricData("metricC", []float64{1, 1, 3, 3, 4, 15}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("metricB", // NOTE(dgryski): not sure if this matches graphite
				[]float64{1, 1, 3, 3, 4, 1}, 1, now32)},
		},
		{
			"logarithm(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 10, 100, 1000, 10000}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("logarithm(metric1)",
				[]float64{0, 1, 2, 3, 4}, 1, now32)},
		},
		{
			"logarithm(metric1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 4, 8, 16, 32}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("logarithm(metric1,2)",
				[]float64{0, 1, 2, 3, 4, 5}, 1, now32)},
		},
		{
			"isNonNull(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{math.NaN(), -1, math.NaN(), -3, 4, 5}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("isNonNull(metric1)",
				[]float64{0, 1, 0, 1, 1, 1}, 1, now32)},
		},
		{
			"isNonNull(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricFoo", []float64{math.NaN(), -1, math.NaN(), -3, 4, 5}, 1, now32),
					types.MakeMetricData("metricBaz", []float64{1, -1, math.NaN(), -3, 4, 5}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("isNonNull(metricFoo)", []float64{0, 1, 0, 1, 1, 1}, 1, now32),
				types.MakeMetricData("isNonNull(metricBaz)", []float64{1, 1, 0, 1, 1, 1}, 1, now32),
			},
		},
		{
			"pearsonClosest(metric1,metric2,1,direction=\"abs\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricX", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
				},
				{"metric2", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{3, math.NaN(), 5, 6, 7, 8}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("metricB",
				[]float64{3, math.NaN(), 5, 6, 7, 8}, 1, now32)},
		},
		{
			"invert(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{-4, -2, -1, 0, 1, 2, 4}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("invert(metric1)",
				[]float64{-0.25, -0.5, -1, math.NaN(), 1, 0.5, 0.25}, 1, now32)},
		},
		{
			"offset(metric1,10)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{93, 94, 95, math.NaN(), 97, 98, 99, 100, 101}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("offset(metric1,10)",
				[]float64{103, 104, 105, math.NaN(), 107, 108, 109, 110, 111}, 1, now32)},
		},
		{
			"offsetToZero(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{93, 94, 95, math.NaN(), 97, 98, 99, 100, 101}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("offsetToZero(metric1)",
				[]float64{0, 1, 2, math.NaN(), 4, 5, 6, 7, 8}, 1, now32)},
		},
		{
			"integral(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 0, 2, 3, 4, 5, math.NaN(), 7, 8}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("integral(metric1)",
				[]float64{1, 1, 3, 6, 10, 15, math.NaN(), 22, 30}, 1, now32)},
		},
		{
			"sortByTotal(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{5, 5, 5, 5, 5, 5}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 4, 4}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metricB", []float64{5, 5, 5, 5, 5, 5}, 1, now32),
				types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 4, 4}, 1, now32),
				types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
			},
		},
		{
			"sortByMaxima(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{5, 5, 5, 5, 5, 5}, 1, now32),
					types.MakeMetricData("metricC", []float64{2, 2, 10, 5, 2, 2}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metricC", []float64{2, 2, 10, 5, 2, 2}, 1, now32),
				types.MakeMetricData("metricB", []float64{5, 5, 5, 5, 5, 5}, 1, now32),
				types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
			},
		},
		{
			"sortByMinima(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
				types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
			},
		},
		{
			"sortByName(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricX", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricA", []float64{0, 1, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{0, 0, 2, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricC", []float64{0, 0, 0, 3, 0, 0}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metricA", []float64{0, 1, 0, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metricB", []float64{0, 0, 2, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metricC", []float64{0, 0, 0, 3, 0, 0}, 1, now32),
				types.MakeMetricData("metricX", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
			},
		},
		{
			"sortByName(metric*,natural=true)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metric12", []float64{0, 1, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metric1234567890", []float64{0, 0, 0, 5, 0, 0}, 1, now32),
					types.MakeMetricData("metric2", []float64{0, 0, 2, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metric11", []float64{0, 0, 0, 3, 0, 0}, 1, now32),
					types.MakeMetricData("metric", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metric", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metric1", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metric2", []float64{0, 0, 2, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metric11", []float64{0, 0, 0, 3, 0, 0}, 1, now32),
				types.MakeMetricData("metric12", []float64{0, 1, 0, 0, 0, 0}, 1, now32),
				types.MakeMetricData("metric1234567890", []float64{0, 0, 0, 5, 0, 0}, 1, now32),
			},
		},
		{
			"squareRoot(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 0, 7, 8, 20, 30, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("squareRoot(metric1)",
				[]float64{1, 1.4142135623730951, 0, 2.6457513110645907, 2.8284271247461903, 4.47213595499958, 5.477225575051661, math.NaN()}, 1, now32)},
		},
		{
			"removeEmptySeries(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32),
					types.MakeMetricData("metric2", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("metric3", []float64{0, 0, 0, 0, 0, 0, 0, 0}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32),
				types.MakeMetricData("metric3", []float64{0, 0, 0, 0, 0, 0, 0, 0}, 1, now32),
			},
		},
		{
			"removeZeroSeries(metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32),
					types.MakeMetricData("metric2", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
					types.MakeMetricData("metric3", []float64{0, 0, 0, 0, 0, 0, 0, 0}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32),
			},
		},
		{
			"removeBelowValue(metric1, 0)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("removeBelowValue(metric1, 0)",
				[]float64{1, 2, math.NaN(), 7, 8, 20, 30, math.NaN()}, 1, now32)},
		},
		{
			"removeAboveValue(metric1, 10)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("removeAboveValue(metric1, 10)",
				[]float64{1, 2, -1, 7, 8, math.NaN(), math.NaN(), math.NaN()}, 1, now32)},
		},
		{
			"removeBelowPercentile(metric1, 50)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("removeBelowPercentile(metric1, 50)",
				[]float64{math.NaN(), math.NaN(), math.NaN(), 7, 8, 20, 30, math.NaN()}, 1, now32)},
		},
		{
			"removeAbovePercentile(metric1, 50)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, -1, 7, 8, 20, 30, math.NaN()}, 1, now32)},
			},
			[]*types.MetricData{types.MakeMetricData("removeAbovePercentile(metric1, 50)",
				[]float64{1, 2, -1, 7, math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32)},
		},
		{
			"linearRegression(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metric1",
						[]float64{1, 2, math.NaN(), math.NaN(), 5, 6}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("linearRegression(metric1)",
					[]float64{1, 2, 3, 4, 5, 6}, 1, now32),
			},
		},
		{
			"polyfit(metric1,3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metric1",
						[]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("polyfit(metric1,3)",
					[]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, now32),
			},
		},
		{
			"polyfit(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metric1",
						[]float64{7.79, 7.7, 7.92, 5.25, 6.24, 7.25, 7.15, 8.56, 7.82, 8.52}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("polyfit(metric1)",
					[]float64{6.94763636364, 7.05260606061, 7.15757575758, 7.26254545455, 7.36751515152,
						7.47248484848, 7.57745454545, 7.68242424242, 7.78739393939, 7.89236363636}, 1, now32),
			},
		},
		{
			"polyfit(metric1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metric1",
						[]float64{7.79, 7.7, 7.92, 5.25, 6.24, math.NaN(), 7.15, 8.56, 7.82, 8.52}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("polyfit(metric1,2)",
					[]float64{7.9733096590909085, 7.364842329545457, 6.933910511363642, 6.680514204545464, 6.604653409090922,
						6.706328125000017, 6.985538352272748, 7.442284090909116, 8.07656534090912, 8.888382102272761}, 1, now32),
			},
		},
		{
			"polyfit(metric1,3,'5sec')",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metric1",
						[]float64{7.79, 7.7, 7.92, 5.25, 6.24, 7.25, 7.15, 8.56, 7.82, 8.52}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("polyfit(metric1,3,'5sec')",
					[]float64{8.22944055944, 7.26958041958, 6.73364801865, 6.54653846154, 6.63314685315,
						6.91836829837, 7.3270979021, 7.78423076923, 8.21466200466, 8.54328671329,
						8.695, 8.5946969697, 8.16727272727, 7.33762237762, 6.03064102564}, 1, now32),
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

func TestEvalMultipleReturns(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.MultiReturnEvalTestItem{
		{
			"divideSeriesLists(metric[12],metric[12])",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric[12]", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
			},
			"divideSeriesListSameGroups",
			map[string][]*types.MetricData{
				"divideSeries(metric1,metric1)": {types.MakeMetricData("divideSeries(metric1,metric1)", []float64{1, 1, 1, 1, 1}, 1, now32)},
				"divideSeries(metric2,metric2)": {types.MakeMetricData("divideSeries(metric2,metric2)", []float64{1, 1, 1, 1, 1}, 1, now32)},
			},
		},
		{
			"multiplySeriesLists(metric[12],metric[12])",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric[12]", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
			},
			"multiplySeriesListSameGroups",
			map[string][]*types.MetricData{
				"multiplySeries(metric1,metric1)": {types.MakeMetricData("multiplySeries(metric1,metric1)", []float64{1, 4, 9, 16, 25}, 1, now32)},
				"multiplySeries(metric2,metric2)": {types.MakeMetricData("multiplySeries(metric2,metric2)", []float64{4, 16, 36, 64, 100}, 1, now32)},
			},
		},
		{
			"diffSeriesLists(metric[12],metric[12])",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric[12]", 0, 1}: {
					types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric2", []float64{2, 4, 6, 8, 10}, 1, now32),
				},
			},
			"diffSeriesListSameGroups",
			map[string][]*types.MetricData{
				"diffSeries(metric1,metric1)": {types.MakeMetricData("diffSeries(metric1,metric1)", []float64{0, 0, 0, 0, 0}, 1, now32)},
				"diffSeries(metric2,metric2)": {types.MakeMetricData("diffSeries(metric2,metric2)", []float64{0, 0, 0, 0, 0}, 1, now32)},
			},
		},
		{
			"sumSeriesWithWildcards(metric1.foo.*.*,1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.qux", []float64{6, 7, 8, 9, 10}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.qux", []float64{7, 8, 9, 10, 11}, 1, now32),
				},
			},
			"sumSeriesWithWildcards",
			map[string][]*types.MetricData{
				"sumSeriesWithWildcards(metric1.baz)": {types.MakeMetricData("sumSeriesWithWildcards(metric1.baz)", []float64{12, 14, 16, 18, 20}, 1, now32)},
				"sumSeriesWithWildcards(metric1.qux)": {types.MakeMetricData("sumSeriesWithWildcards(metric1.qux)", []float64{13, 15, 17, 19, 21}, 1, now32)},
			},
		},
		{
			"multiplySeriesWithWildcards(metric1.foo.*.*,1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1.foo.*.*", 0, 1}: {
					types.MakeMetricData("metric1.foo.bar1.baz", []float64{1, 2, 3, 4, 5}, 1, now32),
					types.MakeMetricData("metric1.foo.bar1.qux", []float64{6, 0, 8, 9, 10}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.baz", []float64{11, 12, 13, 14, 15}, 1, now32),
					types.MakeMetricData("metric1.foo.bar2.qux", []float64{7, 8, 9, 10, 11}, 1, now32),
					types.MakeMetricData("metric1.foo.bar3.baz", []float64{2, 2, 2, 2, 2}, 1, now32),
				},
			},
			"multiplySeriesWithWildcards",
			map[string][]*types.MetricData{
				"multiplySeriesWithWildcards(metric1.baz)": {types.MakeMetricData("multiplySeriesWithWildcards(metric1.baz)", []float64{22, 48, 78, 112, 150}, 1, now32)},
				"multiplySeriesWithWildcards(metric1.qux)": {types.MakeMetricData("multiplySeriesWithWildcards(metric1.qux)", []float64{42, 0, 72, 90, 110}, 1, now32)},
			},
		},
		{
			"stddevSeries(metric1,metric2,metric3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {types.MakeMetricData("metric1", []float64{1, 2, 3, 4, 5}, 1, now32)},
				{"metric2", 0, 1}: {types.MakeMetricData("metric2", []float64{2, 4, 6, 8, 10}, 1, now32)},
				{"metric3", 0, 1}: {types.MakeMetricData("metric3", []float64{1, 2, 3, 4, 5}, 1, now32)},
			},
			"stddevSeries",
			map[string][]*types.MetricData{
				"stddevSeries(metric1,metric2,metric3)": {types.MakeMetricData("stddevSeries(metric1,metric2,metric3)", []float64{0.4714045207910317, 0.9428090415820634, 1.4142135623730951, 1.8856180831641267, 2.357022603955158}, 1, now32)},
			},
		},
		{
			"lowestCurrent(metric1,3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricB", []float64{1, 1, 3, 3, 4, 1}, 1, now32),
					types.MakeMetricData("metricC", []float64{1, 1, 3, 3, 4, 15}, 1, now32),
					types.MakeMetricData("metricD", []float64{1, 1, 3, 3, 4, 3}, 1, now32),
					types.MakeMetricData("metricA", []float64{1, 1, 3, 3, 4, 12}, 1, now32),
				},
			},
			"lowestCurrent",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{1, 1, 3, 3, 4, 12}, 1, now32)},
				"metricB": {types.MakeMetricData("metricB", []float64{1, 1, 3, 3, 4, 1}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{1, 1, 3, 3, 4, 3}, 1, now32)},
			},
		},
		{
			"lowestCurrent(metric1)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricB", []float64{1, 1, 3, 3, 4, 1}, 1, now32),
					types.MakeMetricData("metricC", []float64{1, 1, 3, 3, 4, 15}, 1, now32),
					types.MakeMetricData("metricD", []float64{1, 1, 3, 3, 4, 3}, 1, now32),
					types.MakeMetricData("metricA", []float64{1, 1, 3, 3, 4, 12}, 1, now32),
				},
			},
			"lowestCurrent",
			map[string][]*types.MetricData{
				"metricB": {types.MakeMetricData("metricB", []float64{1, 1, 3, 3, 4, 1}, 1, now32)},
			},
		},
		{
			"limit(metric1,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 1, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{0, 0, 1, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricC", []float64{0, 0, 0, 1, 0, 0}, 1, now32),
					types.MakeMetricData("metricD", []float64{0, 0, 0, 0, 1, 0}, 1, now32),
					types.MakeMetricData("metricE", []float64{0, 0, 0, 0, 0, 1}, 1, now32),
				},
			},
			"limit",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{0, 1, 0, 0, 0, 0}, 1, now32)},
				"metricB": {types.MakeMetricData("metricB", []float64{0, 0, 1, 0, 0, 0}, 1, now32)},
			},
		},
		{
			"limit(metric1,20)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 1, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{0, 0, 1, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricC", []float64{0, 0, 0, 1, 0, 0}, 1, now32),
					types.MakeMetricData("metricD", []float64{0, 0, 0, 0, 1, 0}, 1, now32),
					types.MakeMetricData("metricE", []float64{0, 0, 0, 0, 0, 1}, 1, now32),
				},
			},
			"limit",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{0, 1, 0, 0, 0, 0}, 1, now32)},
				"metricB": {types.MakeMetricData("metricB", []float64{0, 0, 1, 0, 0, 0}, 1, now32)},
				"metricC": {types.MakeMetricData("metricC", []float64{0, 0, 0, 1, 0, 0}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{0, 0, 0, 0, 1, 0}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{0, 0, 0, 0, 0, 1}, 1, now32)},
			},
		},
		{
			"mostDeviant(2,metric*)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricD", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricE", []float64{4, 7, 7, 7, 7, 1}, 1, now32),
				},
			},
			"mostDeviant",
			map[string][]*types.MetricData{
				"metricB": {types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{4, 7, 7, 7, 7, 1}, 1, now32)},
			},
		},
		{
			"mostDeviant(metric*,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricD", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricE", []float64{4, 7, 7, 7, 7, 1}, 1, now32),
				},
			},
			"mostDeviant",
			map[string][]*types.MetricData{
				"metricB": {types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{4, 7, 7, 7, 7, 1}, 1, now32)},
			},
		},
		{
			"pearsonClosest(metricC,metric*,2)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricD", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricE", []float64{4, 7, 7, 7, 7, 1}, 1, now32),
				},
				{"metricC", 0, 1}: {
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
				},
			},
			"pearsonClosest",
			map[string][]*types.MetricData{
				"metricC": {types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{4, 4, 5, 5, 6, 6}, 1, now32)},
			},
		},
		{
			"pearsonClosest(metricC,metric*,3)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{0, 0, 0, 0, 0, 0}, 1, now32),
					types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32),
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricD", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
					types.MakeMetricData("metricE", []float64{4, 7, 7, 7, 7, 1}, 1, now32),
				},
				{"metricC", 0, 1}: {
					types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32),
				},
			},
			"pearsonClosest",
			map[string][]*types.MetricData{
				"metricB": {types.MakeMetricData("metricB", []float64{3, 4, 5, 6, 7, 8}, 1, now32)},
				"metricC": {types.MakeMetricData("metricC", []float64{4, 4, 5, 5, 6, 6}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{4, 4, 5, 5, 6, 6}, 1, now32)},
			},
		},
		{
			"tukeyAbove(metric*,1.5,5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 18, 21, 19, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{19, 19, 21, 17, 23, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32),
				},
			},

			"tukeyAbove",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32)},
			},
		},
		{
			"tukeyAbove(metric*, 3, 5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 18, 21, 19, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{19, 19, 21, 17, 23, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32),
				},
			},

			"tukeyAbove",
			map[string][]*types.MetricData{
				"metricE": {types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32)},
			},
		},
		{
			"tukeyAbove(metric*, 1.5, 5, 6)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{20, 20, 20, 20, 21, 17, 20, 20, 10, 29}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 20, 20, 20, 20, 18, 21, 19, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{20, 20, 20, 20, 19, 19, 21, 17, 23, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{20, 20, 20, 20, 18, 20, 22, 14, 26, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{20, 20, 20, 20, 17, 21, 8, 30, 18, 28}, 1, now32),
				},
			},

			"tukeyAbove(metric*, 1.5, 5, 6)",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{20, 20, 20, 20, 21, 17, 20, 20, 10, 29}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{20, 20, 20, 20, 18, 20, 22, 14, 26, 20}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{20, 20, 20, 20, 17, 21, 8, 30, 18, 28}, 1, now32)},
			},
		},
		{
			"tukeyAbove(metric*,1.5,5,\"6s\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{20, 20, 20, 20, 21, 17, 20, 20, 10, 29}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 20, 20, 20, 20, 18, 21, 19, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{20, 20, 20, 20, 19, 19, 21, 17, 23, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{20, 20, 20, 20, 18, 20, 22, 14, 26, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{20, 20, 20, 20, 17, 21, 8, 30, 18, 28}, 1, now32),
				},
			},

			`tukeyAbove(metric*, 1.5, 5, "6s")`,
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{20, 20, 20, 20, 21, 17, 20, 20, 10, 29}, 1, now32)},
				"metricD": {types.MakeMetricData("metricD", []float64{20, 20, 20, 20, 18, 20, 22, 14, 26, 20}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{20, 20, 20, 20, 17, 21, 8, 30, 18, 28}, 1, now32)},
			},
		},
		{
			"tukeyBelow(metric*,1.5,5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 18, 21, 19, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{19, 19, 21, 17, 23, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32),
				},
			},

			"tukeyBelow",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32)},
			},
		},
		{
			"tukeyBelow(metric*,1.5,5,-4)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 18, 21, 19, 20, 20, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{19, 19, 21, 17, 23, 20, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28, 20, 20, 20, 20}, 1, now32),
				},
			},

			"tukeyBelow",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29, 20, 20, 20, 20}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28, 20, 20, 20, 20}, 1, now32)},
			},
		},
		{
			"tukeyBelow(metric*,1.5,5,\"-4s\")",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 18, 21, 19, 20, 20, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{19, 19, 21, 17, 23, 20, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20, 20, 20, 20, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28, 20, 20, 20, 20}, 1, now32),
				},
			},

			"tukeyBelow",
			map[string][]*types.MetricData{
				"metricA": {types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29, 20, 20, 20, 20}, 1, now32)},
				"metricE": {types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28, 20, 20, 20, 20}, 1, now32)},
			},
		},
		{
			"tukeyBelow(metric*,3,5)",
			map[parser.MetricRequest][]*types.MetricData{
				{"metric*", 0, 1}: {
					types.MakeMetricData("metricA", []float64{21, 17, 20, 20, 10, 29}, 1, now32),
					types.MakeMetricData("metricB", []float64{20, 18, 21, 19, 20, 20}, 1, now32),
					types.MakeMetricData("metricC", []float64{19, 19, 21, 17, 23, 20}, 1, now32),
					types.MakeMetricData("metricD", []float64{18, 20, 22, 14, 26, 20}, 1, now32),
					types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32),
				},
			},

			"tukeyBelow",
			map[string][]*types.MetricData{
				"metricE": {types.MakeMetricData("metricE", []float64{17, 21, 8, 30, 18, 28}, 1, now32)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			th.TestMultiReturnEvalExpr(t, &tt)
		})
	}
}

func TestExtractMetric(t *testing.T) {
	var tests = []struct {
		input  string
		metric string
	}{
		{
			"f",
			"f",
		},
		{
			"func(f)",
			"f",
		},
		{
			"foo.bar.baz",
			"foo.bar.baz",
		},
		{
			"nonNegativeDerivative(foo.bar.baz)",
			"foo.bar.baz",
		},
		{
			"movingAverage(foo.bar.baz,10)",
			"foo.bar.baz",
		},
		{
			"scale(scaleToSeconds(nonNegativeDerivative(foo.bar.baz),60),60)",
			"foo.bar.baz",
		},
		{
			"divideSeries(foo.bar.baz,baz.qux.zot)",
			"foo.bar.baz",
		},
		{
			"{something}",
			"{something}",
		},
		{
			"a.b#c.d",
			"a.b#c.d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if m := helper.ExtractMetric(tt.input); m != tt.metric {
				t.Errorf("extractMetric(%q)=%q, want %q", tt.input, m, tt.metric)
			}
		})
	}
}

func TestEvalCustomFromUntil(t *testing.T) {

	tests := []struct {
		//e     parser.Expr
		target string
		m      map[parser.MetricRequest][]*types.MetricData
		w      []float64
		name   string
		from   int32
		until  int32
	}{
		{
			"timeFunction(\"footime\")",
			map[parser.MetricRequest][]*types.MetricData{},
			[]float64{4200.0, 4260.0, 4320.0},
			"footime",
			4200,
			4350,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalMetrics := th.DeepClone(tt.m)
			exp, _, _ := parser.ParseExpr(tt.target)
			g, err := EvalExpr(ctx, exp, tt.from, tt.until, tt.m, noopGetTargetData)
			if err != nil {
				t.Errorf("failed to eval %v: %s", tt.name, err)
				return
			}
			if g[0] == nil {
				t.Errorf("returned no value %v", tt.target)
				return
			}

			th.DeepEqual(t, tt.target, originalMetrics, tt.m)

			if g[0].StepTime == 0 {
				t.Errorf("missing step for %+v", g)
			}
			if !th.NearlyEqual(g[0].Values, g[0].IsAbsent, tt.w) {
				t.Errorf("failed: %s: got %+v, want %+v", g[0].Name, g[0].Values, tt.w)
			}
			if g[0].Name != tt.name {
				t.Errorf("bad name for %+v: got %v, want %v", g, g[0].Name, tt.name)
			}
		})
	}
}
