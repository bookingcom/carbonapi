package transformNonNull

import (
	"math"
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

func TestTransformNonNull(t *testing.T) {
	now32 := int32(time.Now().Unix())
	cases := []th.EvalTestItem{
		{
			"transformNonNull(ns1.ns2.count)",
			map[parser.MetricRequest][]*types.MetricData{
				{"ns1.ns2.count", 0, 1}: {
					types.MakeMetricData("ns1.ns2.count", []float64{3, math.NaN(), 3, 3, 3}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData(
				"transformNonNull(ns1.ns2.count)",
				[]float64{1, math.NaN(), 1, 1, 1}, 1, now32,
			)},
		},
		{
			"transformNonNull(ns1.ns2.count, 100)",
			map[parser.MetricRequest][]*types.MetricData{
				{"ns1.ns2.count", 0, 1}: {
					types.MakeMetricData("ns1.ns2.count", []float64{3, math.NaN(), 3, 3, 3}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData(
				"transformNonNull(ns1.ns2.count,100)",
				[]float64{100, math.NaN(), 100, 100, 100}, 1, now32,
			)},
		},
	}

	for _, c := range cases {
		t.Run(c.Target, func(t *testing.T) {
			th.TestEvalExpr(t, &c)
		})
	}
}
