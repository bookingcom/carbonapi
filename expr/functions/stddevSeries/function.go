package stddevSeries

import (
	"context"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type stddevSeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &stddevSeries{}
	functions := []string{"stddev", "stddevSeries"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// stddevSeries(*seriesLists)
func (f *stddevSeries) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(ctx, e, from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	e.SetTarget("stddevSeries")
	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())
	return helper.AggregateSeries(name, args, false, false, func(values []float64) (float64, bool) {
		sum := 0.0
		diffSqr := 0.0
		for _, value := range values {
			sum += value
		}
		average := sum / float64(len(values))
		for _, value := range values {
			diffSqr += (value - average) * (value - average)
		}
		return math.Sqrt(diffSqr / float64(len(values))), false
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *stddevSeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"stddevSeries": {
			Description: "Takes one metric or a wildcard seriesList.\nDraws the standard deviation of all metrics passed at each time.\n\nExample:\n\n.. code-block:: none\n\n  &target=stddevSeries(company.server.*.threads.busy)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``stddev``.",
			Function:    "stddevSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "stddevSeries",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
