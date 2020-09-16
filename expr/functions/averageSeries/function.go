package averageSeries

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type averageSeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	f := &averageSeries{}
	res := make([]interfaces.FunctionMetadata, 0)
	for _, n := range []string{"avg", "average", "averageSeries"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// averageSeries(*seriesLists)
func (f *averageSeries) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(ctx, e, from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	e.SetTarget("averageSeries")
	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())
	return helper.AggregateSeries(name, args, false, false, func(values []float64) (float64, bool) {
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		return sum / float64(len(values)), false
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *averageSeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"avg": {
			Description: "Short Alias: avg()\n\nTakes one metric or a wildcard seriesList.\nDraws the average value of all metrics passed at each time.\n\nExample:\n\n.. code-block:: none\n\n  &target=averageSeries(company.server.*.threads.busy)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``average``.",
			Function:    "avg(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "avg",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"averageSeries": {
			Description: "Short Alias: avg()\n\nTakes one metric or a wildcard seriesList.\nDraws the average value of all metrics passed at each time.\n\nExample:\n\n.. code-block:: none\n\n  &target=averageSeries(company.server.*.threads.busy)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``average``.",
			Function:    "averageSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "averageSeries",
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
