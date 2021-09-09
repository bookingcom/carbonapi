package medianSeries

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type medianSeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	f := &medianSeries{}
	res := make([]interfaces.FunctionMetadata, 0)
	for _, n := range []string{"median", "medianSeries"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// medianSeries(*seriesLists)
func (f *medianSeries) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(ctx, e, from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	e.SetTarget("medianSeries")
	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())
	return helper.AggregateSeries(name, args, false, false, func(values []float64) (float64, bool) {
		return helper.Percentile(values, 50, true)
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *medianSeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"median": {
			Description: "Short Alias: median()\n\nTakes one metric or a wildcard seriesList.\nDraws the median value of all metrics passed at each time.\n\nExample:\n\n.. code-block:: none\n\n  &target=medianSeries(company.server.*.threads.busy)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``median``.",
			Function:    "median(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "median",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"medianSeries": {
			Description: "Short Alias: medianSeries()\n\nTakes one metric or a wildcard seriesList.\nDraws the median value of all metrics passed at each time.\n\nExample:\n\n.. code-block:: none\n\n  &target=medianSeries(company.server.*.threads.busy)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``median``.",
			Function:    "medianSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "medianSeries",
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
