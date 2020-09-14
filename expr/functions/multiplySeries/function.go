package multiplySeries

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type multiplySeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &multiplySeries{}
	functions := []string{"multiplySeries"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// multiplySeries(factorsSeriesList)
func (f *multiplySeries) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(ctx, e, from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("multiplySeries(%s)", e.RawArgs())
	return helper.AggregateSeries(name, args, false, func(values []float64) (float64, bool) {
		ret := values[0]
		for _, value := range values[1:] {
			ret *= value
		}
		return ret, false
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *multiplySeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"multiplySeries": {
			Description: "Takes two or more series and multiplies their points. A constant may not be\nused. To multiply by a constant, use the scale() function.\n\nExample:\n\n.. code-block:: none\n\n  &target=multiplySeries(Series.dividends,Series.divisors)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``multiply``.",
			Function:    "multiplySeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "multiplySeries",
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
