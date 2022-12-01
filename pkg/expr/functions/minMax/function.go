package minMax

import (
	"context"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type minMax struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &minMax{}
	functions := []string{"minSeries", "min", "maxSeries", "max"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// maxSeries(*seriesLists)
//
//	alias: max
//
// minSeries(*seriesLists)
//
//	alias: min
func (f *minMax) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(ctx, e, from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())

	switch e.Target() {
	case "maxSeries", "max":
		return helper.AggregateSeries(name, args, false, false, func(values []float64) (float64, bool) {
			max := math.Inf(-1)
			for _, value := range values {
				if value > max {
					max = value
				}
			}
			return max, false
		})
	case "minSeries", "min":
		return helper.AggregateSeries(name, args, false, false, func(values []float64) (float64, bool) {
			min := math.Inf(1)
			for _, value := range values {
				if value < min {
					min = value
				}
			}
			return min, false
		})
	}

	return nil, fmt.Errorf("%w: unsupported target: %v", parser.ErrInvalidArgumentValue, e.Target())
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *minMax) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"minSeries": {
			Description: "Takes one metric or a wildcard seriesList.\nFor each datapoint from each metric passed in, pick the minimum value and graph it.\n\nExample:\n\n.. code-block:: none\n\n  &target=minSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``min``.",
			Function:    "minSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "minSeries",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"min": {
			Description: "Takes one metric or a wildcard seriesList.\nFor each datapoint from each metric passed in, pick the minimum value and graph it.\n\nExample:\n\n.. code-block:: none\n\n  &target=minSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``min``.",
			Function:    "minSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "minSeries",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"maxSeries": {
			Description: "Takes one metric or a wildcard seriesList.\nFor each datapoint from each metric passed in, pick the maximum value and graph it.\n\nExample:\n\n.. code-block:: none\n\n  &target=maxSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``max``.",
			Function:    "maxSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "maxSeries",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"max": {
			Description: "Takes one metric or a wildcard seriesList.\nFor each datapoint from each metric passed in, pick the maximum value and graph it.\n\nExample:\n\n.. code-block:: none\n\n  &target=maxSeries(Server*.connections.total)\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``max``.",
			Function:    "maxSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "maxSeries",
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
