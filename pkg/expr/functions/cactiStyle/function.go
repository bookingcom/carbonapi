package cactiStyle

import (
	"context"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/dustin/go-humanize"
)

type cactiStyle struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &cactiStyle{}
	functions := []string{"cactiStyle"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// cactiStyle(seriesList, system=None, units=None)
func (f *cactiStyle) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	// Get the series data
	original, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	// Get the arguments
	system, err := e.GetStringNamedOrPosArgDefault("system", 1, "")
	if err != nil {
		return nil, err
	}
	unit, err := e.GetStringNamedOrPosArgDefault("units", 2, "")
	if err != nil {
		return nil, err
	}

	// Deal with each of the series
	var metrics []*types.MetricData
	for _, a := range original {
		// Calculate min, max, current
		//
		// This saves calling helper.SummarizeValues 3 times and looping over
		// the metrics 3 times
		//
		// For min:
		// Ignoring any absent values and inf (if we have a value)
		// Using helper.SummarizeValues("min", ...) results in incorrect values, when absent
		// values are present
		//
		minVal := math.Inf(1)
		currentVal := math.Inf(-1)
		maxVal := math.Inf(-1)
		for i, av := range a.Values {
			if !a.IsAbsent[i] {
				minVal = math.Min(minVal, av)
				maxVal = math.Max(maxVal, av)
				currentVal = av
			}
		}

		// Format the output correctly
		min := ""
		max := ""
		current := ""
		switch system {
		case "si":
			mv, mf := humanize.ComputeSI(minVal)
			xv, xf := humanize.ComputeSI(maxVal)
			cv, cf := humanize.ComputeSI(currentVal)

			min = fmt.Sprintf("%.0f%s", mv, mf)
			max = fmt.Sprintf("%.0f%s", xv, xf)
			current = fmt.Sprintf("%.0f%s", cv, cf)

		case "":
			min = fmt.Sprintf("%.0f", minVal)
			max = fmt.Sprintf("%.0f", maxVal)
			current = fmt.Sprintf("%.0f", currentVal)

		default:
			return nil, fmt.Errorf("%s is not supported for system", system)
		}

		// Append the unit if specified
		if len(unit) > 0 {
			min = fmt.Sprintf("%s %s", min, unit)
			max = fmt.Sprintf("%s %s", max, unit)
			current = fmt.Sprintf("%s %s", current, unit)
		}

		r := *a
		r.Name = fmt.Sprintf("%s Current: %s Max: %s Min: %s", a.Name, current, max, min)
		metrics = append(metrics, &r)
	}

	return metrics, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *cactiStyle) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"cactiStyle": {
			Description: "Takes a series list and modifies the aliases to provide column aligned\noutput with Current, Max, and Min values in the style of cacti. Optionally\ntakes a \"system\" value to apply unit formatting in the same style as the\nY-axis, or a \"unit\" string to append an arbitrary unit suffix.\n\n.. code-block:: none\n\n  &target=cactiStyle(ganglia.*.net.bytes_out,\"si\")\n  &target=cactiStyle(ganglia.*.net.bytes_out,\"si\",\"b\")\n\nA possible value for ``system`` is ``si``, which would express your values in\nmultiples of a thousand. A second option is to use ``binary`` which will\ninstead express your values in multiples of 1024 (useful for network devices).\n\nColumn alignment of the Current, Max, Min values works under two conditions:\nyou use a monospace font such as terminus and use a single cactiStyle call, as\nseparate cactiStyle calls are not aware of each other. In case you have\ndifferent targets for which you would like to have cactiStyle to line up, you\ncan use ``group()`` to combine them before applying cactiStyle, such as:\n\n.. code-block:: none\n\n  &target=cactiStyle(group(metricA,metricB))",
			Function:    "cactiStyle(seriesList, system=None, units=None)",
			Group:       "Special",
			Module:      "graphite.render.functions",
			Name:        "cactiStyle",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name: "system",
					Options: []string{
						"si",
						"binary",
					},
					Type: types.String,
				},
				{
					Name: "units",
					Type: types.String,
				},
			},
		},
	}
}
