package legendValue

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/dustin/go-humanize"
)

type legendValue struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &legendValue{}
	functions := []string{"legendValue"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// legendValue(seriesList, newName)
func (f *legendValue) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	arg, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	var system string
	methods := make([]string, len(e.Args())-1)
	for i := 1; i < len(e.Args()); i++ {
		method, err := e.GetStringArg(i)
		if err != nil {
			return nil, err
		}

		if method == "si" || method == "binary" {
			system = method
		} else {
			methods[i-1] = method
		}
	}

	var results []*types.MetricData

	for _, a := range arg {
		r := *a
		for _, method := range methods {
			summaryVal, _, err := helper.SummarizeValues(method, a.Values)
			if err != nil {
				return []*types.MetricData{}, err
			}

			summary := ""
			if system == "si" {
				sv, sf := humanize.ComputeSI(summaryVal)
				summary = fmt.Sprintf("%.1f %s", sv, sf)
			} else if system == "binary" {
				summary = humanize.IBytes(uint64(summaryVal))
			} else if system == "" {
				summary = fmt.Sprintf("%f", summaryVal)
			} else {
				return nil, fmt.Errorf("%s is not supported for system", system)
			}
			r.Name = fmt.Sprintf("%s (%s: %s)", r.Name, method, summary)
		}

		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *legendValue) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"legendValue": {
			Description: "Takes one metric or a wildcard seriesList and a string in quotes.\nAppends a value to the metric name in the legend.  Currently one or several of: `last`, `avg`,\n`total`, `min`, `max`.\nThe last argument can be `si` (default) or `binary`, in that case values will be formatted in the\ncorresponding system.\n\n.. code-block:: none\n\n  &target=legendValue(Sales.widgets.largeBlue, 'avg', 'max', 'si')",
			Function:    "legendValue(seriesList, *valueTypes)",
			Group:       "Alias",
			Module:      "graphite.render.functions",
			Name:        "legendValue",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Multiple: true,
					Name:     "valuesTypes",
					Options: []string{
						"average",
						"count",
						"diff",
						"last",
						"max",
						"median",
						"min",
						"multiply",
						"range",
						"stddev",
						"sum",
						"si",
						"binary",
					},
					Type: types.String,
				},
			},
		},
	}
}
