package legendValue

import (
	"context"
	"fmt"
	"strings"

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
	var methods []string
	for i := 1; i < len(e.Args()); i++ {
		method, err := e.GetStringArg(i)
		if err != nil {
			return nil, err
		}

		if method == "si" || method == "binary" {
			system = method
		} else {
			methods = append(methods, method)
		}
	}

	var results []*types.MetricData

	for _, a := range arg {
		var values []string
		for _, method := range methods {
			summaryVal, _, err := helper.SummarizeValues(method, a.Values)
			if err != nil {
				return []*types.MetricData{}, err
			}

			summary := ""
			switch system {
			case "si":
				sv, sf := humanize.ComputeSI(summaryVal)
				summary = fmt.Sprintf("%.1f %s", sv, sf)
			case "binary":
				summary = humanize.IBytes(uint64(summaryVal))
			case "":
				summary = fmt.Sprintf("%f", summaryVal)
			default:
				return nil, fmt.Errorf("%s is not supported for system", system)
			}
			values = append(values, fmt.Sprintf("%s: %s", method, summary))
		}

		r := *a
		r.Name = fmt.Sprintf("%s (%s)", r.Name, strings.Join(values, ", "))
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
