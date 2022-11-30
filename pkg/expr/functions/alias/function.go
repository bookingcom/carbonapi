package alias

import (
	"context"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type alias struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &alias{}
	for _, n := range []string{"alias"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *alias) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	arg, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	alias, err := e.GetStringArg(1)
	if err != nil {
		return nil, err
	}

	var results []*types.MetricData

	for _, a := range arg {
		r := *a
		r.Name = alias
		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *alias) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"alias": {
			Description: "Takes one metric or a wildcard seriesList and a string in quotes.\nPrints the string instead of the metric name in the legend.\n\n.. code-block:: none\n\n  &target=alias(Sales.widgets.largeBlue,\"Large Blue Widgets\")",
			Function:    "alias(seriesList, newName)",
			Group:       "Alias",
			Module:      "graphite.render.functions",
			Name:        "alias",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "newName",
					Required: true,
					Type:     types.String,
				},
			},
		},
	}
}
