package invert

import (
	"context"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type invert struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &invert{}
	functions := []string{"invert"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// invert(seriesList)
func (f *invert) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return helper.ForEachSeriesDo(ctx, e, from, until, values, func(a *types.MetricData, r *types.MetricData) *types.MetricData {
		for i, v := range a.Values {
			if a.IsAbsent[i] || v == 0 {
				r.Values[i] = 0
				r.IsAbsent[i] = true
				continue
			}
			r.Values[i] = 1 / v
		}
		return r
	}, getTargetData)
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *invert) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"invert": {
			Description: "Takes one metric or a wildcard seriesList, and inverts each datapoint (i.e. 1/x).\n\nExample:\n\n.. code-block:: none\n\n  &target=invert(Server.instance01.threads.busy)",
			Function:    "invert(seriesList)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "invert",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
