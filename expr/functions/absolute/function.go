package absolute

import (
	"context"
	"math"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type absolute struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &absolute{}
	for _, n := range []string{"absolute"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *absolute) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return helper.ForEachSeriesDo(ctx, e, from, until, values, func(a *types.MetricData, r *types.MetricData) *types.MetricData {
		for i, v := range a.Values {
			if a.IsAbsent[i] {
				r.Values[i] = 0
				r.IsAbsent[i] = true
				continue
			}
			r.Values[i] = math.Abs(v)
		}
		return r
	}, getTargetData)
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *absolute) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"absolute": {
			Description: "Takes one metric or a wildcard seriesList and applies the mathematical abs function to each\ndatapoint transforming it to its absolute value.\n\nExample:\n\n.. code-block:: none\n\n  &target=absolute(Server.instance01.threads.busy)\n  &target=absolute(Server.instance*.threads.busy)",
			Function:    "absolute(seriesList)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "absolute",
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
