package aliasByMetric

import (
	"context"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"

	"strings"
)

type aliasByMetric struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &aliasByMetric{}
	for _, n := range []string{"aliasByMetric"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *aliasByMetric) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return helper.ForEachSeriesDo(ctx, e, from, until, values, func(a *types.MetricData, r *types.MetricData) *types.MetricData {
		metric := helper.ExtractMetric(a.Name)
		part := strings.Split(metric, ".")
		r.Name = part[len(part)-1]
		r.Values = a.Values
		r.IsAbsent = a.IsAbsent
		return r
	}, getTargetData)
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *aliasByMetric) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"aliasByMetric": {
			Description: "Takes a seriesList and applies an alias derived from the base metric name.\n\n.. code-block:: none\n\n  &target=aliasByMetric(carbon.agents.graphite.creates)",
			Function:    "aliasByMetric(seriesList)",
			Group:       "Alias",
			Module:      "graphite.render.functions",
			Name:        "aliasByMetric",
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
