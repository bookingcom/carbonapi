package offsetToZero

import (
	"context"
	"math"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type offsetToZero struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &offsetToZero{}
	functions := []string{"offsetToZero"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// offsetToZero(seriesList)
func (f *offsetToZero) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return helper.ForEachSeriesDo(ctx, e, from, until, values, func(a *types.MetricData, r *types.MetricData) *types.MetricData {
		minimum := math.Inf(1)
		for i, v := range a.Values {
			if !a.IsAbsent[i] && v < minimum {
				minimum = v
			}
		}
		for i, v := range a.Values {
			if a.IsAbsent[i] {
				r.Values[i] = 0
				r.IsAbsent[i] = true
				continue
			}
			r.Values[i] = v - minimum
		}
		return r
	}, getTargetData)
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *offsetToZero) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"offsetToZero": {
			Description: "Offsets a metric or wildcard seriesList by subtracting the minimum\nvalue in the series from each datapoint.\n\nUseful to compare different series where the values in each series\nmay be higher or lower on average but you're only interested in the\nrelative difference.\n\nAn example use case is for comparing different round trip time\nresults. When measuring RTT (like pinging a server), different\ndevices may come back with consistently different results due to\nnetwork latency which will be different depending on how many\nnetwork hops between the probe and the device. To compare different\ndevices in the same graph, the network latency to each has to be\nfactored out of the results. This is a shortcut that takes the\nfastest response (lowest number in the series) and sets that to zero\nand then offsets all of the other datapoints in that series by that\namount. This makes the assumption that the lowest response is the\nfastest the device can respond, of course the more datapoints that\nare in the series the more accurate this assumption is.\n\nExample:\n\n.. code-block:: none\n\n  &target=offsetToZero(Server.instance01.responseTime)\n  &target=offsetToZero(Server.instance*.responseTime)",
			Function:    "offsetToZero(seriesList)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "offsetToZero",
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
