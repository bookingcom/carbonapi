package aliasByNode

import (
	"context"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"

	"strings"
)

type aliasByNode struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &aliasByNode{}
	for _, n := range []string{"aliasByNode"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *aliasByNode) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	fields, err := e.GetIntArgs(1)
	if err != nil {
		return nil, err
	}

	var results []*types.MetricData

	for _, a := range args {

		metric := helper.ExtractMetric(a.Name)
		nodes := strings.Split(metric, ".")

		var name []string
		for _, f := range fields {
			if f < 0 {
				f += len(nodes)
			}
			if f >= len(nodes) || f < 0 {
				continue
			}
			name = append(name, nodes[f])
		}

		r := *a
		r.Name = strings.Join(name, ".")
		results = append(results, &r)
	}

	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *aliasByNode) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"aliasByNode": {
			Description: "Takes a seriesList and applies an alias derived from one or more \"node\"\nportion/s of the target name or tags. Node indices are 0 indexed.\n\n.. code-block:: none\n\n  &target=aliasByNode(ganglia.*.cpu.load5,1)\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. code-block :: none\n\n  &target=seriesByTag(\"name=~cpu.load.*\", \"server=~server[1-9}+\", \"datacenter=dc1\")|aliasByNode(\"datacenter\", \"server\", 1)\n\n  # will produce output series like\n  # dc1.server1.load5, dc1.server2.load5, dc1.server1.load10, dc1.server2.load10",
			Function:    "aliasByNode(seriesList, *nodes)",
			Group:       "Alias",
			Module:      "graphite.render.functions",
			Name:        "aliasByNode",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Multiple: true,
					Name:     "nodes",
					Required: true,
					Type:     types.NodeOrTag,
				},
			},
		},
	}
}
