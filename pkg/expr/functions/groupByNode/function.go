package groupByNode

import (
	"context"
	"fmt"
	"strings"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type groupByNode struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &groupByNode{}
	functions := []string{"groupByNode", "groupByNodes"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// groupByNode(seriesList, nodeNum, callback)
// groupByNodes(seriesList, callback, *nodes)
func (f *groupByNode) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	var callback string
	var fields []int

	if e.Target() == "groupByNode" {
		var field int
		field, err = e.GetIntArg(1)
		if err != nil {
			return nil, err
		}

		callback, err = e.GetStringArg(2)
		if err != nil {
			return nil, err
		}
		fields = []int{field}
	} else {
		callback, err = e.GetStringArg(1)
		if err != nil {
			return nil, err
		}

		fields, err = e.GetIntArgs(2)
		if err != nil {
			return nil, err
		}
	}

	var results []*types.MetricData

	groups := make(map[string][]*types.MetricData)
	nodeList := []string{}
	for _, a := range args {
		metric := helper.ExtractMetric(a.Name)
		nodes := strings.Split(metric, ".")
		nodeKey := make([]string, 0, len(fields))
		for _, f := range fields {
			if f < 0 || f >= len(nodes) {
				return nil, fmt.Errorf("%s: %w: %d", e.Target(), parser.ErrInvalidArgumentValue, f)
			}
			nodeKey = append(nodeKey, nodes[f])
		}
		node := strings.Join(nodeKey, ".")
		if len(groups[node]) == 0 {
			nodeList = append(nodeList, node)
		}

		groups[node] = append(groups[node], a)
	}

	for _, k := range nodeList {
		k := k // k's reference is used later, so it's important to make it unique per loop
		v := groups[k]

		// Ensure that names won't be parsed as consts, appending stub to them
		expr := fmt.Sprintf("%s(stub_%s)", callback, k)

		// create a stub context to evaluate the callback in
		nexpr, _, err := parser.ParseExpr(expr)
		// remove all stub_ prefixes we've prepended before
		nexpr.SetRawArgs(strings.Replace(nexpr.RawArgs(), "stub_", "", 1))
		for argIdx := range nexpr.Args() {
			nexpr.Args()[argIdx].SetTarget(strings.Replace(nexpr.Args()[0].Target(), "stub_", "", 1))
		}
		if err != nil {
			return nil, err
		}

		nvalues := values
		if e.Target() == "groupByNode" || e.Target() == "groupByNodes" {
			nvalues = map[parser.MetricRequest][]*types.MetricData{
				{
					Metric: k,
					From:   from,
					Until:  until,
				}: v,
			}
		}

		r, _ := f.Evaluator.EvalExpr(ctx, nexpr, from, until, nvalues, getTargetData)
		if r != nil {
			r[0].Name = k
			results = append(results, r...)
		}
	}

	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *groupByNode) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"groupByNode": {
			Description: "Takes a serieslist and maps a callback to subgroups within as defined by a common node\n\n.. code-block:: none\n\n  &target=groupByNode(ganglia.by-function.*.*.cpu.load5,2,\"sumSeries\")\n\nWould return multiple series which are each the result of applying the \"sumSeries\" function\nto groups joined on the second node (0 indexed) resulting in a list of targets like\n\n.. code-block :: none\n\n  sumSeries(ganglia.by-function.server1.*.cpu.load5),sumSeries(ganglia.by-function.server2.*.cpu.load5),...\n\nNode may be an integer referencing a node in the series name or a string identifying a tag.\n\nThis is an alias for using :py:func:`groupByNodes <groupByNodes>` with a single node.",
			Function:    "groupByNode(seriesList, nodeNum, callback='average')",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "groupByNode",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "nodeNum",
					Required: true,
					Type:     types.NodeOrTag,
				},
				{
					Default: types.NewSuggestion("average"),
					Name:    "callback",
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
					},
					Required: true,
					Type:     types.AggFunc,
				},
			},
		},
		"groupByNodes": {
			Description: "Takes a serieslist and maps a callback to subgroups within as defined by multiple nodes\n\n.. code-block:: none\n\n  &target=groupByNodes(ganglia.server*.*.cpu.load*,\"sum\",1,4)\n\nWould return multiple series which are each the result of applying the \"sum\" aggregation\nto groups joined on the nodes' list (0 indexed) resulting in a list of targets like\n\n.. code-block :: none\n\n  sumSeries(ganglia.server1.*.cpu.load5),sumSeries(ganglia.server1.*.cpu.load10),sumSeries(ganglia.server1.*.cpu.load15),sumSeries(ganglia.server2.*.cpu.load5),sumSeries(ganglia.server2.*.cpu.load10),sumSeries(ganglia.server2.*.cpu.load15),...\n\nThis function can be used with all aggregation functions supported by\n:py:func:`aggregate <aggregate>`: ``average``, ``median``, ``sum``, ``min``, ``max``, ``diff``,\n``stddev``, ``range`` & ``multiply``.\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. code-block :: none\n\n  &target=seriesByTag(\"name=~cpu.load.*\", \"server=~server[1-9}+\", \"datacenter=~dc[1-9}+\")|groupByNodes(\"average\", \"datacenter\", 1)\n\n  # will produce output series like\n  # dc1.load5, dc2.load5, dc1.load10, dc2.load10\n\nThis complements :py:func:`aggregateWithWildcards <aggregateWithWildcards>` which takes a list of wildcard nodes.",
			Function:    "groupByNodes(seriesList, callback, *nodes)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "groupByNodes",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name: "callback",
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
					},
					Required: true,
					Type:     types.AggFunc,
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
