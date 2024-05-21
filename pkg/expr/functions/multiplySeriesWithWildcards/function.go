package multiplySeriesWithWildcards

import (
	"context"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type multiplySeriesWithWildcards struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &multiplySeriesWithWildcards{}
	functions := []string{"multiplySeriesWithWildcards"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// multiplySeriesWithWildcards(seriesList, *position)
func (f *multiplySeriesWithWildcards) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	/* TODO(dgryski): make sure the arrays are all the same 'size'
	   (duplicated from sumSeriesWithWildcards because of similar logic but multiplication) */
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	fields, err := e.GetIntArgs(1)
	if err != nil {
		return nil, err
	}

	return helper.AggregateSeriesWithWildcards("multiplySeriesWithWildcards", args, fields, func(values []float64) (float64, bool) {
		ret := values[0]
		for _, value := range values[1:] {
			ret *= value
		}

		return ret, false
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *multiplySeriesWithWildcards) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"multiplySeriesWithWildcards": {
			Description: "Call multiplySeries after inserting wildcards at the given position(s).\n\nExample:\n\n.. code-block:: none\n\n  &target=multiplySeriesWithWildcards(web.host-[0-7}.{avg-response,total-request}.value, 2)\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=multiplySeries(web.host-0.{avg-response,total-request}.value)&target=multiplySeries(web.host-1.{avg-response,total-request}.value)...\n\nThis is an alias for :py:func:`aggregateWithWildcards <aggregateWithWildcards>` with aggregation ``multiply``.",
			Function:    "multiplySeriesWithWildcards(seriesList, *position)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "multiplySeriesWithWildcards",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Multiple: true,
					Name:     "position",
					Type:     types.Node,
				},
			},
		},
	}
}
