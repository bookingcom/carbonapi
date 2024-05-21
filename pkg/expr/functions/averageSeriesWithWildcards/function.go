package averageSeriesWithWildcards

import (
	"context"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type averageSeriesWithWildcards struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &averageSeriesWithWildcards{}
	for _, n := range []string{"averageSeriesWithWildcards"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// averageSeriesWithWildcards(seriesLIst, *position)
func (f *averageSeriesWithWildcards) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	/* TODO(dgryski): make sure the arrays are all the same 'size'
	   (duplicated from sumSeriesWithWildcards because of similar logic but aggregation) */
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	fields, err := e.GetIntArgs(1)
	if err != nil {
		return nil, err
	}

	return helper.AggregateSeriesWithWildcards("averageSeriesWithWildcards", args, fields, func(values []float64) (float64, bool) {
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		return sum / float64(len(values)), false
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *averageSeriesWithWildcards) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"averageSeriesWithWildcards": {
			Description: "Call averageSeries after inserting wildcards at the given position(s).\n\nExample:\n\n.. code-block:: none\n\n  &target=averageSeriesWithWildcards(host.cpu-[0-7}.cpu-{user,system}.value, 1)\n\nThis would be the equivalent of\n\n.. code-block:: none\n\n  &target=averageSeries(host.*.cpu-user.value)&target=averageSeries(host.*.cpu-system.value)\n\nThis is an alias for :py:func:`aggregateWithWildcards <aggregateWithWildcards>` with aggregation ``average``.",
			Function:    "averageSeriesWithWildcards(seriesList, *position)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "averageSeriesWithWildcards",
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
