package group

import (
	"context"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type group struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &group{}
	functions := []string{"group"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// group(*seriesLists)
func (f *group) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(ctx, e, from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	return args, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *group) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"group": {
			Description: "Takes an arbitrary number of seriesLists and adds them to a single seriesList. This is used\nto pass multiple seriesLists to a function which only takes one",
			Function:    "group(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "group",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Type:     types.SeriesList,
				},
			},
		},
	}
}
