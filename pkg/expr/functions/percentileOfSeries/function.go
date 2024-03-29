package percentileOfSeries

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type percentileOfSeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &percentileOfSeries{}
	functions := []string{"percentileOfSeries"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// percentileOfSeries(seriesList, n, interpolate=False)
func (f *percentileOfSeries) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	// TODO(dgryski): make sure the arrays are all the same 'size'
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	percent, err := e.GetFloatArg(1)
	if err != nil {
		return nil, err
	}

	interpolate, err := e.GetBoolNamedOrPosArgDefault("interpolate", 2, false)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())
	return helper.AggregateSeries(name, args, false, false, func(values []float64) (float64, bool) {
		return helper.Percentile(values, percent, interpolate)
	})
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *percentileOfSeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"percentileOfSeries": {
			Description: "percentileOfSeries returns a single series which is composed of the n-percentile\nvalues taken across a wildcard series at each point. Unless `interpolate` is\nset to True, percentile values are actual values contained in one of the\nsupplied series.",
			Function:    "percentileOfSeries(seriesList, n, interpolate=False)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "percentileOfSeries",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "n",
					Required: true,
					Type:     types.Integer,
				},
				{
					Default: types.NewSuggestion(false),
					Name:    "interpolate",
					Type:    types.Boolean,
				},
			},
		},
	}
}
