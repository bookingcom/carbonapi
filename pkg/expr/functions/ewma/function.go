package ewma

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/dgryski/go-onlinestats"
)

type ewma struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &ewma{}
	functions := []string{"ewma", "exponentialWeightedMovingAverage"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// ewma(seriesList, alpha)
func (f *ewma) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	arg, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	alpha, err := e.GetFloatArg(1)
	if err != nil {
		return nil, err
	}

	e.SetTarget("ewma")

	// ugh, helper.ForEachSeriesDo does not handle arguments properly
	var results []*types.MetricData
	for _, a := range arg {
		name := fmt.Sprintf("ewma(%s,%v)", a.Name, alpha)

		r := *a
		r.Name = name
		r.Values = make([]float64, len(a.Values))
		r.IsAbsent = make([]bool, len(a.Values))

		ewma := onlinestats.NewExpWeight(alpha)

		for i, v := range a.Values {
			if a.IsAbsent[i] {
				r.IsAbsent[i] = true
				continue
			}

			ewma.Push(v)
			r.Values[i] = ewma.Mean()
		}
		results = append(results, &r)
	}
	return results, nil
}

func (f *ewma) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"exponentialWeightedMovingAverage": {
			Description: "Takes a series of values and a alpha and produces an exponential moving\naverage using algorithm described at https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average\n\nExample:\n\n.. code-block:: none\n\n  &target=exponentialWeightedMovingAverage(*.transactions.count, 0.1)",
			Function:    "exponentialWeightedMovingAverage(seriesList, alpha)",
			Group:       "Calculate",
			Module:      "graphite.render.functions.custom",
			Name:        "exponentialWeightedMovingAverage",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "alpha",
					Required: true,
					Suggestions: types.NewSuggestions(
						0.1,
						0.5,
						0.7,
					),
					Type: types.Float,
				},
			},
		},
		"ewma": {
			Description: "Takes a series of values and a alpha and produces an exponential moving\naverage using algorithm described at https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average\n\nExample:\n\n.. code-block:: none\n\n  &target=exponentialWeightedMovingAverage(*.transactions.count, 0.1)",
			Function:    "exponentialWeightedMovingAverage(seriesList, alpha)",
			Group:       "Calculate",
			Module:      "graphite.render.functions.custom",
			Name:        "ewma",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "alpha",
					Required: true,
					Suggestions: types.NewSuggestions(
						0.1,
						0.5,
						0.7,
					),
					Type: types.Float,
				},
			},
		},
	}
}
