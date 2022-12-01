package seriesList

import (
	"context"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type seriesList struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &seriesList{}
	functions := []string{"divideSeriesLists", "diffSeriesLists", "multiplySeriesLists", "powSeriesLists"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *seriesList) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	numerators, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	denominators, err := helper.GetSeriesArg(ctx, e.Args()[1], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	if len(numerators) != len(denominators) {
		return nil, fmt.Errorf("%w: %s", parser.ErrDifferentCountMetrics, e.Target())
	}

	var results []*types.MetricData
	functionName := e.Target()[:len(e.Target())-len("Lists")]

	var compute func(l, r float64) (float64, bool)

	switch e.Target() {
	case "divideSeriesLists":
		compute = func(l, r float64) (float64, bool) {
			if r == 0 {
				return 0, true
			}
			return l / r, false
		}
	case "multiplySeriesLists":
		compute = func(l, r float64) (float64, bool) { return l * r, false }
	case "diffSeriesLists":
		compute = func(l, r float64) (float64, bool) { return l - r, false }
	case "powSeriesLists":
		compute = func(l, r float64) (float64, bool) { return math.Pow(l, r), false }
	}
	for i, numerator := range numerators {
		denominator := denominators[i]
		name := fmt.Sprintf("%s(%s,%s)", functionName, numerator.Name, denominator.Name)
		result := helper.CombineSeries(numerator, denominator, name, compute)
		results = append(results, result)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *seriesList) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"divideSeriesLists": {
			Description: "Iterates over a two lists and divides list1[0} by list2[0}, list1[1} by list2[1} and so on.\nThe lists need to be the same length",
			Function:    "divideSeriesLists(dividendSeriesList, divisorSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "divideSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "dividendSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "divisorSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"diffSeriesLists": {
			Description: "Iterates over a two lists and substracts list1[0} by list2[0}, list1[1} by list2[1} and so on.\nThe lists need to be the same length",
			Function:    "diffSeriesLists(firstSeriesList, secondSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "diffSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "firstSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "secondSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"multiplySeriesLists": {
			Description: "Iterates over a two lists and multiplies list1[0} by list2[0}, list1[1} by list2[1} and so on.\nThe lists need to be the same length",
			Function:    "multiplySeriesLists(sourceSeriesList, factorSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "multiplySeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "sourceSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "factorSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"powSeriesLists": {
			Description: "Iterates over a two lists and do list1[0} in power of list2[0}, list1[1} in power of  list2[1} and so on.\nThe lists need to be the same length",
			Function:    "powSeriesLists(sourceSeriesList, factorSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "powSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "sourceSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "factorSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
