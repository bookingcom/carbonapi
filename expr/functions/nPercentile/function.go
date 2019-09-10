package nPercentile

import (
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type nPercentile struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &nPercentile{}
	functions := []string{"nPercentile"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// nPercentile(seriesList, n)
func (f *nPercentile) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	arg, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}
	percent, err := e.GetFloatArg(1)
	if err != nil {
		return nil, err
	}

	var results []*types.MetricData
	for _, a := range arg {
		r := *a
		r.Name = fmt.Sprintf("nPercentile(%s,%g)", a.Name, percent)
		r.Values = make([]float64, len(a.Values))
		r.IsAbsent = make([]bool, len(a.Values))

		var values []float64
		for i, v := range a.IsAbsent {
			if !v {
				values = append(values, a.Values[i])
			}
		}

		value := helper.Percentile(values, percent, true)
		for i := range r.Values {
			r.Values[i] = value
		}

		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *nPercentile) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"nPercentile": {
			Description: "Returns n-percent of each series in the seriesList.",
			Function:    "nPercentile(seriesList, n)",
			Group:       "Calculate",
			Module:      "graphite.render.functions",
			Name:        "nPercentile",
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
			},
		},
	}
}
