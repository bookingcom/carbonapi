package scaleToSeconds

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type scaleToSeconds struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &scaleToSeconds{}
	functions := []string{"scaleToSeconds"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// scaleToSeconds(seriesList, seconds)
func (f *scaleToSeconds) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	arg, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	seconds, err := e.GetFloatArg(1)
	if err != nil {
		return nil, err
	}

	var results []*types.MetricData

	for _, a := range arg {
		r := *a
		r.Name = fmt.Sprintf("scaleToSeconds(%s,%d)", a.Name, int(seconds))
		r.Values = make([]float64, len(a.Values))
		r.IsAbsent = make([]bool, len(a.Values))

		factor := seconds / float64(a.StepTime)

		for i, v := range a.Values {
			if a.IsAbsent[i] {
				r.Values[i] = 0
				r.IsAbsent[i] = true
				continue
			}
			r.Values[i] = v * factor
		}
		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *scaleToSeconds) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"scaleToSeconds": {
			Description: "Takes one metric or a wildcard seriesList and returns \"value per seconds\" where\nseconds is a last argument to this functions.\n\nUseful in conjunction with derivative or integral function if you want\nto normalize its result to a known resolution for arbitrary retentions",
			Function:    "scaleToSeconds(seriesList, seconds)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "scaleToSeconds",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "seconds",
					Required: true,
					Type:     types.Integer,
				},
			},
		},
	}
}
