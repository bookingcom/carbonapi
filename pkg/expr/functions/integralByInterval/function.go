package integralByInterval

import (
	"context"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
)

type integralByInterval struct {
	interfaces.FunctionBase
}

// GetOrder define the execution order of functions
func GetOrder() interfaces.Order {
	return interfaces.Any
}

// New create new integralByInterval functions.
func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &integralByInterval{}
	functions := []string{"integralByInterval"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// Do integralByInterval(seriesList, intervalString)
func (f *integralByInterval) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, nil
	}

	bucketSize, err := e.GetIntervalArg(1, 1)
	if err != nil {
		return nil, err
	}

	startTime := from
	results := make([]*types.MetricData, 0, len(args))
	for _, arg := range args {
		current := 0.0
		currentTime := arg.StartTime

		name := fmt.Sprintf("integralByInterval(%s,'%s')", arg.Name, e.Args()[1].StringValue())
		result := &types.MetricData{
			Metric: dataTypes.Metric{
				Name:      name,
				Values:    make([]float64, len(arg.Values)),
				IsAbsent:  arg.IsAbsent,
				StepTime:  arg.StepTime,
				StartTime: arg.StartTime,
				StopTime:  arg.StopTime,
			},
		}
		for i, v := range arg.Values {
			if (currentTime-startTime)/bucketSize != (currentTime-startTime-arg.StepTime)/bucketSize {
				current = 0
			}
			if math.IsNaN(v) {
				v = 0
			}
			current += v
			result.Values[i] = current
			currentTime += arg.StepTime
		}
		results = append(results, result)
	}

	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *integralByInterval) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"integralByInterval": {
			Description: "This will do the same as integral() funcion, except resetting the total to 0 at the given time in the parameter “from” Useful for finding totals per hour/day/week/..",
			Function:    "integralByInterval(seriesList, intervalString)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "integralByInterval",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				}, {
					Name:     "intervalString",
					Required: true,
					Suggestions: types.NewSuggestions(
						"10min",
						"1h",
						"1d",
					),
					Type: types.Interval,
				},
			},
		},
	}
}
