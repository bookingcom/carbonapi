package holtWintersConfidenceBands

import (
	"context"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/holtwinters"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
)

type holtWintersConfidenceBands struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &holtWintersConfidenceBands{}
	functions := []string{"holtWintersConfidenceBands"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *holtWintersConfidenceBands) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	var results []*types.MetricData
	args, err := helper.GetSeriesArg(ctx, e.Args()[0], from-7*86400, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	delta, err := e.GetFloatNamedOrPosArgDefault("delta", 1, 3)
	if err != nil {
		return nil, err
	}

	for _, arg := range args {
		stepTime := arg.StepTime
		values := make([]float64, len(arg.Values))
		for i := 0; i < len(values); i++ {
			values[i] = arg.Values[i]
			if arg.IsAbsent[i] {
				values[i] = math.NaN()
			}
		}
		datapoints := int((until - from) / stepTime)
		lowerBand, upperBand := holtwinters.HoltWintersConfidenceBands(values, datapoints, stepTime, delta)
		lowerSeries := types.MetricData{Metric: dataTypes.Metric{
			Name:      fmt.Sprintf("holtWintersConfidenceLower(%s)", arg.Name),
			Values:    lowerBand,
			IsAbsent:  make([]bool, len(lowerBand)),
			StepTime:  arg.StepTime,
			StartTime: arg.StopTime - int32(datapoints)*stepTime,
			StopTime:  arg.StopTime,
		}}

		for i, val := range lowerSeries.Values {
			if math.IsNaN(val) {
				lowerSeries.Values[i] = 0
				lowerSeries.IsAbsent[i] = true
			}
		}

		upperSeries := types.MetricData{Metric: dataTypes.Metric{
			Name:      fmt.Sprintf("holtWintersConfidenceUpper(%s)", arg.Name),
			Values:    upperBand,
			IsAbsent:  make([]bool, len(upperBand)),
			StepTime:  arg.StepTime,
			StartTime: arg.StopTime - int32(datapoints)*stepTime,
			StopTime:  arg.StopTime,
		}}

		for i, val := range upperSeries.Values {
			if math.IsNaN(val) {
				upperSeries.Values[i] = 0
				upperSeries.IsAbsent[i] = true
			}
		}

		results = append(results, &lowerSeries)
		results = append(results, &upperSeries)
	}
	return results, nil

}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *holtWintersConfidenceBands) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"holtWintersConfidenceBands": {
			Description: "Performs a Holt-Winters forecast using the series as input data and plots\nupper and lower bands with the predicted forecast deviations.",
			Function:    "holtWintersConfidenceBands(seriesList, delta=3, bootstrapInterval='7d')",
			Group:       "Calculate",
			Module:      "graphite.render.functions",
			Name:        "holtWintersConfidenceBands",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Default: types.NewSuggestion(3),
					Name:    "delta",
					Type:    types.Integer,
				},
				{
					Default: types.NewSuggestion("7d"),
					Name:    "bootstrapInterval",
					Suggestions: types.NewSuggestions(
						"7d",
						"30d",
					),
					Type: types.Interval,
				},
			},
		},
	}
}
