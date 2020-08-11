package divideSeries

import (
	"errors"
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type divideSeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &divideSeries{}
	functions := []string{"divideSeries"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// divideSeries(dividendSeriesList, divisorSeriesList)
func (f *divideSeries) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	if len(e.Args()) < 1 {
		return nil, parser.ErrMissingTimeseries
	}

	firstArg, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}

	var useMetricNames bool

	var numerators []*types.MetricData
	var originalDenominator *types.MetricData
	if len(e.Args()) == 2 {
		useMetricNames = true
		numerators = firstArg
		denominators, err := helper.GetSeriesArg(e.Args()[1], from, until, values)
		if err != nil {
			return nil, err
		}
		if len(denominators) != 1 {
			return nil, types.ErrWildcardNotAllowed
		}

		originalDenominator = denominators[0]
	} else if len(firstArg) == 2 && len(e.Args()) == 1 {
		numerators = append(numerators, firstArg[0])
		originalDenominator = firstArg[1]
	} else {
		return nil, errors.New("must be called with 2 series or a wildcard that matches exactly 2 series")
	}

	denominator := *originalDenominator
	var results []*types.MetricData
	for _, originalNumerator := range numerators {
		numerator := *originalNumerator
		name := fmt.Sprintf("divideSeries(%s)", e.RawArgs())
		if useMetricNames {
			name = fmt.Sprintf("divideSeries(%s,%s)", numerator.Name, denominator.Name)
		}

		step := helper.LCM(numerator.StepTime, denominator.StepTime)

		numerator.SetValuesPerPoint(int(step / numerator.StepTime))
		denominator.SetValuesPerPoint(int(step / denominator.StepTime))

		start := numerator.StartTime
		if start > denominator.StartTime {
			start = denominator.StartTime
		}
		end := numerator.StopTime
		if end < denominator.StopTime {
			end = denominator.StopTime
		}
		end -= (end - start) % step
		length := int((end - start) / step)

		numeratorAbsent := numerator.AggregatedAbsent()
		if len(numeratorAbsent) > length {
			length = len(numeratorAbsent)
		}
		numeratorValues := numerator.AggregatedValues()
		if len(numeratorValues) > length {
			length = len(numeratorValues)
		}
		denominatorAbsent := denominator.AggregatedAbsent()
		if len(denominatorAbsent) > length {
			length = len(denominatorAbsent)
		}
		denominatorValues := denominator.AggregatedValues()
		if len(denominatorValues) > length {
			length = len(denominatorValues)
		}
		values := make([]float64, length)
		for i := 0; i < length; i++ {
			if i >= len(numeratorAbsent) || i >= len(denominatorAbsent) ||
				i >= len(numeratorValues) || i >= len(denominatorValues) {
				values[i] = math.NaN()
				continue
			}
			if numeratorAbsent[i] || denominatorAbsent[i] || denominatorValues[i] == 0 {
				values[i] = math.NaN()
				continue
			}
			values[i] = numeratorValues[i] / denominatorValues[i]
		}

		result := types.MakeMetricData(name, values, step, start)
		results = append(results, result)
	}

	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *divideSeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"divideSeries": {
			Description: "Takes a dividend metric and a divisor metric and draws the division result.\nA constant may *not* be passed. To divide by a constant, use the scale()\nfunction (which is essentially a multiplication operation) and use the inverse\nof the dividend. (Division by 8 = multiplication by 1/8 or 0.125)\n\nExample:\n\n.. code-block:: none\n\n  &target=divideSeries(Series.dividends,Series.divisors)",
			Function:    "divideSeries(dividendSeriesList, divisorSeries)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "divideSeries",
			Params: []types.FunctionParam{
				{
					Name:     "dividendSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "divisorSeries",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
