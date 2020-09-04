package filterSeries

import (
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type filterSeries struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	f := &filterSeries{}
	res := make([]interfaces.FunctionMetadata, 0)
	for _, n := range []string{"filterSeries"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// filterSeries(*seriesLists)
func (f *filterSeries) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}

	callback, err := e.GetStringArg(1)
	if err != nil {
		return nil, err
	}

	var callbackFunc func([]float64, []bool) (float64, bool)
	switch callback {
	case "max":
		callbackFunc = types.AggMax
	case "min":
		callbackFunc = types.AggMin
	case "sum":
		callbackFunc = types.AggSum
	case "average":
		callbackFunc = types.AggMean
	case "last":
		callbackFunc = types.AggLast
	// TODO: this implementation does not support diff, median, multiply, range, stddev
	default:
		return nil, fmt.Errorf("%w: unsupported consolidation function %v", parser.ErrInvalidArgumentValue, callback)
	}

	var operators = map[string]struct{}{
		"=":  {},
		"!=": {},
		">":  {},
		">=": {},
		"<":  {},
		"<=": {},
	}

	operator, err := e.GetStringArg(2)
	if err != nil {
		return nil, err
	}

	_, ok := operators[operator]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported operator %v", parser.ErrInvalidArgumentValue, operator)
	}

	threshold, err := e.GetFloatArg(3)
	if err != nil {
		return nil, fmt.Errorf("deneme: %w", err)
	}

	var results []*types.MetricData
	for _, a := range args {

		val, _ := callbackFunc(a.Values, a.IsAbsent)
		filterOut := true
		switch operator {
		case "=":
			if val == threshold {
				filterOut = false
			}
		case "!=":
			if val != threshold {
				filterOut = false
			}
		case ">":
			if val > threshold {
				filterOut = false
			}
		case ">=":
			if val >= threshold {
				filterOut = false
			}
		case "<":
			if val < threshold {
				filterOut = false
			}
		case "<=":
			if val <= threshold {
				filterOut = false
			}
		}
		if filterOut {
			continue
		}

		results = append(results, a)
	}

	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *filterSeries) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"filterSeries": {
			Description: "Takes one metric or a wildcard seriesList followed by a consolidation function, an operator and a threshold.\nDraws only the metrics which match the filter expression.\n\nExample:\n\n.. code-block:: none\n\n  &target=filterSeries(system.interface.eth*.packetsSent, 'max', '>', 1000)\n\nThis would only display interfaces which has a peak throughput higher than 1000 packets/min.\n\nSupported aggregation functions: ``average``, ``median``, ``sum``, ``min``,\n``max``, ``diff``, ``stddev``, ``range``, ``multiply`` & ``last``.\n\nSupported operators: ``=``, ``!=``, ``>``, ``>=``, ``<`` & ``<=``.",
			Function:    "filterSeries(seriesList, func, operator, threshold)",
			Group:       "Filter Series",
			Module:      "graphite.render.functions",
			Name:        "filterSeries",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "func",
					Required: true,
					Options: []string{
						"average",
						"last",
						"max",
						"min",
						"sum",
					},
					Type: types.AggFunc,
				},
				{
					Name:     "operator",
					Required: true,
					Options: []string{
						"!=",
						"<",
						"<=",
						"=",
						">",
						">=",
					},
					Type: types.String,
				},
				{
					Name:     "threshold",
					Required: true,
					Type:     types.Float,
				},
			},
		},
	}
}
