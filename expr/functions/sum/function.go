package sum

import (
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type sum struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &sum{}
	functions := []string{"sum", "sumSeries"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// SumAggregation
func SumAggregation(values []float64) (float64, bool) {
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum, false
}

// sumSeries(*seriesLists)
func (f *sum) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	// TODO(dgryski): make sure the arrays are all the same 'size'
	args, err := helper.GetSeriesArgsAndRemoveNonExisting(e, from, until, values)
	if err != nil {
		return nil, err
	}

	e.SetTarget("sumSeries")
	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())
	return helper.AggregateSeries(name, args, false, SumAggregation)
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *sum) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"sum": {
			Description: "Short form: sum()\n\nThis will add metrics together and return the sum at each datapoint. (See\nintegral for a sum over time)\n\nExample:\n\n.. code-block:: none\n\n  &target=sum(company.server.application*.requestsHandled)\n\nThis would show the sum of all requests handled per minute (provided\nrequestsHandled are collected once a minute).   If metrics with different\nretention rates are combined, the coarsest metric is graphed, and the sum\nof the other metrics is averaged for the metrics with finer retention rates.\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``sum``.",
			Function:    "sum(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "sum",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"sumSeries": {
			Description: "Short form: sum()\n\nThis will add metrics together and return the sum at each datapoint. (See\nintegral for a sum over time)\n\nExample:\n\n.. code-block:: none\n\n  &target=sum(company.server.application*.requestsHandled)\n\nThis would show the sum of all requests handled per minute (provided\nrequestsHandled are collected once a minute).   If metrics with different\nretention rates are combined, the coarsest metric is graphed, and the sum\nof the other metrics is averaged for the metrics with finer retention rates.\n\nThis is an alias for :py:func:`aggregate <aggregate>` with aggregation ``sum``.",
			Function:    "sumSeries(*seriesLists)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "sumSeries",
			Params: []types.FunctionParam{
				{
					Multiple: true,
					Name:     "seriesLists",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
