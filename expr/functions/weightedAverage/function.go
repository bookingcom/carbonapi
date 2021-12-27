package weightedAverage

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bookingcom/carbonapi/expr/functions/sum"
	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type weightedAverage struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &weightedAverage{}
	functions := []string{"weightedAverage"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// exclude(seriesList, pattern)
func (f *weightedAverage) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	avgArg, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	weightArg, err := helper.GetSeriesArg(ctx, e.Args()[1], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	nodes, err := e.GetIntArgs(2)
	if err != nil {
		return nil, err
	}

	avgGrouped, avgNodeKeys, err := helper.GroupByNodes(nodes, avgArg)
	if err != nil {
		return nil, err
	}
	weightGrouped, weightNodeKeys, err := helper.GroupByNodes(nodes, weightArg)
	if err != nil {
		return nil, err
	}

	var keys []string
	for avgKey := range avgNodeKeys {
		if _, found := weightNodeKeys[avgKey]; found {
			keys = append(keys, avgKey)
		}
	}

	var productMetrics []*types.MetricData
	for _, key := range keys {
		if len(avgGrouped[key]) == 0 || len(weightGrouped[key]) == 0 {
			continue
		}
		weight := weightGrouped[key][len(weightGrouped[key])-1]
		avg := avgGrouped[key][len(avgGrouped[key])-1]
		productMetricName := fmt.Sprintf("product(%s, %s)", weight.Name, avg.Name)
		productMetric := helper.CombineSeries(avg, weight, productMetricName, productOperator)
		productMetrics = append(productMetrics, productMetric)
	}
	if len(productMetrics) == 0 {
		return []*types.MetricData{}, nil
	}

	sumOfProductMetricName := "sumOfProducts"
	sumOfProductsMetrics, err := helper.AggregateSeries(sumOfProductMetricName, productMetrics, false, false, sum.SumAggregation)
	if err != nil {
		return nil, err
	}
	sumOfWeightsMetricName := "sumOfWeights"
	sumOfWeightsMetrics, err := helper.AggregateSeries(sumOfWeightsMetricName, weightArg, false, false, sum.SumAggregation)
	if err != nil {
		return nil, err
	}
	weightedAverageMetricName := getWeightedAverageMetricName(avgArg, weightArg, nodes)
	weightedAverageMetric := helper.CombineSeries(sumOfProductsMetrics[0], sumOfWeightsMetrics[0], weightedAverageMetricName, divOperator)

	results := make([]*types.MetricData, 1)
	results[0] = weightedAverageMetric

	return results, nil
}

func productOperator(a, b float64) (float64, bool) {
	return a * b, false
}

func divOperator(a, b float64) (float64, bool) {
	if b == 0 {
		return 0, true
	}
	return (a / b), false
}

func getWeightedAverageMetricName(avgSeriesList, weightSeriesList []*types.MetricData, nodes []int) string {
	avgsName := getAggregatedMetricName(avgSeriesList)
	weightsName := getAggregatedMetricName(weightSeriesList)
	nodesStrList := make([]string, len(nodes))
	for i, node := range nodes {
		nodesStrList[i] = strconv.Itoa(node)
	}
	nodesName := strings.Join(nodesStrList, ",")
	return fmt.Sprintf("weightedAverage(%s, %s, %s)", avgsName, weightsName, nodesName)
}

func getAggregatedMetricName(seriesList []*types.MetricData) string {
	seriesSet := make(map[string]bool)
	for _, series := range seriesList {
		seriesSet[series.Name] = true
	}
	seriesUniqueList := make([]string, 0, len(seriesSet))
	for seriesName := range seriesSet {
		seriesUniqueList = append(seriesUniqueList, seriesName)
	}
	sort.Strings(seriesUniqueList)
	return strings.Join(seriesUniqueList, ",")
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *weightedAverage) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"weightedAverage": {
			Description: "Takes a series of average values and a series of weights and produces a weighted average for all values. The corresponding values should share one or more zero-indexed nodes and/or tags.",
			Function:    "weightedAverage(seriesListAvg, seriesListWeight, *nodes)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "weightedAverage",
			Params: []types.FunctionParam{
				{
					Name:     "seriesListAvg",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "seriesListWeight",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "nodes",
					Multiple: true,
					Type:     types.NodeOrTag,
				},
			},
		},
	}
}
