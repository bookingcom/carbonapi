package asPercent

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bookingcom/carbonapi/pkg/expr/functions/sum"
	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type asPercent struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &asPercent{}
	for _, n := range []string{"asPercent"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// asPercentWithNodes handles the case where we have *nodes argument.
// There are two subcases: total=Node or total is a seriesList with wildcards
// We support only the second subcase
func asPercentWithNodes(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	seriesList, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	if len(seriesList) == 0 {
		return seriesList, nil
	}
	total, err := helper.GetSeriesArg(ctx, e.Args()[1], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	nodes, err := e.GetIntArgs(2)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]bool)
	metaSeries, err := groupByNodes(nodes, keys, seriesList)
	if err != nil {
		return nil, err
	}
	tmpTotalSeries, err := groupByNodes(nodes, keys, total)
	if err != nil {
		return nil, err
	}

	totalSeries := make(map[string]*types.MetricData)
	for key := range tmpTotalSeries {
		if len(tmpTotalSeries[key]) == 1 {
			totalSeries[key] = tmpTotalSeries[key][0]
		} else {
			name := fmt.Sprintf("sumSeries(%s)", e.Args()[1].Target())
			aggregated, err := helper.AggregateSeries(name, seriesList, false, false, sum.SumAggregation)
			if err != nil {
				return nil, err
			}
			totalSeries[key] = aggregated[0]
		}
	}

	// Sort keys so we get results in the same order
	var results []*types.MetricData
	keysArray := make([]string, 0, len(keys))
	for k := range keys {
		keysArray = append(keysArray, k)
	}
	sort.Strings(keysArray)
	for _, key := range keysArray {
		numerators, ok := metaSeries[key]
		if !ok {
			series2 := totalSeries[key]
			name := fmt.Sprintf("asPercent(MISSING,%s)", series2.Name)
			values := make([]float64, len(series2.Values))
			isAbsent := make([]bool, len(series2.Values))
			for i := 0; i < len(values); i++ {
				values[i] = 0
				isAbsent[i] = true
			}
			result := types.New(name, values, isAbsent, series2.StepTime, series2.StartTime)
			results = append(results, result)
			continue
		}
		for _, series1 := range numerators {
			if _, found := totalSeries[key]; !found {
				name := fmt.Sprintf("asPercent(%s,MISSING)", series1.Name)
				values := make([]float64, len(series1.Values))
				isAbsent := make([]bool, len(series1.Values))
				for i := 0; i < len(values); i++ {
					values[i] = 0
					isAbsent[i] = true
				}
				result := types.New(name, values, isAbsent, series1.StepTime, series1.StartTime)
				results = append(results, result)
				continue
			}
			series2 := totalSeries[key]
			name := fmt.Sprintf("asPercent(%s,%s)", series1.Name, series2.Name)
			result := helper.CombineSeries(series1, series2, name, percent)
			results = append(results, result)
		}
	}
	return results, nil
}

func asPercentPairs(numerators, denominators []*types.MetricData) ([]*types.MetricData, error) {
	// Sort lists by name so that they match up.
	sort.Sort(helper.ByName(numerators))
	sort.Sort(helper.ByName(denominators))

	var results []*types.MetricData
	for idx, numerator := range numerators {
		denominator := denominators[idx]
		name := fmt.Sprintf("asPercent(%s,%s)", numerator.Name, denominator.Name)
		result := helper.CombineSeries(numerator, denominator, name, percent)
		results = append(results, result)
	}
	return results, nil
}

// asPercentWithoutNodes handles the cases where we don't have the *nodes argumnt.
// There are four subcases: total = None, total is constant, total is one series and total is a seriesList with the same length as the first argument
func asPercentWithoutNodes(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	seriesList, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}
	if len(seriesList) == 0 {
		return seriesList, nil
	}

	if err != nil {
		return nil, err
	}
	n := len(seriesList[0].Values)

	var total *types.MetricData
	var totalText string
	switch {
	case len(e.Args()) == 1:
		name := fmt.Sprintf("sumSeries(%s)", e.Args()[0].Target())
		aggregated, err := helper.AggregateSeries(name, seriesList, false, false, sum.SumAggregation)
		if err != nil {
			return nil, err
		}
		total = aggregated[0]
		totalText = name
	case len(e.Args()) == 2 && e.Args()[1].IsConst():
		value, err := e.GetFloatArg(1)
		if err != nil {
			return nil, err
		}
		isAbsent := make([]bool, n)
		values := make([]float64, n)
		for i := 0; i < n; i++ {
			values[i] = value
			isAbsent[i] = false
		}
		totalText = fmt.Sprintf("%0.2f", value)
		total = types.New(totalText, values, isAbsent, seriesList[0].StepTime, seriesList[0].StartTime)
	case len(e.Args()) == 2 && (e.Args()[1].IsName() || e.Args()[1].IsFunc()):
		totalArg, err := helper.GetSeriesArg(ctx, e.Args()[1], from, until, values, getTargetData)
		if err != nil {
			return nil, err
		}
		switch len(totalArg) {
		case 1:
			total = totalArg[0]
			if e.Args()[1].IsName() {
				totalText = e.Args()[1].Target()
			} else {
				totalText = fmt.Sprintf("%s(%s)", e.Args()[1].Target(), e.Args()[1].RawArgs())
			}
		case len(seriesList):
			return asPercentPairs(seriesList, totalArg)
		default:
			return nil, types.ErrWildcardNotAllowed
		}
	default:
		return nil, parser.ParseError("total must be either a constant or a series")
	}

	var results []*types.MetricData
	for _, numerator := range seriesList {
		name := fmt.Sprintf("asPercent(%s)", numerator.Name)
		if totalText != "" {
			name = fmt.Sprintf("asPercent(%s,%s)", numerator.Name, totalText)

		}
		result := helper.CombineSeries(numerator, total, name, percent)
		results = append(results, result)
	}
	return results, nil
}

// asPercent(seriesList, total=None, *nodes)
func (f *asPercent) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {

	switch len(e.Args()) {
	case 0:
		// we need at least one argument
		return nil, parser.ErrMissingArgument
	case 1, 2:
		return asPercentWithoutNodes(ctx, e, from, until, values, getTargetData)
	default:
		return asPercentWithNodes(ctx, e, from, until, values, getTargetData)
	}
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *asPercent) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"asPercent": {
			Description: "Calculates a percentage of the total of a wildcard series. If `total` is specified,\neach series will be calculated as a percentage of that total. If `total` is not specified,\nthe sum of all points in the wildcard series will be used instead.\n\nA list of nodes can optionally be provided, if so they will be used to match series with their\ncorresponding totals following the same logic as :py:func:`groupByNodes <groupByNodes>`.\n\nWhen passing `nodes` the `total` parameter may be a series list or `None`.  If it is `None` then\nfor each series in `seriesList` the percentage of the sum of series in that group will be returned.\n\nWhen not passing `nodes`, the `total` parameter may be a single series, reference the same number\nof series as `seriesList` or be a numeric value.\n\nExample:\n\n.. code-block:: none\n\n  # Server01 connections failed and succeeded as a percentage of Server01 connections attempted\n  &target=asPercent(Server01.connections.{failed,succeeded}, Server01.connections.attempted)\n\n  # For each server, its connections failed as a percentage of its connections attempted\n  &target=asPercent(Server*.connections.failed, Server*.connections.attempted)\n\n  # For each server, its connections failed and succeeded as a percentage of its connections attempted\n  &target=asPercent(Server*.connections.{failed,succeeded}, Server*.connections.attempted, 0)\n\n  # apache01.threads.busy as a percentage of 1500\n  &target=asPercent(apache01.threads.busy,1500)\n\n  # Server01 cpu stats as a percentage of its total\n  &target=asPercent(Server01.cpu.*.jiffies)\n\n  # cpu stats for each server as a percentage of its total\n  &target=asPercent(Server*.cpu.*.jiffies, None, 0)\n\nWhen using `nodes`, any series or totals that can't be matched will create output series with\nnames like ``asPercent(someSeries,MISSING)`` or ``asPercent(MISSING,someTotalSeries)`` and all\nvalues set to None. If desired these series can be filtered out by piping the result through\n``|exclude(\"MISSING\")`` as shown below:\n\n.. code-block:: none\n\n  &target=asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)\n\n  # will produce 3 output series:\n  # asPercent(Server1.memory.used,Server1.memory.total) [values will be as expected}\n  # asPercent(Server2.memory.used,MISSING) [all values will be None}\n  # asPercent(MISSING,Server3.memory.total) [all values will be None}\n\n  &target=asPercent(Server{1,2}.memory.used,Server{1,3}.memory.total,0)|exclude(\"MISSING\")\n\n  # will produce 1 output series:\n  # asPercent(Server1.memory.used,Server1.memory.total) [values will be as expected}\n\nEach node may be an integer referencing a node in the series name or a string identifying a tag.\n\n.. note::\n\n  When `total` is a seriesList, specifying `nodes` to match series with the corresponding total\n  series will increase reliability.",
			Function:    "asPercent(seriesList, total=None, *nodes)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "asPercent",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name: "total",
					Type: types.SeriesList,
				},
				{
					Multiple: true,
					Name:     "nodes",
					Type:     types.NodeOrTag,
				},
			},
		},
	}
}

func percent(a, b float64) (float64, bool) {
	if b == 0 {
		return 0, true
	}
	return (a / b) * 100, false
}

func aggKey(series *types.MetricData, fields []int) (string, error) {
	metric := helper.ExtractMetric(series.Name)
	nodes := strings.Split(metric, ".")
	nodeKey := make([]string, 0, len(fields))
	for _, f := range fields {
		if f < 0 || f >= len(nodes) {
			return "", fmt.Errorf("%w: %d", parser.ErrInvalidArgumentValue, f)
		}
		nodeKey = append(nodeKey, nodes[f])
	}
	node := strings.Join(nodeKey, ".")
	return node, nil
}

func groupByNodes(nodes []int, keys map[string]bool, seriesList []*types.MetricData) (map[string][]*types.MetricData, error) {
	metaSeries := make(map[string][]*types.MetricData)

	for _, series := range seriesList {
		key, err := aggKey(series, nodes)
		if err != nil {
			return nil, err
		}
		if _, found := metaSeries[key]; !found {
			metaSeries[key] = make([]*types.MetricData, 1)
			metaSeries[key][0] = series
		} else {
			metaSeries[key] = append(metaSeries[key], series)
		}
		keys[key] = true
	}
	return metaSeries, nil
}
