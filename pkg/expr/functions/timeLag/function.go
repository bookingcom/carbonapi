package timeLag

import (
	"context"
	"fmt"

	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type timeLag struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &timeLag{}
	functions := []string{"timeLagSeries", "timeLagSeriesLists"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func MakeTimeLag(consumerMetric, producerMetric *types.MetricData, name string) *types.MetricData {
	pStart := producerMetric.StartTime
	pStep := producerMetric.StepTime
	pLen := (int32)(len(producerMetric.Values))
	cStart := consumerMetric.StartTime
	cStep := consumerMetric.StepTime

	r := *consumerMetric
	r.Name = name

	r.Values = make([]float64, len(consumerMetric.Values))
	r.IsAbsent = make([]bool, len(consumerMetric.Values))

	var pIndex int32 = 0
	for i, v := range consumerMetric.Values {
		// reset producer offset and scan it again if consumer offset decreased
		if i > 0 && consumerMetric.Values[i-1] > v {
			pIndex = 0
		}
		if consumerMetric.IsAbsent[i] || len(producerMetric.Values) == 0 {
			r.IsAbsent[i] = true
			continue
		}

		npIndex := pIndex
		// npIndex: find first index in producer metric that is higher than v
		for (producerMetric.IsAbsent[npIndex] || producerMetric.Values[npIndex] <= v) && (npIndex+1) < pLen {
			npIndex++
			for producerMetric.IsAbsent[npIndex] && (npIndex+1) < pLen {
				npIndex++
			}
			// maintain: pIndex is highest index for which producer metric <= v
			if !producerMetric.IsAbsent[npIndex] && producerMetric.Values[npIndex] <= v {
				pIndex = npIndex
			}
		}
		// we can't compute timeLag for the value that is lower than the smallest data point in producer metric
		if producerMetric.IsAbsent[pIndex] || producerMetric.Values[pIndex] > v {
			r.IsAbsent[i] = true
			continue
		}

		cTime := cStart + (int32)(i)*cStep
		pTime := pStart + pIndex*pStep

		r.Values[i] = (float64)(cTime - pTime)
	}
	return &r
}

func (f *timeLag) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	if len(e.Args()) < 1 {
		return nil, parser.ErrMissingTimeseries
	} else if len(e.Args()) < 2 && e.Target() == "timeLagSeriesLists" {
		return nil, fmt.Errorf("%w: %s must be called with two lists of series", parser.ErrMissingArgument, e.Target())
	}

	firstArg, err := helper.GetSeriesArg(ctx, e.Args()[0], from, until, values, getTargetData)
	if err != nil {
		return nil, err
	}

	var useMetricNames bool

	var consumerMetrics []*types.MetricData
	var producerMetrics []*types.MetricData
	if len(e.Args()) == 2 {
		useMetricNames = true
		consumerMetrics = firstArg
		var err error
		producerMetrics, err = helper.GetSeriesArg(ctx, e.Args()[1], from, until, values, getTargetData)
		if err != nil {
			return nil, err
		}
		switch e.Target() {
		case "timeLagSeries":
			if len(producerMetrics) != 1 {
				return nil, types.ErrWildcardNotAllowed
			}
		case "timeLagSeriesLists":
			if len(producerMetrics) != len(consumerMetrics) {
				return nil, fmt.Errorf("%w: %s", parser.ErrDifferentCountMetrics, e.Target())
			}
		}
	} else if len(firstArg) == 2 && len(e.Args()) == 1 {
		consumerMetrics = append(consumerMetrics, firstArg[0])
		producerMetrics = append(producerMetrics, firstArg[1])
	} else {
		return nil, fmt.Errorf("%w: %s must be called with 2 series or a wildcard that matches exactly 2 series", parser.ErrDifferentCountMetrics, e.Target())
	}

	var results []*types.MetricData
	for i, consumerMetric := range consumerMetrics {
		producerIndex := 0
		if len(producerMetrics) > 1 { // can only be for timeLagSeriesLists
			producerIndex = i
		}
		producerMetric := producerMetrics[producerIndex]
		var name string
		if useMetricNames {
			name = fmt.Sprintf("timeLagSeries(%s,%s)", consumerMetric.Name, producerMetric.Name)
		} else {
			name = fmt.Sprintf("timeLagSeries(%s)", e.RawArgs())
		}

		r := MakeTimeLag(consumerMetric, producerMetric, name)

		results = append(results, r)
	}

	return results, nil

}

func (f *timeLag) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"timeLagSeries": {
			Description: "Computes time lag of two time series representing the time of processing the same data.\nA constant may *not* be passed.\n\nExample:\n\n.. code-block:: none\n\n  &target=timeLagSeries(service_a.consume.max_offset,service_b.produce.max_offset)",
			Function:    "timeLagSeries(consumeMaxOffsetSeries, produceMaxOffsetSeries)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "timeLagSeries",
			Params: []types.FunctionParam{
				{
					Name:     "consumeMaxOffsetSeries",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "produceMaxOffsetSeries",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"timeLagSeriesLists": {
			Description: "Iterates over a two lists and computes timeLagSeries(list1[0},list2[0}), timeLagSeries(list1[1},list2[1}) and so on.\nThe lists need to be the same length",
			Function:    "timeLagSeriesLists(consumeMaxOffsetSeriesList, produceMaxOffsetSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "timeLagSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "consumeMaxOffsetSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "produceMaxOffsetSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
