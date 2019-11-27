package timeLag

import (
	"errors"
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
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
	functions := []string{"timeLagSeries"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// timeLag(consumeMaxOffsetSeries, produceMaxOffsetSeries)
func (f *timeLag) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	if len(e.Args()) < 1 {
		return nil, parser.ErrMissingTimeseries
	}

	firstArg, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}

	var useMetricNames bool

	var consumerMetrics []*types.MetricData
	var producerMetric *types.MetricData
	if len(e.Args()) == 2 {
		useMetricNames = true
		consumerMetrics = firstArg
		producerMetrics, err := helper.GetSeriesArg(e.Args()[1], from, until, values)
		if err != nil {
			return nil, err
		}
		if len(producerMetrics) != 1 {
			return nil, types.ErrWildcardNotAllowed
		}

		producerMetric = producerMetrics[0]
	} else if len(firstArg) == 2 && len(e.Args()) == 1 {
		consumerMetrics = append(consumerMetrics, firstArg[0])
		producerMetric = firstArg[1]
	} else {
		return nil, errors.New("must be called with 2 series or a wildcard that matches exactly 2 series")
	}

    pStart := producerMetric.StartTime
    pStep := producerMetric.StepTime
    pLen := (int32)(len(producerMetric.Values))
	var results []*types.MetricData
	for _, consumerMetric := range consumerMetrics {
        cStart := consumerMetric.StartTime
        cStep := consumerMetric.StepTime

        r := *consumerMetric
		if useMetricNames {
			r.Name = fmt.Sprintf("timeLagSeries(%s,%s)", consumerMetric.Name, producerMetric.Name)
		} else {
			r.Name = fmt.Sprintf("timeLagSeries(%s)", e.RawArgs())
		}
		r.Values = make([]float64, len(consumerMetric.Values))
		r.IsAbsent = make([]bool, len(consumerMetric.Values))

        var pIndex int32 = 0;
        for producerMetric.IsAbsent[pIndex] && pIndex+1 < pLen {
            pIndex++
        }

		for i, v := range consumerMetric.Values {

			if consumerMetric.IsAbsent[i] || len(producerMetric.Values) == 0 {
				r.IsAbsent[i] = true
				continue
			}

            npIndex := pIndex
            // npIndex: find first index in producer metric that is higher than v
            for (producerMetric.IsAbsent[npIndex] || producerMetric.Values[npIndex] <= v) && (npIndex+1) < pLen {
                npIndex++
                for producerMetric.IsAbsent[npIndex] && (npIndex + 1) < pLen {
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

            r.Values[i] = (float64)(cTime - pTime);
		}
		results = append(results, &r)
	}

	return results, nil

}

func (f *timeLag) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"timeLagSeries": {
			Description: "Computes time lag of two time series representing the time of processing the same data.\nA constant may *not* be passed.\n\nExample:\n\n.. code-block:: none\n\n  &target=timeLagSeries(service_a.consume.max_offset,service_b.produce.max_offset)",
			Function:    "timeLagSeries(consumeMaxOffsetSeries, produceMaxOffsetSeries)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
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
	}
}
