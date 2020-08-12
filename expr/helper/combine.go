package helper

import (
	"math"

	"github.com/bookingcom/carbonapi/expr/types"
)

type Operator func(l, r float64) float64

// CombineSeries applied operator() on two series. If they do not have the same length, series are consolidated with a Lower Common Multiple step.
func CombineSeries(originalA, originalB *types.MetricData, name string, operator Operator) *types.MetricData {
	// TODO: rewrite consolidation API: https://github.com/bookingcom/carbonapi/issues/278
	a := *originalA
	b := *originalB

	step := LCM(a.StepTime, b.StepTime)

	a.SetValuesPerPoint(int(step / a.StepTime))
	b.SetValuesPerPoint(int(step / b.StepTime))

	start := a.StartTime
	if start > b.StartTime {
		start = b.StartTime
	}
	end := a.StopTime
	if end < b.StopTime {
		end = b.StopTime
	}
	end -= (end - start) % step
	length := int((end - start) / step)

	aAbsent := a.AggregatedAbsent()
	if len(aAbsent) > length {
		length = len(aAbsent)
	}
	aValues := a.AggregatedValues()
	if len(aValues) > length {
		length = len(aValues)
	}
	bAbsent := b.AggregatedAbsent()
	if len(bAbsent) > length {
		length = len(bAbsent)
	}
	bValues := b.AggregatedValues()
	if len(bValues) > length {
		length = len(bValues)
	}
	values := make([]float64, length)
	for i := 0; i < length; i++ {
		if i >= len(aAbsent) || i >= len(bAbsent) ||
			i >= len(aValues) || i >= len(bValues) {
			values[i] = math.NaN()
			continue
		}
		if aAbsent[i] || bAbsent[i] {
			values[i] = math.NaN()
			continue
		}
		values[i] = operator(aValues[i], bValues[i])
	}

	return types.MakeMetricData(name, values, step, start)
}
