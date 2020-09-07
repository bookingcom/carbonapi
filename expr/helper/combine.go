package helper

import (
	"github.com/bookingcom/carbonapi/expr/types"
)

type Operator func(l, r float64) (float64, bool)

// CombineSeries applied operator() on two series. If they do not have the same length, series are consolidated with a Lower Common Multiple step.
func CombineSeries(originalA, originalB *types.MetricData, name string, operator Operator) *types.MetricData {

	step := LCM(originalA.StepTime, originalB.StepTime)

	a := originalA.Consolidate(int(step / originalA.StepTime))
	b := originalB.Consolidate(int(step / originalB.StepTime))

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

	if len(a.IsAbsent) > length {
		length = len(a.IsAbsent)
	}
	if len(a.Values) > length {
		length = len(a.Values)
	}
	if len(b.IsAbsent) > length {
		length = len(b.IsAbsent)
	}
	if len(b.Values) > length {
		length = len(b.Values)
	}
	values := make([]float64, length)
	isAbsent := make([]bool, length)
	for i := 0; i < length; i++ {
		if i >= len(a.IsAbsent) || i >= len(b.IsAbsent) ||
			i >= len(a.Values) || i >= len(b.Values) {
			isAbsent[i] = true
			continue
		}
		if a.IsAbsent[i] || b.IsAbsent[i] {
			isAbsent[i] = true
			continue
		}
		values[i], isAbsent[i] = operator(a.Values[i], b.Values[i])
	}

	return types.New(name, values, isAbsent, step, start)
}
