package helper

import (
	"github.com/bookingcom/carbonapi/pkg/expr/types"
)

func Normalize(args []*types.MetricData) ([]*types.MetricData, int32, int32, int32, error) {
	return normalize(args, true)
}

// normalize can borrow unchanged point slices when the caller only reads them.
// Normalize retains its detached-copy contract for callers that may mutate them.
func normalize(args []*types.MetricData, copyUnchanged bool) ([]*types.MetricData, int32, int32, int32, error) {
	if len(args) == 0 {
		return []*types.MetricData{}, 0, 0, 0, nil
	}
	if len(args) == 1 {
		return args, args[0].StartTime, args[0].StopTime, args[0].StepTime, nil
	}
	step := LCM(args[0].StepTime, args[1].StepTime)
	start := args[0].StartTime
	end := args[0].StopTime
	for _, i := range args {
		step = LCM(step, i.StepTime)
	}
	result := make([]*types.MetricData, len(args))
	for i, s := range args {
		if copyUnchanged {
			result[i] = s.Consolidate(int(step / s.StepTime))
		} else {
			result[i] = consolidateForRead(s, int(step/s.StepTime))
		}
		if start > s.StartTime {
			start = s.StartTime
		}
		if end < s.StopTime {
			end = s.StopTime
		}
	}
	end -= (end - start) % step
	return result, start, end, step, nil
}

// consolidateForRead borrows point slices only when no resampling is needed.
// The caller must read them without mutation and write any output separately.
func consolidateForRead(s *types.MetricData, valuesPerPoint int) *types.MetricData {
	if valuesPerPoint != 1 {
		return s.Consolidate(valuesPerPoint)
	}
	series := *s
	series.ValuesPerPoint = 1
	return &series
}
