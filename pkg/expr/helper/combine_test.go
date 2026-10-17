package helper

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/expr/types"
)

func TestCombineSeriesBorrowedInputsMatchCopies(t *testing.T) {
	for _, steps := range [][]int32{{1, 1}, {2, 3}, {6, 3}, {3, 6}} {
		t.Run(fmt.Sprint(steps), func(t *testing.T) {
			a := types.MakeMetricData("a", []float64{1, math.NaN(), 3, 4, 5, 6, 7}, steps[0], 120)
			b := types.MakeMetricData("b", []float64{2, 4, math.NaN(), 8}, steps[1], 126)
			before := types.MarshalJSON([]*types.MetricData{a, b})
			step := LCM(a.StepTime, b.StepTime)
			ca := a.Consolidate(int(step / a.StepTime))
			cb := b.Consolidate(int(step / b.StepTime))
			for _, op := range []Operator{
				func(a, b float64) (float64, bool) { return a + b, false },
				func(a, b float64) (float64, bool) { return a / b, b == 0 },
			} {
				got := CombineSeries(a, b, "result", op)
				want := CombineSeries(ca, cb, "result", op)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("result differs from detached/resampled operands: got %+v, want %+v", got, want)
				}
				got.Values[0] = -100
				got.IsAbsent[0] = true
			}
			if string(types.MarshalJSON([]*types.MetricData{a, b})) != string(before) {
				t.Fatal("combination or output mutation modified shared operands")
			}
		})
	}
}
