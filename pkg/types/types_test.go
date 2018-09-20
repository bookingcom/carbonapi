package types

import (
	"testing"
)

func TestMergeResponsesBasic(t *testing.T) {
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{0},
			IsAbsent: []bool{true},
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1},
		IsAbsent: []bool{false},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesMismatched(t *testing.T) {
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{0, 0},
			IsAbsent: []bool{true, true},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{0, 0},
		IsAbsent: []bool{true, true},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func TestMergeResponsesEmpty(t *testing.T) {
	input := []Metric{}
	expected := Metric{}

	doTest(t, input, expected)
}

func TestMergeResponsesPreferFirstPresent(t *testing.T) {
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{0},
			IsAbsent: []bool{true},
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
		},
		Metric{
			Name:     "metric",
			Values:   []float64{2},
			IsAbsent: []bool{false},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1},
		IsAbsent: []bool{false},
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes1(t *testing.T) {
	// lower resolution metric first
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{0, 1},
			IsAbsent: []bool{true, false},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1, 0},
			IsAbsent: []bool{false, true},
			StepTime: 1,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes2(t *testing.T) {
	// lower resolution metric first
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1, 0},
			IsAbsent: []bool{false, true},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{0, 1},
			IsAbsent: []bool{true, false},
			StepTime: 1,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes3(t *testing.T) {
	// (0, 1) metric first
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{0, 1},
			IsAbsent: []bool{true, false},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1, 0},
			IsAbsent: []bool{false, true},
			StepTime: 1,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes4(t *testing.T) {
	// (0, 1) metric first
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{0, 1},
			IsAbsent: []bool{true, false},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1, 0},
			IsAbsent: []bool{false, true},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes5(t *testing.T) {
	// (1, 0) metric first
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{1, 0},
			IsAbsent: []bool{false, true},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{0, 1},
			IsAbsent: []bool{true, false},
			StepTime: 1,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func TestMergeResponsesDifferingStepTimes6(t *testing.T) {
	// (1, 0) metric first
	input := []Metric{
		Metric{
			Name:     "metric",
			Values:   []float64{1, 0},
			IsAbsent: []bool{false, true},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{0, 1},
			IsAbsent: []bool{true, false},
			StepTime: 1,
		},
		Metric{
			Name:     "metric",
			Values:   []float64{1},
			IsAbsent: []bool{false},
			StepTime: 2,
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
		StepTime: 1,
	}

	doTest(t, input, expected)
}

func doTest(t *testing.T, input []Metric, expected Metric) {
	got := mergeMetrics(input)

	if !metricsEqual(got, expected) {
		t.Errorf("Response mismatch\nExp: %+v\nGot: %+v\n", expected, got)
	}
}

func metricsEqual(a, b Metric) bool {
	if a.Name != b.Name ||
		a.StartTime != b.StartTime ||
		a.StopTime != b.StopTime ||
		a.StepTime != b.StepTime ||
		len(a.Values) != len(b.Values) ||
		len(a.IsAbsent) != len(b.IsAbsent) ||
		len(a.Values) != len(a.IsAbsent) {
		return false
	}

	for i := 0; i < len(a.Values); i++ {
		if a.Values[i] != b.Values[i] || a.IsAbsent[i] != b.IsAbsent[i] {
			return false
		}
	}

	return true
}
