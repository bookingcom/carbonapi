package types

import (
	"sort"
	"testing"
)

func TestMergeInfos(t *testing.T) {
	infos := [][]Info{
		[]Info{Info{}},
		[]Info{Info{}},
	}

	got := MergeInfos(infos)
	if len(got) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(got))
	}
}

func TestMergeMatches(t *testing.T) {
	matches := [][]Match{
		[]Match{Match{}},
		[]Match{Match{}},
	}

	got := MergeMatches(matches)
	if len(got) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(got))
	}
}

func TestSortMetrics(t *testing.T) {
	metrics := []Metric{
		Metric{
			StepTime: 2,
		},
		Metric{
			StepTime: 1,
		},
	}

	sort.Sort(byStepTime(metrics))

	if metrics[0].StepTime != 1 {
		t.Error("Didn't sort metrics")
	}
}

func TestMergeMetricsBasic(t *testing.T) {
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{0},
				IsAbsent: []bool{true},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{1},
				IsAbsent: []bool{false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1},
		IsAbsent: []bool{false},
	}

	got := MergeMetrics(input)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !metricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}
}

func TestMergeMetricsDifferent(t *testing.T) {
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric1",
				Values:   []float64{0},
				IsAbsent: []bool{true},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric2",
				Values:   []float64{1},
				IsAbsent: []bool{false},
			},
		},
	}

	got := MergeMetrics(input)
	if len(got) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(got))
	}
}

func TestmergeMetricsBasic(t *testing.T) {
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

func TestmergeMetricsMismatched(t *testing.T) {
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

func TestmergeMetricsEmpty(t *testing.T) {
	input := []Metric{}
	expected := Metric{}

	doTest(t, input, expected)
}

func TestmergeMetricsPreferFirstPresent(t *testing.T) {
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

func TestmergeMetricsDifferingStepTimes1(t *testing.T) {
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

func TestmergeMetricsDifferingStepTimes2(t *testing.T) {
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

func TestmergeMetricsDifferingStepTimes3(t *testing.T) {
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

func TestmergeMetricsDifferingStepTimes4(t *testing.T) {
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

func TestmergeMetricsDifferingStepTimes5(t *testing.T) {
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

func TestmergeMetricsDifferingStepTimes6(t *testing.T) {
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
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got)
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
