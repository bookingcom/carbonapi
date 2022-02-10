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
	matches := []Matches{
		Matches{
			Matches: []Match{Match{
				Path: "foo",
			}},
		},
		Matches{
			Matches: []Match{Match{
				Path: "bar",
			}},
		},
	}

	got := MergeMatches(matches)
	if len(got.Matches) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(got.Matches))
	}
}

func TestMergeMatchesDeduplicate(t *testing.T) {
	matches := []Matches{
		Matches{
			Matches: []Match{Match{
				Path: "foo",
			}},
		},
		Matches{
			Matches: []Match{Match{
				Path: "foo",
			}},
		},
	}

	got := MergeMatches(matches)
	if len(got.Matches) != 1 {
		t.Errorf("Expected 1 element, got %d", len(got.Matches))
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

func TestMergeManyMetricsBasic(t *testing.T) {
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

	got, _, _ := MergeMetrics(input, false, 0)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}
}

func TestMergeManyMismatchedMetrics(t *testing.T) {
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{2, 1},
				IsAbsent: []bool{false, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{2, 1},
		IsAbsent: []bool{false, false},
	}

	got, ps, ins := MergeMetrics(input, true, 10)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if ps != 2 {
		t.Errorf("Expected 2 points , got %d", ps)
	}

	if ins != 1 {
		t.Errorf("Expected 1 Mismatched points , got %d", ins)
	}
}

func TestMergeRiskyMetrics(t *testing.T) {
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{2, 1},
				IsAbsent: []bool{false, false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{2, 1},
		IsAbsent: []bool{false, false},
	}

	got, ps, ins := MergeMetrics(input, true, 10)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if ps != 2 {
		t.Errorf("Expected 2 metric points, got %d", ps)
	}
	if ins != 0 {
		t.Errorf("Expected 0 Mismatched metric points, got %d", ins)
	}
}

func TestMergeManyMinorityMismatchedMetrics(t *testing.T) {
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{1, 1},
				IsAbsent: []bool{false, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{2, 1},
				IsAbsent: []bool{false, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{2, 1},
				IsAbsent: []bool{false, false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1},
		IsAbsent: []bool{false, false},
	}

	got, ps, ins := MergeMetrics(input, true, 10)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if ps != 2 {
		t.Errorf("Expected 2 metric points, got %d", ps)
	}
	if ins != 1 {
		t.Errorf("Expected 1 Mismatched metric points, got %d", ins)
	}
}

func TestMergeManyRiskyAndMismatchedMetrics(t *testing.T) {
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{1, 0, 2},
				IsAbsent: []bool{false, true, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{2, 0, 3},
				IsAbsent: []bool{false, true, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{2, 1, 4},
				IsAbsent: []bool{false, false, false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{1, 1, 2},
		IsAbsent: []bool{false, false, false},
	}

	got, ps, ins := MergeMetrics(input, true, 10)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if ps != 3 {
		t.Errorf("Expected 2 metric points, got %d", ps)
	}
	if ins != 2 {
		t.Errorf("Expected 2 Mismatched metric points, got %d", ins)
	}
}

func TestMergeManyMetricsDifferent(t *testing.T) {
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

	got, _, _ := MergeMetrics(input, false, 0)
	if len(got) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(got))
	}
}

func TestMergeMetricsBasic(t *testing.T) {
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

func TestMergeMetricsMismatched(t *testing.T) {
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

func TestMergeMetricsEmpty(t *testing.T) {
	input := []Metric{}
	expected := Metric{}

	doTest(t, input, expected)
}

func TestMergeMetricsPreferFirstPresent(t *testing.T) {
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

func TestMergeMetricsDifferingStepTimes1(t *testing.T) {
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

func TestMergeMetricsDifferingStepTimes2(t *testing.T) {
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

func TestMergeMetricsDifferingStepTimes3(t *testing.T) {
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

func TestMergeMetricsDifferingStepTimes4(t *testing.T) {
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

func TestMergeMetricsDifferingStepTimes5(t *testing.T) {
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

func TestMergeMetricsDifferingStepTimes6(t *testing.T) {
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
	got, _ := mergeMetrics(input, false)

	if !MetricsEqual(got, expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got)
	}
}
