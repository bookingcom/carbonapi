package types

import (
	"math"
	"sort"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"go.uber.org/zap"
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

func TestMergeManyMetricsWithNormal(t *testing.T) {
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

	logger := zap.NewNop()
	got, _ := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeNormal}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}
}

func TestMergeManyMismatchedMetricsWithCheck(t *testing.T) {
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

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeCheck}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 2 {
		t.Errorf("Expected 2 points , got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 1 {
		t.Errorf("Expected 1 mismatched points , got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 0 {
		t.Errorf("Expected 0 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeManyMismatchedMetricsWithMajority(t *testing.T) {
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

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeMajority}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 2 {
		t.Errorf("Expected 2 points , got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 1 {
		t.Errorf("Expected 1 mismatched point , got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 0 {
		t.Errorf("Expected 0 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeManyMinorityMismatchedMetricsWithCheck(t *testing.T) {
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

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeCheck}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 2 {
		t.Errorf("Expected 2 metric points, got %d", stats.DataPointCount)
	}
	if stats.MismatchCount != 1 {
		t.Errorf("Expected 1 mismatched point, got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 0 {
		t.Errorf("Expected 0 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeManyMinorityMismatchedMetricsWithMajority(t *testing.T) {
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
		Values:   []float64{2, 1},
		IsAbsent: []bool{false, false},
	}

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeMajority}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 2 {
		t.Errorf("Expected 2 metric points, got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 1 {
		t.Errorf("Expected 1 mismatched points, got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 1 {
		t.Errorf("Expected 1 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeManyRiskyAndMismatchedMetricsWithCheck(t *testing.T) {
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

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeCheck}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 3 {
		t.Errorf("Expected 2 metric points, got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 2 {
		t.Errorf("Expected 2 mismatched points, got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 0 {
		t.Errorf("Expected 0 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeManyRiskyAndMismatchedMetricsWithMajority(t *testing.T) {
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
		Values:   []float64{2, 1, 2},
		IsAbsent: []bool{false, false, false},
	}

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeMajority}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 3 {
		t.Errorf("Expected 3 metric points, got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 2 {
		t.Errorf("Expected 2 mismatched points, got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 1 {
		t.Errorf("Expected 1 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeManyRiskyAndMismatchedMetricsWithMajorityApproximateBadFloat(t *testing.T) {
	f1 := 0.1
	f2 := 0.2
	f3 := 0.3
	f3appr := f1 + f2
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{0.25, 0, 2},
				IsAbsent: []bool{false, true, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{f3, 0, 3},
				IsAbsent: []bool{false, true, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{f3appr, 1, 4},
				IsAbsent: []bool{false, false, false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{math.Max(f3, f3appr), 1, 2},
		IsAbsent: []bool{false, false, false},
	}

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{
		RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
		RenderReplicaMismatchApproximateCheck: true,
	}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 3 {
		t.Errorf("Expected 3 metric points, got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 2 {
		t.Errorf("Expected 2 mismatched points, got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 1 {
		t.Errorf("Expected 1 fixed mismatch point , got %d", stats.FixedMismatchCount)
	}
}

func TestMergeMismatchedMetricsWithMajorityApproximateBadFloat(t *testing.T) {
	f1 := 0.1
	f2 := 0.2
	f3 := 0.3
	f3appr := f1 + f2
	input := [][]Metric{
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{f3appr, 1, 4},
				IsAbsent: []bool{false, true, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{f3, 1, 4},
				IsAbsent: []bool{false, true, false},
			},
		},
		[]Metric{
			Metric{
				Name:     "metric",
				Values:   []float64{f3, 1, 4},
				IsAbsent: []bool{false, false, false},
			},
		},
	}

	expected := Metric{
		Name:     "metric",
		Values:   []float64{f3appr, 1, 4},
		IsAbsent: []bool{false, false, false},
	}

	logger := zap.NewNop()
	got, stats := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{
		RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
		RenderReplicaMismatchApproximateCheck: true,
	}, logger)
	if len(got) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(got))
	}

	if !MetricsEqual(got[0], expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got[0])
	}

	if stats.DataPointCount != 3 {
		t.Errorf("Expected 3 metric points, got %d", stats.DataPointCount)
	}

	if stats.MismatchCount != 0 {
		t.Errorf("Expected 0 mismatched points, got %d", stats.MismatchCount)
	}

	if stats.FixedMismatchCount != 0 {
		t.Errorf("Expected 0 fixed mismatch point , got %d", stats.FixedMismatchCount)
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

	logger := zap.NewNop()
	got, _ := MergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeNormal}, logger)
	if len(got) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(got))
	}
}

func TestMergeMetricsNormal(t *testing.T) {
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

func TestMergeMetricsMismatchedNormal(t *testing.T) {
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
	got, _ := mergeMetrics(input, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeNormal})

	if !MetricsEqual(got, expected) {
		t.Errorf("Merge failed\nExp: %+v\nGot: %+v\n", expected, got)
	}
}
