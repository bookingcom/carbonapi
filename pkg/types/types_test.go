package types

import (
	"github.com/bookingcom/carbonapi/cfg"
	"go.uber.org/zap"
	"math"
	"reflect"
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

func TestGetBrokenGlobs(t *testing.T) {
	var tests = []struct {
		name       string
		metric     string
		glob       Matches
		maxBatch   int
		newQueries []string
	}{
		{
			name:     "test1",
			metric:   "a1.*.*.*",
			maxBatch: 5,
			glob: Matches{
				Name: "a1.*.*.*",
				Matches: []Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.*", "a1.b1.c2.*"},
		},
		{
			name:     "test2",
			metric:   "a1.b*.c*.d*",
			maxBatch: 6,
			glob: Matches{
				Name: "a1.b*.c*.d*",
				Matches: []Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d*", "a1.b1.c2.d*"},
		},
		{
			name:     "test3",
			metric:   "a1.b*.c*.d*",
			maxBatch: 2,
			glob: Matches{
				Name: "a1.b*.c*.d*",
				Matches: []Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d1", "a1.b1.c1.d2", "a1.b1.c1.d3", "a1.b1.c2.d1", "a1.b1.c2.d2", "a1.b1.c2.d3"},
		},
		{
			name:     "test4",
			metric:   "a1.*.*.*",
			maxBatch: 6,
			glob: Matches{
				Name: "a1.*.*.*",
				Matches: []Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b2.c2.d2", IsLeaf: true}, {Path: "a1.b3.c3.d3", IsLeaf: true},
					{Path: "a1.b4.c4.d4", IsLeaf: true}, {Path: "a1.b5.c5.d5", IsLeaf: true}, {Path: "a1.b6.c6.d6", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d1", "a1.b2.c2.d2", "a1.b3.c3.d3", "a1.b4.c4.d4", "a5.b5.c5.d5", "a1.b6.c6.d6"},
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			newQueries := GetBrokenGlobs(tst.metric, tst.glob, tst.maxBatch)
			if len(newQueries) != len(tst.newQueries) {
				t.Fatalf("newQueries is different from expected: %+v, %+v", newQueries, tst.newQueries)
			}
		})
	}
}

func TestFindBestStars(t *testing.T) {
	var tests = []struct {
		name         string
		starNSs      []starNS
		maxBatchSize int
		found        []int
		finalProd    int
	}{
		{
			name: "test1",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 1001,
			found:        []int{1, 2},
			finalProd:    1000,
		},
		{
			name: "test2",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 100,
			found:        []int{2},
			finalProd:    50,
		},
		{
			name: "test3",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 5,
			found:        []int{},
			finalProd:    1,
		},
		{
			name: "test4",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 30000,
			found:        []int{0, 2, 3},
			finalProd:    15000,
		},
		{
			name: "test5",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 30001,
			found:        []int{1, 2, 3},
			finalProd:    30000,
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			var found []int
			finalProd := findBestStars(tst.starNSs, 0, 1, tst.maxBatchSize, &found)
			if !reflect.DeepEqual(found, tst.found) {
				t.Fatalf("found is different from expected: %+v, %+v", found, tst.found)
			}
			if finalProd != tst.finalProd {
				t.Fatalf("finalProd is different from expected: %d, %d", finalProd, tst.finalProd)
			}
		})
	}
}
