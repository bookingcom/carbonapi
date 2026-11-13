package types

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/cfg"
)

// legacyMergeMetrics is mergeMetrics as of bk-master 89d6f213, before the
// freshest-shape base and per-reply skipping. The differential tests below use
// it as the oracle.
func legacyMergeMetrics(metrics []Metric, replicaMismatchConfig cfg.RenderReplicaMismatchConfig) (metric Metric, stats MetricRenderStats) {
	if len(metrics) == 0 {
		return Metric{}, MetricRenderStats{}
	}

	if len(metrics) == 1 {
		m := metrics[0]
		m.materializePoints()
		return m, MetricRenderStats{}
	}

	var equalityFunc floatEqualityFunc
	if replicaMismatchConfig.RenderReplicaMismatchApproximateCheck {
		equalityFunc = AreFloatsApproximatelyEqual
	}

	replicaMatchMode := replicaMismatchConfig.RenderReplicaMatchMode

	var mismatches, fixedMismatches int

	sort.Sort(byStepTime(metrics))
	healed := 0

	clusterMap := make(map[string]bool)
	for _, m := range metrics {
		for _, cl := range m.SourceClusters {
			clusterMap[cl] = true
		}
	}
	clusters := make([]string, 0, len(clusterMap))
	for cl := range clusterMap {
		clusters = append(clusters, cl)
	}
	sort.Strings(clusters)
	// metrics[0] has the highest resolution of metrics
	metric = metrics[0]
	metric.materializePoints()
	var cursors []packedPointCursor
	for _, replica := range metrics[1:] {
		if replica.packed != nil {
			cursors = make([]packedPointCursor, len(metrics))
			for i := range cursors {
				cursors[i].block = -1
			}
			break
		}
	}
	metric.SourceClusters = clusters
	// Schema compatibility is constant across all points. Preserve the old
	// first-incompatible-replica boundary without rechecking it per point.
	replicaLimit := 1
	for replicaLimit < len(metrics) && metrics[replicaLimit].StepTime == metric.StepTime && metrics[replicaLimit].pointCount() == len(metric.Values) {
		replicaLimit++
	}
	valuesForPoint := make([]float64, 0, len(metrics))
	isMismatchFindConfig := replicaMatchMode != cfg.ReplicaMatchModeNormal
	for i := range metric.Values {
		if cursors != nil && i&63 == 0 {
			for j := 1; j < replicaLimit; j++ {
				if p := metrics[j].packed; p != nil {
					cursors[j].advanceToBlock(p, i>>6)
				}
			}
		}
		pointExists := !metric.IsAbsent[i]
		shouldLookForMismatch := isMismatchFindConfig
		mismatchObserved := false
		valuesForPoint = valuesForPoint[:0]
		if pointExists {
			valuesForPoint = append(valuesForPoint, metric.Values[i])
		}
		for j := 1; j < replicaLimit; j++ {
			if pointExists && !shouldLookForMismatch {
				break
			}
			m := metrics[j]

			var value float64
			if m.packed != nil {
				var absent bool
				value, absent = cursors[j].pointAt(m.packed, i)
				if absent {
					continue
				}
			} else {
				if m.IsAbsent[i] {
					continue
				}
				value = m.Values[i]
			}

			valuesForPoint = append(valuesForPoint, value)

			if !pointExists {
				metric.IsAbsent[i] = false
				metric.Values[i] = value
				healed++
				pointExists = true
			}

			if !mismatchObserved {
				mismatchObserved = (equalityFunc == nil && metric.Values[i] != value) ||
					(equalityFunc != nil && !equalityFunc(metric.Values[i], value))
				if mismatchObserved && replicaMatchMode == cfg.ReplicaMatchModeCheck {
					// mismatch exists, enough for check mode
					shouldLookForMismatch = false
				}
			}
		}
		if !mismatchObserved {
			continue
		}

		mismatches++
		if replicaMatchMode == cfg.ReplicaMatchModeMajority {
			majorityValue, isMajority, err := getPointMajorityValue(valuesForPoint, equalityFunc)
			if err == nil && isMajority {
				metric.Values[i] = majorityValue
				fixedMismatches++
			}
		}
	}

	return metric, MetricRenderStats{
		DataPointCount:     len(metric.Values),
		MismatchCount:      mismatches,
		FixedMismatchCount: fixedMismatches,
	}
}

var mergeTestConfigs = func() []cfg.RenderReplicaMismatchConfig {
	var configs []cfg.RenderReplicaMismatchConfig
	for _, mode := range []cfg.ReplicaMatchMode{cfg.ReplicaMatchModeNormal, cfg.ReplicaMatchModeCheck, cfg.ReplicaMatchModeMajority} {
		for _, approx := range []bool{false, true} {
			configs = append(configs, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: mode, RenderReplicaMismatchApproximateCheck: approx})
		}
	}
	return configs
}()

// mergeTestInput returns an independent copy of replies, packing the flagged
// ones, because mergeMetrics sorts its input and heals the base in place.
func mergeTestInput(ms []Metric, packed []bool) []Metric {
	out := make([]Metric, len(ms))
	for i, m := range ms {
		m.Values = append([]float64(nil), m.Values...)
		m.IsAbsent = append([]bool(nil), m.IsAbsent...)
		m.SourceClusters = append([]string(nil), m.SourceClusters...)
		if packed != nil && packed[i] {
			m.PackPoints()
		}
		out[i] = m
	}
	return out
}

func assertSameMerge(t *testing.T, got Metric, gotStats MetricRenderStats, want Metric, wantStats MetricRenderStats) {
	t.Helper()
	if gotStats != wantStats {
		t.Fatalf("stats differ: got %+v, want %+v", gotStats, wantStats)
	}
	if got.packed != nil || want.packed != nil || len(got.Values) != len(want.Values) {
		t.Fatalf("unmaterialized or different length output: got %d points, want %d", len(got.Values), len(want.Values))
	}
	for i := range got.Values {
		if math.Float64bits(got.Values[i]) != math.Float64bits(want.Values[i]) {
			t.Fatalf("value bits differ at %d: got %x, want %x", i, math.Float64bits(got.Values[i]), math.Float64bits(want.Values[i]))
		}
	}
	got.Values, want.Values = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata or absence differs:\ngot  %+v\nwant %+v", got, want)
	}
}

// mergeTestReply builds a 60 s reply starting at start; NaN marks an absent point.
func mergeTestReply(cluster string, start int32, values ...float64) Metric {
	m := Metric{
		Name:           "metric",
		StartTime:      start,
		StopTime:       start + 60*int32(len(values)),
		StepTime:       60,
		Values:         make([]float64, len(values)),
		IsAbsent:       make([]bool, len(values)),
		SourceClusters: []string{cluster},
	}
	for i, v := range values {
		if math.IsNaN(v) {
			m.IsAbsent[i] = true
		} else {
			m.Values[i] = v
		}
	}
	return m
}

func assertMergedPoints(t *testing.T, got Metric, want Metric) {
	t.Helper()
	if got.StartTime != want.StartTime || got.StopTime != want.StopTime || !reflect.DeepEqual(got.IsAbsent, want.IsAbsent) || !reflect.DeepEqual(got.Values, want.Values) {
		t.Fatalf("got %d-%d %v %v, want %d-%d %v %v", got.StartTime, got.StopTime, got.Values, got.IsAbsent, want.StartTime, want.StopTime, want.Values, want.IsAbsent)
	}
}

var nan = math.NaN()

func TestMergeBaseTieBreaks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replies []Metric
		want    int
	}{
		{"one shape keeps the first reply", []Metric{mergeTestReply("A", 600, 1, nan), mergeTestReply("B", 600, 1, 2)}, 0},
		{"latest present point decides the shape", []Metric{mergeTestReply("A", 600, 1, 2, 3), mergeTestReply("B", 600, 1, 2, 3, 4)}, 1},
		{"then most present points", []Metric{mergeTestReply("A", 600, nan, nan, 3, 4, 5), mergeTestReply("B", 660, 2, 3, 4, 5)}, 1},
		{"then reply order", []Metric{mergeTestReply("A", 600, nan, 2, 3, 4, 5), mergeTestReply("B", 660, 2, 3, 4, 5)}, 0},
		{"base is the first reply of the freshest shape", []Metric{mergeTestReply("A", 600, 1, 2, 3, nan, nan), mergeTestReply("B", 600, 1, 2, 3), mergeTestReply("C", 600, 1, 2, 3, 4, 5)}, 0},
		{"all absent keeps the first reply", []Metric{mergeTestReply("A", 600, nan, nan), mergeTestReply("B", 600, nan)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mergeBase(tc.replies); got != tc.want {
				t.Fatalf("mergeBase = %d, want %d", got, tc.want)
			}
			packed := mergeTestInput(tc.replies, nil)
			for i := range packed {
				packed[i].packed = &packedPoints{n: len(packed[i].Values), values: make([]uint64, (len(packed[i].Values)+63)/64), absent: make([]uint64, (len(packed[i].Values)+63)/64)}
				for j, absent := range packed[i].IsAbsent {
					if absent {
						packed[i].packed.absent[j/64] |= uint64(1) << (uint(j) & 63)
					}
				}
				packed[i].Values, packed[i].IsAbsent = nil, nil
			}
			if got := mergeBase(packed); got != tc.want {
				t.Fatalf("packed mergeBase = %d, want %d", got, tc.want)
			}
		})
	}
}

// A store that answers first but still lacks its newest points must not
// leave nulls that another reply can fill.
func TestMergeMetricsStaleFirstReplyHealedFromFresherReply(t *testing.T) {
	for _, tc := range []struct {
		name         string
		replies      []Metric
		want         Metric
		legacyBroken bool
	}{
		{
			name:    "trailing absent points",
			replies: []Metric{mergeTestReply("A", 600, 1, 2, 3, nan, nan), mergeTestReply("B", 600, 1, 2, 3, 4, 5)},
			want:    mergeTestReply("", 600, 1, 2, 3, 4, 5),
		},
		{
			name:         "shorter series",
			replies:      []Metric{mergeTestReply("A", 600, 1, 2, 3), mergeTestReply("B", 600, 1, 2, 3, 4, 5)},
			want:         mergeTestReply("", 600, 1, 2, 3, 4, 5),
			legacyBroken: true,
		},
		{
			name: "shorter, stale and complete replies",
			replies: []Metric{
				mergeTestReply("A", 600, 1, 2, 3),
				mergeTestReply("B", 600, 1, 2, 3, nan, nan),
				mergeTestReply("C", 600, 1, nan, 3, 4, 5),
			},
			want:         mergeTestReply("", 600, 1, 2, 3, 4, 5),
			legacyBroken: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, c := range mergeTestConfigs {
				got, stats := mergeMetrics(mergeTestInput(tc.replies, nil), c)
				assertMergedPoints(t, got, tc.want)
				if stats.MismatchCount != 0 {
					t.Fatalf("%s: %d mismatches, want 0", c.String(), stats.MismatchCount)
				}
				legacy, _ := legacyMergeMetrics(mergeTestInput(tc.replies, nil), c)
				if broken := !reflect.DeepEqual(legacy.Values, tc.want.Values) || !reflect.DeepEqual(legacy.IsAbsent, tc.want.IsAbsent); broken != tc.legacyBroken {
					t.Fatalf("%s: legacy merge broken=%v, want %v (%v %v)", c.String(), broken, tc.legacyBroken, legacy.Values, legacy.IsAbsent)
				}
			}
		})
	}
}

// A reply with another point count in the middle of the list is skipped on
// its own; the replies after it still fill the base's gaps.
func TestMergeMetricsSkipsOnlyMismatchedReply(t *testing.T) {
	replies := []Metric{
		mergeTestReply("A", 600, 1, nan, 3, 4, 5),
		mergeTestReply("B", 600, 1, 2, 3, 4),
		mergeTestReply("C", 600, 1, 2, 3, 4, nan),
	}
	want := mergeTestReply("", 600, 1, 2, 3, 4, 5)
	for _, c := range mergeTestConfigs {
		got, stats := mergeMetrics(mergeTestInput(replies, nil), c)
		assertMergedPoints(t, got, want)
		if stats.MismatchCount != 0 || stats.DataPointCount != 5 {
			t.Fatalf("%s: stats %+v", c.String(), stats)
		}
		if legacy, _ := legacyMergeMetrics(mergeTestInput(replies, nil), c); !legacy.IsAbsent[1] {
			t.Fatalf("%s: legacy merge filled the gap; the test no longer covers the old limit", c.String())
		}
	}
}

func TestMergeMetricsMajorityUnchangedOnDisagreement(t *testing.T) {
	majority := cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeMajority}

	// One shape, first reply stale: the base stays the first reply, so a
	// strict majority wins and an unresolved conflict keeps the base value,
	// exactly as before.
	replies := []Metric{
		mergeTestReply("A", 600, 1, 2, 7, nan),
		mergeTestReply("B", 600, 1, 5, 8, 10),
		mergeTestReply("C", 600, 1, 5, 9, 11),
	}
	got, stats := mergeMetrics(mergeTestInput(replies, nil), majority)
	assertMergedPoints(t, got, mergeTestReply("", 600, 1, 5, 7, 10))
	if stats != (MetricRenderStats{DataPointCount: 4, MismatchCount: 3, FixedMismatchCount: 1}) {
		t.Fatalf("stats %+v", stats)
	}
	for _, c := range mergeTestConfigs {
		got, gs := mergeMetrics(mergeTestInput(replies, nil), c)
		want, ws := legacyMergeMetrics(mergeTestInput(replies, nil), c)
		assertSameMerge(t, got, gs, want, ws)
	}

	// A shorter reply in the middle no longer removes the replies after it
	// from the vote.
	replies = []Metric{
		mergeTestReply("A", 600, 1, 2, 3, nan),
		mergeTestReply("B", 600, 1, 5, 3),
		mergeTestReply("C", 600, 1, 5, 3, 4),
		mergeTestReply("D", 600, 1, 5, 9, 4),
	}
	got, stats = mergeMetrics(mergeTestInput(replies, nil), majority)
	assertMergedPoints(t, got, mergeTestReply("", 600, 1, 5, 3, 4))
	if stats != (MetricRenderStats{DataPointCount: 4, MismatchCount: 2, FixedMismatchCount: 2}) {
		t.Fatalf("stats %+v", stats)
	}
}

var mergeTestValueBits = []uint64{
	0, 1 << 63, // +0 and -0
	0x7ff0000000000000, 0xfff0000000000000, // +Inf and -Inf
	0x7ff8000000000000, 0x7ff8000000000123, 0xfff8000000000001, // NaN payloads
	0x3fd3333333333333, 0x3fd3333333333334, // 0.3 and 0.1+0.2
	1,
}

func mergeTestValue(rng *rand.Rand) float64 {
	if rng.Intn(3) == 0 {
		return math.Float64frombits(mergeTestValueBits[rng.Intn(len(mergeTestValueBits))])
	}
	// A small value range so replies often agree and majorities form.
	return float64(rng.Intn(4))
}

func mergeTestAbsence(rng *rand.Rand, n int) []bool {
	absent := make([]bool, n)
	density := []float64{0, 0.05, 0.5, 0.95, 1}[rng.Intn(5)]
	for i := range absent {
		absent[i] = rng.Float64() < density
	}
	if rng.Intn(3) == 0 { // stale tail, as from a store still replaying
		for i := n - rng.Intn(n+1); i < n; i++ {
			absent[i] = true
		}
	}
	if rng.Intn(4) == 0 { // leading gap
		for i, end := 0, rng.Intn(n+1); i < end; i++ {
			absent[i] = true
		}
	}
	return absent
}

var mergeTestSizes = []int{1, 2, 5, 64, 129, 300}

// Replies of one shape whose present points agree must merge bit-identically
// to the old algorithm, including NaN payloads, absent-slot payloads and the
// mismatch counts, in every match mode.
func TestMergeMetricsAgreeingRepliesMatchLegacy(t *testing.T) {
	rng := rand.New(rand.NewSource(20261113))
	for iter := 0; iter < 300; iter++ {
		n := mergeTestSizes[iter%len(mergeTestSizes)]
		values, absentValues := make([]float64, n), make([]float64, n)
		for i := range values {
			values[i] = mergeTestValue(rng)
			// Decoders write 0 or NaN into absent slots.
			absentValues[i] = math.Float64frombits([]uint64{0, 0x7ff8000000000001}[rng.Intn(2)])
		}
		var replies []Metric
		var packed []bool
		for r := 0; r < 2+rng.Intn(4); r++ {
			m := Metric{Name: "a", StartTime: 600, StopTime: 600 + 60*int32(n), StepTime: 60, Values: make([]float64, n), IsAbsent: mergeTestAbsence(rng, n), SourceClusters: []string{string(rune('A' + r))}}
			for i := range m.Values {
				m.Values[i] = values[i]
				if m.IsAbsent[i] {
					m.Values[i] = absentValues[i]
				}
			}
			replies = append(replies, m)
			packed = append(packed, rng.Intn(2) == 0)
		}
		if rng.Intn(3) == 0 { // a coarser archive, ignored by both
			coarse := Metric{Name: "a", StartTime: 600, StopTime: 600 + 120*int32(n/2+1), StepTime: 120, Values: make([]float64, n/2+1), IsAbsent: mergeTestAbsence(rng, n/2+1), SourceClusters: []string{"Z"}}
			for i := range coarse.Values {
				coarse.Values[i] = mergeTestValue(rng)
			}
			at := rng.Intn(len(replies) + 1)
			replies = append(replies[:at], append([]Metric{coarse}, replies[at:]...)...)
			packed = append(packed[:at], append([]bool{false}, packed[at:]...)...)
		}
		for _, c := range mergeTestConfigs {
			want, ws := legacyMergeMetrics(mergeTestInput(replies, nil), c)
			got, gs := mergeMetrics(mergeTestInput(replies, packed), c)
			assertSameMerge(t, got, gs, want, ws)
		}
	}
}

// mergeTestFreshest is a direct statement of the base rule: among the
// finest-step replies, the latest present point, then the most present
// points, then reply order.
func mergeTestFreshest(sorted []Metric) int {
	key := func(m Metric) (int64, int) {
		last, present := int64(math.MinInt64), 0
		for i, absent := range m.IsAbsent {
			if !absent {
				last = int64(m.StartTime) + int64(i)*int64(m.StepTime)
				present++
			}
		}
		return last, present
	}
	best := 0
	for j := 1; j < len(sorted) && sorted[j].StepTime == sorted[0].StepTime; j++ {
		lj, pj := key(sorted[j])
		lb, pb := key(sorted[best])
		if lj > lb || (lj == lb && pj > pb) {
			best = j
		}
	}
	return best
}

// For any mix of shapes and values, including disagreeing values in majority
// mode, the merge equals the old algorithm run on the replies of the freshest
// shape (in reply order) followed by the others, which it then ignores.
func TestMergeMetricsMixedShapesMatchLegacyOnFreshestShape(t *testing.T) {
	rng := rand.New(rand.NewSource(20261114))
	for iter := 0; iter < 300; iter++ {
		n := mergeTestSizes[iter%len(mergeTestSizes)]
		shared := make([]float64, n+4)
		for i := range shared {
			shared[i] = mergeTestValue(rng)
		}
		var replies []Metric
		var packed []bool
		for r := 0; r < 2+rng.Intn(5); r++ {
			count, start, step := n, int32(600), int32(60)
			switch rng.Intn(5) {
			case 0: // shorter: missing its newest or oldest points
				count = max(1, n-1-rng.Intn(3))
				start += 60 * int32(rng.Intn(2)*(n-count))
			case 1: // longer
				count = n + 1 + rng.Intn(3)
				start -= 60 * int32(rng.Intn(2)*(count-n))
			case 2:
				if rng.Intn(3) == 0 {
					step, count = 120, n/2+1
				}
			}
			m := Metric{Name: "a", StartTime: start, StopTime: start + step*int32(count), StepTime: step, Values: make([]float64, count), IsAbsent: mergeTestAbsence(rng, count), SourceClusters: []string{string(rune('A' + r))}}
			agree := rng.Intn(2) == 0
			for i := range m.Values {
				if agree {
					m.Values[i] = shared[i]
				} else {
					m.Values[i] = mergeTestValue(rng)
				}
			}
			replies = append(replies, m)
			packed = append(packed, rng.Intn(2) == 0)
		}

		sorted := mergeTestInput(replies, nil)
		sort.Stable(byStepTime(sorted))
		freshest := sorted[mergeTestFreshest(sorted)]
		var ordered, rest []Metric
		for _, m := range sorted {
			if m.StepTime == freshest.StepTime && len(m.Values) == len(freshest.Values) {
				ordered = append(ordered, m)
			} else {
				rest = append(rest, m)
			}
		}
		ordered = append(ordered, rest...)

		for _, c := range mergeTestConfigs {
			want, ws := legacyMergeMetrics(mergeTestInput(ordered, nil), c)
			got, gs := mergeMetrics(mergeTestInput(replies, packed), c)
			assertSameMerge(t, got, gs, want, ws)
		}
	}
}
