package types

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"go.uber.org/zap"
)

func copyPackedOracleInput(input [][]Metric, packed bool) [][]Metric {
	out := make([][]Metric, len(input))
	for i, ms := range input {
		out[i] = append([]Metric(nil), ms...)
		for j := range out[i] {
			if ms[j].Values != nil {
				out[i][j].Values = make([]float64, len(ms[j].Values))
				copy(out[i][j].Values, ms[j].Values)
			}
			if ms[j].IsAbsent != nil {
				out[i][j].IsAbsent = make([]bool, len(ms[j].IsAbsent))
				copy(out[i][j].IsAbsent, ms[j].IsAbsent)
			}
			if packed {
				out[i][j].PackPoints()
			}
		}
	}
	return out
}

func assertPackedBits(t testing.TB, got, want []Metric) {
	t.Helper()
	sort.Slice(got, func(i, j int) bool { return got[i].Name < got[j].Name })
	sort.Slice(want, func(i, j int) bool { return want[i].Name < want[j].Name })
	if len(got) != len(want) {
		t.Fatal("metric count differs")
	}
	for i := range got {
		if got[i].packed != nil || len(got[i].Values) != len(want[i].Values) {
			t.Fatal("unmaterialized or different length output")
		}
		for j, v := range got[i].Values {
			if math.Float64bits(v) != math.Float64bits(want[i].Values[j]) {
				t.Fatalf("bits differ at %d/%d", i, j)
			}
		}
		a, b := got[i], want[i]
		a.Values, b.Values = nil, nil
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("metadata/absence differs: %+v / %+v", got[i], want[i])
		}
	}
}

func TestPackedPointsLossless(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 127, 128, 129, 1440, 1441, 2000} {
		m := Metric{Name: "a", StartTime: -300, StopTime: 900, StepTime: 60, Values: make([]float64, n), IsAbsent: make([]bool, n)}
		for i := range m.Values {
			m.IsAbsent[i] = i%3 != 0
			if i%67 == 0 {
				m.Values[i] = math.Float64frombits([]uint64{1 << 63, 0x7ff8000000000123, 0x7ff0000000000000, 0xfff0000000000000, 1}[i%5])
			}
		}
		want := copyPackedOracleInput([][]Metric{{m}}, false)[0]
		m.PackPoints()
		if n >= 128 && m.packed == nil {
			t.Fatal("sparse fixture did not pack")
		}
		m.materializePoints()
		assertPackedBits(t, []Metric{m}, want)
	}
}

func TestPackedReplicaMergeOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20261018))
	for seed := 0; seed < 200; seed++ {
		input := make([][]Metric, 1+seed%4)
		for replica := range input {
			for series := 0; series < 3; series++ {
				n := 128 + seed%79
				if seed%7 == 0 && replica == 2 {
					n += 3
				}
				m := Metric{Name: string(rune('a' + series)), StartTime: -300, StopTime: 900, StepTime: 60, Values: make([]float64, n), IsAbsent: make([]bool, n), SourceClusters: []string{string(rune('A' + replica))}}
				if seed%11 == 0 && replica == 2 {
					m.StepTime = 120
				}
				for i := range m.Values {
					m.IsAbsent[i] = rng.Intn(4) == 0
					if rng.Intn(20) == 0 || (seed%3 == 0 && replica == 1 && series == 2) {
						m.Values[i] = math.Float64frombits(rng.Uint64())
					}
					if i%57 == 0 {
						m.Values[i] = math.Float64frombits(0x7ff8000000000123)
					}
				}
				input[replica] = append(input[replica], m)
			}
		}
		for _, mode := range []cfg.ReplicaMatchMode{cfg.ReplicaMatchModeNormal, cfg.ReplicaMatchModeCheck, cfg.ReplicaMatchModeMajority} {
			for _, approx := range []bool{false, true} {
				c := cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: mode, RenderReplicaMismatchApproximateCheck: approx}
				want, ws := MergeMetrics(copyPackedOracleInput(input, false), c, zap.NewNop())
				pending := copyPackedOracleInput(input, true)
				got, gs := MergeMetrics(pending, c, zap.NewNop())
				if gs != ws {
					t.Fatalf("stats differ: %+v / %+v", gs, ws)
				}
				assertPackedBits(t, got, want)
				// A second merge must not observe mutations to packed input, even
				// though normal replica healing mutates the selected output.
				again, as := MergeMetrics(pending, c, zap.NewNop())
				want, ws = MergeMetrics(copyPackedOracleInput(input, false), c, zap.NewNop())
				if as != ws {
					t.Fatal("reused packed input stats differ")
				}
				assertPackedBits(t, again, want)
			}
		}
	}
}

func TestPackedPointsRetainedSize(t *testing.T) {
	m := Metric{Values: make([]float64, 1440), IsAbsent: make([]bool, 1440)}
	for i := range m.Values {
		m.IsAbsent[i] = true
		if i%31 == 0 {
			m.Values[i] = float64(i + 1)
		}
	}
	m.PackPoints()
	if m.packed == nil {
		t.Fatal("did not pack")
	}
	retained := (len(m.packed.values)+len(m.packed.absent))*8 + 64
	if retained > 1440*9/12 {
		t.Fatalf("retains %d bytes; want at least 12x reduction", retained)
	}
}

func TestPackedCursorSkippedBlocks(t *testing.T) {
	m := Metric{Values: make([]float64, 5001), IsAbsent: make([]bool, 5001)}
	for i := range m.Values {
		if i%67 == 0 {
			m.Values[i] = math.Float64frombits(uint64(i) | 0x7ff8000000000000)
		}
		m.IsAbsent[i] = i%13 == 0
	}
	want := copyPackedOracleInput([][]Metric{{m}}, false)[0][0]
	m.PackPoints()
	if m.packed == nil {
		t.Fatal("did not pack")
	}
	for _, indices := range [][]int{{0, 1, 64, 511, 512, 4095, 4096, 5000}, {67, 67, 4000, 5000}} {
		cursor := packedPointCursor{block: -1}
		for _, i := range indices {
			cursor.advanceToBlock(m.packed, i>>6)
			value, absent := cursor.pointAt(m.packed, i)
			if absent != want.IsAbsent[i] || (!absent && math.Float64bits(value) != math.Float64bits(want.Values[i])) {
				t.Fatalf("cursor mismatch at %d", i)
			}
		}
	}
}
