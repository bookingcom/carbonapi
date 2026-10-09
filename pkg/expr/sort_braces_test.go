package expr

import (
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/expr/types"
)

// legacySortByBraces is the implementation before the literal fast path.
func legacySortByBraces(metrics []*types.MetricData, part int, pattern string) {
	bStart := strings.IndexRune(pattern, '{')
	bEnd := strings.IndexRune(pattern, '}')
	if bStart == -1 || bEnd <= bStart {
		return
	}

	parts := make([]string, len(metrics))
	for i, metric := range metrics {
		parts[i] = getPart(metric, part)
	}
	src := make([]*types.MetricData, len(metrics))
	used := make([]bool, len(metrics))
	copy(src, metrics)
	j := 0

	alternatives := strings.Split(pattern[bStart+1:bEnd], ",")
	for _, alternative := range alternatives {
		glob := pattern[:bStart] + alternative + pattern[bEnd+1:]
		for i := 0; i < len(src); i++ {
			if used[i] {
				continue
			}
			if match, _ := filepath.Match(glob, parts[i]); match {
				metrics[j] = src[i]
				j = j + 1
				used[i] = true
			}
		}
	}
	for i, metric := range src {
		if !used[i] {
			metrics[j] = metric
			j = j + 1
		}
	}
}

func TestSortByBracesMatchesLegacy(t *testing.T) {
	rnd := rand.New(rand.NewSource(3))
	words := []string{"ok", "ko", "envoy_ok", "envoy_ko", "a", "b", "ab", "a*", "x?y", "[1]", `a\b`, "", "a]", "sum"}
	pick := func() string { return words[rnd.Intn(len(words))] }

	for iter := 0; iter < 3000; iter++ {
		n := rnd.Intn(30)
		names := make([]string, n)
		for i := range names {
			names[i] = "root." + pick() + pick() + "." + pick() + "_" + strconv.Itoa(i)
		}

		alts := make([]string, 1+rnd.Intn(4))
		for i := range alts {
			alts[i] = pick()
			switch rnd.Intn(5) {
			case 0:
				alts[i] += "*"
			case 1:
				alts[i] = "?" + alts[i]
			case 2:
				alts[i] = "[ae]" + alts[i]
			}
		}
		prefix, suffix := "", ""
		if rnd.Intn(3) == 0 {
			prefix = pick()
		}
		if rnd.Intn(3) == 0 {
			suffix = "*"
		}
		pattern := prefix + "{" + strings.Join(alts, ",") + "}" + suffix

		mk := func() []*types.MetricData {
			ms := make([]*types.MetricData, n)
			for i := range ms {
				ms[i] = types.New(names[i], nil, nil, 60, 0)
			}
			return ms
		}
		want, got := mk(), mk()
		legacySortByBraces(want, 1, pattern)
		sortByBraces(got, 1, pattern)
		for i := range want {
			if want[i].Name != got[i].Name {
				t.Fatalf("pattern %q names %q: position %d got %q want %q", pattern, names, i, got[i].Name, want[i].Name)
			}
		}
	}
}
