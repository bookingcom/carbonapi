/*
Package types defines the main Graphite types we use internally.

The definitions correspond to the types of responses to the /render, /info, and
/metrics/find handlers in graphite-web and go-carbon.
*/
package types

import (
	"sort"

	"go.uber.org/zap"
)

var (
	corruptionThreshold = 1.0
	corruptionLogger    = zap.New(nil)
)

func SetCorruptionWatcher(threshold float64, logger *zap.Logger) {
	corruptionThreshold = threshold
	corruptionLogger = logger
}

type FindRequest struct {
	Query string
}

type InfoRequest struct {
	Target string
}

type RenderRequest struct {
	Targets []string
	From    int32
	Until   int32
}

/* NOTE(gmagnusson):
If it turns out that converting generated protobuf structs to and from this
type is too expensive, it could change to be an interface

	type Metric interface {
		Values() []float64
		// etc
	}

with an implementation

	type metric struct {
		v2 *carbonapi_v2.Metric
		// other types
	}

	func (m metric) Values() []float64 {
		if m.v2 != nil {
			return m.v2.Values
		}
		// etc
	}

The interface would probably need to have a Merge(other) method as well.
*/

// Metric represents a part of a time series.
type Metric struct {
	Name      string
	StartTime int32
	StopTime  int32
	StepTime  int32
	Values    []float64
	IsAbsent  []bool
}

// MergeMetrics merges metrics by name.
func MergeMetrics(metrics [][]Metric) []Metric {
	if len(metrics) == 0 {
		return nil
	}

	if len(metrics) == 1 {
		return metrics[0]
	}

	names := make(map[string][]Metric)

	for _, ms := range metrics {
		for _, m := range ms {
			names[m.Name] = append(names[m.Name], m)
		}
	}

	merged := make([]Metric, 0)
	for _, ms := range names {
		merged = append(merged, mergeMetrics(ms))
	}

	return merged
}

type byStepTime []Metric

func (s byStepTime) Len() int { return len(s) }

func (s byStepTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byStepTime) Less(i, j int) bool {
	return s[i].StepTime < s[j].StepTime
}

func mergeMetrics(metrics []Metric) Metric {
	if len(metrics) == 0 {
		return Metric{}
	}

	if len(metrics) == 1 {
		return metrics[0]
	}

	sort.Sort(byStepTime(metrics))
	healed := 0

	// metrics[0] has the highest resolution of metrics
	metric := metrics[0]
	for i := range metric.Values {
		if !metric.IsAbsent[i] {
			continue
		}

		// found a missing value, look for a replacement
		for j := 1; j < len(metrics); j++ {
			m := metrics[j]

			if m.StepTime != metric.StepTime || len(m.Values) != len(metric.Values) {
				break
			}

			// found one
			if !m.IsAbsent[i] {
				metric.IsAbsent[i] = m.IsAbsent[i]
				metric.Values[i] = m.Values[i]
				healed++
				break
			}
		}
	}

	if c := float64(healed) / float64(len(metric.Values)); c > corruptionThreshold {
		corruptionLogger.Warn("metric corruption",
			zap.String("metric", metric.Name),
			zap.Float64("corruption", c),
			zap.Float64("threshold", corruptionThreshold),
		)
	}

	return metric
}

// Info contains metadata about a metric in Graphite.
type Info struct {
	Host              string
	Name              string
	AggregationMethod string
	MaxRetention      int32
	XFilesFactor      float32
	Retentions        []Retention
}

// MergeInfos merges Info structures.
func MergeInfos(infos [][]Info) []Info {
	if len(infos) == 0 {
		return nil
	}

	if len(infos) == 1 {
		return infos[0]
	}

	merged := make([]Info, 0, len(infos))
	for _, info := range infos {
		merged = append(merged, info...)
	}

	return merged
}

// Retention is the Graphite retention schema for a metric archive.
type Retention struct {
	SecondsPerPoint int32
	NumberOfPoints  int32
}

// Matches describes a glob match from a Graphite store.
type Matches struct {
	Name    string
	Matches []Match
}

type Match struct {
	Path   string
	IsLeaf bool
}

// MergeMatches merges Match structures.
func MergeMatches(matches []Matches) Matches {
	if len(matches) == 0 {
		return Matches{}
	}

	if len(matches) == 1 {
		return matches[0]
	}

	merged := Matches{}

	set := make(map[Match]struct{})
	for _, match := range matches {
		if merged.Name == "" {
			merged.Name = match.Name
		}

		for _, m := range match.Matches {
			set[m] = struct{}{}
		}
	}

	merged.Matches = make([]Match, 0, len(set))
	for match := range set {
		merged.Matches = append(merged.Matches, match)
	}

	return merged
}

func MetricsEqual(a, b Metric) bool {
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
