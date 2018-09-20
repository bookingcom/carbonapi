/*
Package types defines the main Graphite types we use internally.

The definitions correspond to the types of responses to the /render, /info, and
/metrics/find handlers in graphite-web and go-carbon.
*/
package types

import (
	"sort"
)

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
	names := make(map[string][]Metric)

	for _, ms := range metrics {
		for _, m := range ms {
			names[m.Name] = append(names[m.Name], m)
		}
	}

	merged := make([]Metric, 0)
	for _, ms := range names {
		sort.Sort(byStepTime(ms))
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

	// We assume metrics[0] has the highest resolution of metrics
	metric := metrics[0]
	for i := range metric.Values {
		if !metric.IsAbsent[i] {
			continue
		}

		// found a missing value, look for a replacement
		for j := 1; j < len(metrics); j++ {
			m := metrics[j]

			if len(m.Values) != len(metric.Values) {
				break
			}

			// found one
			if !m.IsAbsent[i] {
				metric.IsAbsent[i] = m.IsAbsent[i]
				metric.Values[i] = m.Values[i]
				break
			}
		}
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

// Match describes a glob match from a Graphite store.
type Match struct {
	Path   string
	IsLeaf bool
}

// MergeMatches merges Match structures.
func MergeMatches(matches [][]Match) []Match {
	merged := make([]Match, 0, len(matches))
	for _, match := range matches {
		merged = append(merged, match...)
	}

	return merged
}
