/*
Package types defines the main Graphite types we use internally.

The definitions correspond to the types of responses to the /render, /info, and
/metrics/find handlers in graphite-web and go-carbon.
*/
package types

// Metric represents a part of a time series.
type Metric struct {
	Name      string
	StartTime int32
	StopTime  int32
	StepTime  int32
	Values    []float64
	IsAbsent  []bool
}

func MergeMetrics(metrics [][]Metric) []Metric {
	return nil
}

// Info contains metadata about a metric in Graphite.
type Info struct {
	Name              string
	AggregationMethod string
	MaxRetention      int32
	XFilesFactor      float32
	Retentions        []Retention
}

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

func MergeMatches(matches [][]Match) []Match {
	merged := make([]Match, 0, len(matches))
	for _, match := range matches {
		merged = append(merged, match...)
	}

	return merged
}
