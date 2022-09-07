/*
Package types defines the main Graphite types we use internally.

The definitions correspond to the types of responses to the /render, /info, and
/metrics/find handlers in graphite-web and go-carbon.
*/
package types

// TODO (grzkv): Name of this module makes 0 sense

import (
	"errors"
	"github.com/bookingcom/carbonapi/cfg"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	ErrMetricsNotFound = ErrNotFound("No metrics returned")
	ErrMatchesNotFound = ErrNotFound("No matches found")
	ErrInfoNotFound    = ErrNotFound("No information found")
)

// ErrNotFound signals the HTTP not found error
type ErrNotFound string

// Error makes ErrNotFound compliant with the error interface
func (err ErrNotFound) Error() string {
	return string(err)
}

// TODO (grzkv): Move to separate file

type FindRequest struct {
	Query string
	Trace
}

func NewFindRequest(query string) FindRequest {
	return FindRequest{
		Query: query,
		Trace: NewTrace(),
	}
}

type InfoRequest struct {
	Target string
	Trace
}

func NewInfoRequest(target string) InfoRequest {
	return InfoRequest{
		Target: target,
		Trace:  NewTrace(),
	}
}

type RenderRequest struct {
	Targets []string
	From    int32
	Until   int32
	Trace
}

func NewRenderRequest(targets []string, from int32, until int32) RenderRequest {
	return RenderRequest{
		Targets: targets,
		From:    from,
		Until:   until,
		Trace:   NewTrace(),
	}
}

// TODO: Remove.
type Trace struct {
	callCount     *int64
	inMarshalNS   *int64
	inLimiterNS   *int64
	inHTTPCallNS  *int64
	inReadBodyNS  *int64
	inUnmarshalNS *int64
	OutDuration   *prometheus.HistogramVec
}

func (t Trace) ObserveOutDuration(ti time.Duration, dc string, cluster string) {
	if t.OutDuration != nil { // TODO: check when it is nil
		(*t.OutDuration).With(prometheus.Labels{"cluster": cluster, "dc": dc}).Observe(ti.Seconds())
	}
}

func (t Trace) Report() []int64 {
	n := int64(1)
	c := *t.callCount
	if c > 0 {
		n = c
	}

	return []int64{
		c,
		*t.inMarshalNS / n,
		*t.inLimiterNS / n,
		*t.inHTTPCallNS / n,
		*t.inReadBodyNS / n,
		*t.inUnmarshalNS / n,
	}
}

func (t Trace) IncCall() {
	atomic.AddInt64(t.callCount, 1)
}

func (t Trace) AddMarshal(start time.Time) {
	d := time.Since(start)
	atomic.AddInt64(t.inMarshalNS, int64(d))
}

func (t Trace) AddLimiter(start time.Time) {
	d := time.Since(start)
	atomic.AddInt64(t.inLimiterNS, int64(d))
}

func (t Trace) AddHTTPCall(start time.Time) {
	d := time.Since(start)
	atomic.AddInt64(t.inHTTPCallNS, int64(d))
}

func (t Trace) AddReadBody(start time.Time) {
	d := time.Since(start)
	atomic.AddInt64(t.inReadBodyNS, int64(d))
}

func (t Trace) AddUnmarshal(start time.Time) {
	d := time.Since(start)
	atomic.AddInt64(t.inUnmarshalNS, int64(d))
}

func NewTrace() Trace {
	return Trace{
		callCount:     new(int64),
		inMarshalNS:   new(int64),
		inLimiterNS:   new(int64),
		inHTTPCallNS:  new(int64),
		inReadBodyNS:  new(int64),
		inUnmarshalNS: new(int64),
	}
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

// TODO (grzkv): Move to a separate package

// Metric represents a part of a time series.
type Metric struct {
	Name      string
	StartTime int32
	StopTime  int32
	StepTime  int32
	Values    []float64
	IsAbsent  []bool
}

// MetricRenderStats represents the stats of rendering and merging metrics.
type MetricRenderStats struct {
	DataPointCount     int
	MismatchCount      int
	FixedMismatchCount int
}

// MergeMetrics merges metrics by name.
// It returns merged metrics, number of rendered data points for the returned metrics,
// and number of mismatched data points seen (if mismatchCheck is true).
func MergeMetrics(metrics [][]Metric, replicaMismatchConfig cfg.RenderReplicaMismatchConfig, logger *zap.Logger) ([]Metric, MetricRenderStats) {
	if len(metrics) == 0 {
		return nil, MetricRenderStats{}
	}

	if len(metrics) == 1 {
		pointCount := 0
		ms := metrics[0]
		for _, m := range ms {
			pointCount += len(m.Values)
		}
		return ms, MetricRenderStats{
			DataPointCount: pointCount,
		}
	}

	metricByNames := make(map[string][]Metric)

	for _, ms := range metrics {
		for _, m := range ms {
			metricByNames[m.Name] = append(metricByNames[m.Name], m)
		}
	}

	merged := make([]Metric, 0)
	var metricsStat MetricRenderStats
	type metricReport struct {
		MetricName       string `json:"metric_name"`
		Start            int32  `json:"start"`
		Stop             int32  `json:"stop"`
		Step             int32  `json:"step"`
		MismatchedPoints int    `json:"mismatched_points"`
	}
	var mismatchedMetricReports []metricReport
	for _, ms := range metricByNames {
		m, stats := mergeMetrics(ms, replicaMismatchConfig)
		unfixedMismatches := stats.MismatchCount - stats.FixedMismatchCount
		if unfixedMismatches > 0 &&
			len(mismatchedMetricReports) < replicaMismatchConfig.RenderReplicaMismatchReportLimit {
			mismatchedMetricReports = append(mismatchedMetricReports, metricReport{
				MetricName:       m.Name,
				Start:            m.StartTime,
				Stop:             m.StopTime,
				Step:             m.StepTime,
				MismatchedPoints: stats.MismatchCount - stats.FixedMismatchCount,
			})
		}
		merged = append(merged, m)
		metricsStat.MismatchCount += stats.MismatchCount
		metricsStat.FixedMismatchCount += stats.FixedMismatchCount
		metricsStat.DataPointCount += stats.DataPointCount
	}

	metricsUnfixedMismatchCount := metricsStat.MismatchCount - metricsStat.FixedMismatchCount
	if metricsUnfixedMismatchCount > 0 {
		logger.Info("metric unfixed replica mismatch observed",
			zap.Any("replica_mismatched_metrics", mismatchedMetricReports),
			zap.Int("replica_mismatches_total", metricsStat.MismatchCount),
			zap.Int("replica_fixed_mismatches_total", metricsStat.FixedMismatchCount),
			zap.Int("replica_points_total", metricsStat.DataPointCount),
		)
	}

	return merged, metricsStat
}

type byStepTime []Metric

func (s byStepTime) Len() int { return len(s) }

func (s byStepTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byStepTime) Less(i, j int) bool {
	return s[i].StepTime < s[j].StepTime
}

var (
	epsilon        = math.Nextafter(1.0, 2.0) - 1.0
	floatMinNormal = math.Float64frombits(0x0010000000000000)
)

type floatEqualityFunc func(a, b float64) bool

func AreFloatsApproximatelyEqual(a, b float64) bool {
	if a == b {
		return true
	}

	absA := math.Abs(a)
	absB := math.Abs(b)
	diff := math.Abs(a - b)

	if a == 0 || b == 0 || (absA+absB < floatMinNormal) {
		return diff < (epsilon * floatMinNormal)
	} else {
		div := math.MaxFloat64
		if div > absA+absB {
			div = absA + absB
		}
		return diff/div < epsilon
	}
}

func getPointMajorityValue(values []float64, equalityFunc floatEqualityFunc) (majorityValue float64, isMajority bool, err error) {
	valuesCount := len(values)
	if valuesCount == 0 {
		return 0, false, errors.New("no value for majority voting")
	}

	// Boyer–Moore majority vote algorithm
	i := 0
	var m float64
	for _, v := range values {
		if i == 0 {
			m = v
			i = 1
		} else if (equalityFunc == nil && m == v) || (equalityFunc != nil && equalityFunc(m, v)) {
			i++
		} else {
			i--
		}
		if i > valuesCount/2 {
			return m, true, nil
		}
	}

	// check if m is the majority value
	var majorityCount int
	for _, v := range values {
		if (equalityFunc == nil && m == v) || (equalityFunc != nil && equalityFunc(m, v)) {
			majorityCount++
		}
	}

	return m, majorityCount > valuesCount/2, nil
}

func mergeMetrics(metrics []Metric, replicaMismatchConfig cfg.RenderReplicaMismatchConfig) (metric Metric, stats MetricRenderStats) {
	if len(metrics) == 0 {
		return Metric{}, MetricRenderStats{}
	}

	if len(metrics) == 1 {
		m := metrics[0]
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

	// metrics[0] has the highest resolution of metrics
	metric = metrics[0]
	valuesForPoint := make([]float64, 0, len(metrics))
	isMismatchFindConfig := replicaMatchMode != cfg.ReplicaMatchModeNormal
	for i := range metric.Values {
		pointExists := !metric.IsAbsent[i]
		shouldLookForMismatch := isMismatchFindConfig
		mismatchObserved := false
		valuesForPoint = valuesForPoint[:0]
		if pointExists {
			valuesForPoint = append(valuesForPoint, metric.Values[i])
		}
		for j := 1; j < len(metrics); j++ {
			if pointExists && !shouldLookForMismatch {
				break
			}
			m := metrics[j]

			if m.StepTime != metric.StepTime || len(m.Values) != len(metric.Values) {
				break
			}

			if m.IsAbsent[i] {
				continue
			}

			valuesForPoint = append(valuesForPoint, m.Values[i])

			if !pointExists {
				metric.IsAbsent[i] = m.IsAbsent[i]
				metric.Values[i] = m.Values[i]
				healed++
				pointExists = true
			}

			if !mismatchObserved {
				mismatchObserved = (equalityFunc == nil && metric.Values[i] != m.Values[i]) ||
					(equalityFunc != nil && !equalityFunc(metric.Values[i], m.Values[i]))
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

// Match describes a single glob match
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
