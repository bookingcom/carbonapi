package types

import (
	"bytes"
	"math"
	"strconv"
	"time"

	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/types"

	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"

	pickle "github.com/lomik/og-rek"
)

var (
	// ErrWildcardNotAllowed is an eval error returned when a wildcard/glob argument is found where a single series is required.
	ErrWildcardNotAllowed = parser.ParseError("found wildcard where series expected")
	// ErrTooManyArguments is an eval error returned when too many arguments are provided.
	ErrTooManyArguments = parser.ParseError("too many arguments")
)

// MetricData contains necessary data to represent parsed metric (ready to be send out or drawn)
type MetricData struct {
	types.Metric

	GraphOptions

	ValuesPerPoint    int
	AggregateFunction func([]float64, []bool) (float64, bool)
}

// MakeMetricData creates new metrics data with given metric timeseries. values have math.NaN() for absent
func MakeMetricData(name string, values []float64, step, start int32) *MetricData {

	absent := make([]bool, len(values))

	for i, v := range values {
		if math.IsNaN(v) {
			values[i] = 0
			absent[i] = true
		}
	}
	stop := start + int32(len(values))*step

	return &MetricData{Metric: types.Metric{
		Name:      name,
		Values:    values,
		StartTime: start,
		StepTime:  step,
		StopTime:  stop,
		IsAbsent:  absent,
	}}
}

// MarshalCSV marshals metric data to CSV
func MarshalCSV(results []*MetricData, location *time.Location) []byte {

	var b []byte

	for _, r := range results {

		step := r.StepTime
		t := r.StartTime
		for i, v := range r.Values {
			b = append(b, '"')
			b = append(b, r.Name...)
			b = append(b, '"')
			b = append(b, ',')

			tmp := time.Unix(int64(t), 0)
			if location != nil {
				tmp = tmp.In(location)
			}

			b = append(b, tmp.Format("2006-01-02 15:04:05")...)
			b = append(b, ',')
			if !r.IsAbsent[i] {
				b = strconv.AppendFloat(b, v, 'f', -1, 64)
			}
			b = append(b, '\n')
			t += step
		}
	}
	return b
}

// ConsolidateJSON consolidates values to maxDataPoints size
func ConsolidateJSON(maxDataPoints int, results []*MetricData) (consolidated []*MetricData) {
	var startTime int32 = -1
	var endTime int32 = -1

	for _, r := range results {
		t := r.StartTime
		if startTime == -1 || startTime > t {
			startTime = t
		}
		t = r.StopTime
		if endTime == -1 || endTime < t {
			endTime = t
		}
	}

	timeRange := endTime - startTime

	if timeRange <= 0 {
		return
	}

	ret := make([]*MetricData, len(results))
	for i, r := range results {
		numberOfDataPoints := math.Floor(float64(timeRange) / float64(r.StepTime))
		if numberOfDataPoints > float64(maxDataPoints) {
			valuesPerPoint := math.Ceil(numberOfDataPoints / float64(maxDataPoints))
			ret[i] = r.Consolidate(int(valuesPerPoint))
		} else {
			ret[i] = r
		}
	}
	return ret
}

// MarshalJSON marshals metric data to JSON
func MarshalJSON(results []*MetricData) []byte {
	var b []byte
	b = append(b, '[')

	var topComma bool
	for _, r := range results {
		if r == nil {
			continue
		}

		if topComma {
			b = append(b, ',')
		}
		topComma = true

		b = append(b, `{"target":`...)
		b = strconv.AppendQuoteToASCII(b, r.Name)
		b = append(b, `,"datapoints":[`...)

		var innerComma bool
		t := r.StartTime
		absent := r.IsAbsent
		for i, v := range r.Values {
			if innerComma {
				b = append(b, ',')
			}
			innerComma = true

			b = append(b, '[')

			if absent[i] || math.IsInf(v, 0) || math.IsNaN(v) {
				b = append(b, "null"...)
			} else {
				b = strconv.AppendFloat(b, v, 'f', -1, 64)
			}

			b = append(b, ',')

			b = strconv.AppendInt(b, int64(t), 10)

			b = append(b, ']')

			t += r.StepTime
		}

		b = append(b, `]}`...)
	}

	b = append(b, ']')

	return b
}

// MarshalPickle marshals metric data to pickle format
func MarshalPickle(results []*MetricData) []byte {

	var p []map[string]interface{}

	for _, r := range results {
		values := make([]interface{}, len(r.Values))
		for i, v := range r.Values {
			if r.IsAbsent[i] {
				values[i] = pickle.None{}
			} else {
				values[i] = v
			}

		}
		p = append(p, map[string]interface{}{
			"name":   r.Name,
			"start":  r.StartTime,
			"end":    r.StopTime,
			"step":   r.StepTime,
			"values": values,
		})
	}

	var buf bytes.Buffer

	penc := pickle.NewEncoder(&buf)
	penc.Encode(p)

	return buf.Bytes()
}

// MarshalProtobuf marshals metric data to protobuf
func MarshalProtobuf(results []*MetricData) ([]byte, error) {
	metrics := make([]types.Metric, 0)
	for _, metric := range results {
		metrics = append(metrics, metric.Metric)
	}

	return carbonapi_v2.RenderEncoder(metrics)
}

// MarshalRaw marshals metric data to graphite's internal format, called 'raw'
func MarshalRaw(results []*MetricData) []byte {

	var b []byte

	for _, r := range results {

		b = append(b, r.Name...)

		b = append(b, ',')
		b = strconv.AppendInt(b, int64(r.StartTime), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(r.StopTime), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(r.StepTime), 10)
		b = append(b, '|')

		var comma bool
		for i, v := range r.Values {
			if comma {
				b = append(b, ',')
			}
			comma = true
			if r.IsAbsent[i] {
				b = append(b, "None"...)
			} else {
				b = strconv.AppendFloat(b, v, 'f', -1, 64)
			}
		}

		b = append(b, '\n')
	}
	return b
}

// Consolidate returns a consolidated copy of this MetricData.
func (r *MetricData) Consolidate(valuesPerPoint int) *MetricData {
	ret := *r
	if valuesPerPoint == 1 || valuesPerPoint == 0 {
		ret.ValuesPerPoint = 1
		ret.Values = make([]float64, len(r.Values))
		ret.IsAbsent = make([]bool, len(r.IsAbsent))
		copy(ret.Values, r.Values)
		copy(ret.IsAbsent, r.IsAbsent)
		return &ret
	}

	ret.ValuesPerPoint = valuesPerPoint
	ret.StepTime = r.StepTime * int32(valuesPerPoint)
	if ret.AggregateFunction == nil {
		ret.AggregateFunction = AggMean
	}

	n := len(r.Values)/valuesPerPoint + 1
	aggV := make([]float64, 0, n)
	aggA := make([]bool, 0, n)

	v := r.Values
	absent := r.IsAbsent

	for len(v) >= valuesPerPoint {
		val, abs := ret.AggregateFunction(v[:valuesPerPoint], absent[:valuesPerPoint])
		if math.IsNaN(val) {
			val = 0
		}
		aggV = append(aggV, val)
		aggA = append(aggA, abs)
		v = v[valuesPerPoint:]
		absent = absent[valuesPerPoint:]
	}

	if len(v) > 0 {
		val, abs := ret.AggregateFunction(v, absent)
		if math.IsNaN(val) {
			val = 0
		}
		aggV = append(aggV, val)
		aggA = append(aggA, abs)
	}

	ret.Values = aggV
	ret.IsAbsent = aggA

	return &ret
}

// AggMean computes mean (sum(v)/len(v), excluding NaN points) of values
func AggMean(v []float64, absent []bool) (float64, bool) {
	var sum float64
	var n int
	for i, vv := range v {
		if !math.IsNaN(vv) && !absent[i] {
			sum += vv
			n++
		}
	}
	return sum / float64(n), n == 0
}

// AggMax computes max of values
func AggMax(v []float64, absent []bool) (float64, bool) {
	var m = math.Inf(-1)
	var abs = true
	for i, vv := range v {
		if !absent[i] && !math.IsNaN(vv) {
			abs = false
			if m < vv {
				m = vv
			}
		}
	}
	return m, abs
}

// AggMin computes min of values
func AggMin(v []float64, absent []bool) (float64, bool) {
	var m = math.Inf(1)
	var abs = true
	for i, vv := range v {
		if !absent[i] && !math.IsNaN(vv) {
			abs = false
			if m > vv {
				m = vv
			}
		}
	}
	return m, abs
}

// AggSum computes sum of values
func AggSum(v []float64, absent []bool) (float64, bool) {
	var sum float64
	var abs = true
	for i, vv := range v {
		if !math.IsNaN(vv) && !absent[i] {
			sum += vv
			abs = false
		}
	}
	return sum, abs
}

// AggFirst returns first point
func AggFirst(v []float64, absent []bool) (float64, bool) {
	var m = math.Inf(-1)
	var abs = true
	if len(v) > 0 {
		return v[0], absent[0]
	}
	return m, abs
}

// AggLast returns last point
func AggLast(v []float64, absent []bool) (float64, bool) {
	var m = math.Inf(-1)
	var abs = true
	if len(v) > 0 {
		return v[len(v)-1], absent[len(v)-1]
	}
	return m, abs
}
