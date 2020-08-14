package helper

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"

	"github.com/wangjohn/quickselect"
	"gonum.org/v1/gonum/mat"
)

var evaluator interfaces.Evaluator

// Backref is a pre-compiled expression for backref
var Backref = regexp.MustCompile(`\\(\d+)`)

var ErrUnknownFunction = parser.ParseError("unknown function")

// SetEvaluator sets evaluator for all helper functions
func SetEvaluator(e interfaces.Evaluator) {
	evaluator = e
}

// GetSeriesArg returns argument from series.
func GetSeriesArg(arg parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	if !arg.IsName() && !arg.IsFunc() {
		return nil, parser.ErrMissingTimeseries
	}

	a, err := evaluator.EvalExpr(arg, from, until, values)
	if err != nil {
		return nil, err
	}

	return a, nil
}

// RemoveEmptySeriesFromName removes empty series from list of names.
func RemoveEmptySeriesFromName(args []*types.MetricData) string {
	var argNames []string
	for _, arg := range args {
		argNames = append(argNames, arg.Name)
	}

	return strings.Join(argNames, ",")
}

// GetSeriesArgs returns arguments of series
func GetSeriesArgs(e []parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	var args []*types.MetricData

	for _, arg := range e {
		a, err := GetSeriesArg(arg, from, until, values)
		if err != nil && err != parser.ErrSeriesDoesNotExist {
			return nil, err
		}
		args = append(args, a...)
	}

	if len(args) == 0 {
		return nil, parser.ErrSeriesDoesNotExist
	}

	return args, nil
}

// GetSeriesArgsAndRemoveNonExisting will fetch all required arguments, but will also filter out non existing Series
// This is needed to be graphite-web compatible in cases when you pass non-existing Series to, for example, sumSeries
func GetSeriesArgsAndRemoveNonExisting(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	args, err := GetSeriesArgs(e.Args(), from, until, values)
	if err != nil {
		return nil, err
	}

	// We need to rewrite name if there are some missing metrics
	if len(args) < len(e.Args()) {
		e.SetRawArgs(RemoveEmptySeriesFromName(args))
	}

	return args, nil
}

type seriesFunc func(*types.MetricData, *types.MetricData) *types.MetricData

// ForEachSeriesDo do action for each serie in list.
func ForEachSeriesDo(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, function seriesFunc) ([]*types.MetricData, error) {
	arg, err := GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, parser.ErrMissingTimeseries
	}
	var results []*types.MetricData

	for _, a := range arg {
		r := *a
		r.Name = fmt.Sprintf("%s(%s)", e.Target(), a.Name)
		r.Values = make([]float64, len(a.Values))
		r.IsAbsent = make([]bool, len(a.Values))
		results = append(results, function(a, &r))
	}
	return results, nil
}

// AggregateFunc type that defined aggregate function
type AggregateFunc func([]float64) float64

// AggregateSeries aggregates series
func AggregateSeries(e parser.Expr, args []*types.MetricData, function AggregateFunc) ([]*types.MetricData, error) {

	seriesList, start, end, step, err := Normalize(args)
	if err != nil {
		return nil, err
	}
	length := int((end - start) / step)
	name := fmt.Sprintf("%s(%s)", e.Target(), e.RawArgs())
	result := make([]float64, length)

	for i := 0; i < length; i++ {
		var values []float64
		for _, s := range seriesList {
			if i < len(s.IsAbsent) && !s.IsAbsent[i] {
				values = append(values, s.Values[i])
			}
		}
		result[i] = math.NaN()
		if len(values) > 0 {
			result[i] = function(values)
		}
	}
	ret := types.MakeMetricData(name, result, step, start)
	return []*types.MetricData{ret}, nil
}

// SummarizeValues summarizes values
func SummarizeValues(f string, values []float64) float64 {
	rv := 0.0

	if len(values) == 0 {
		return math.NaN()
	}

	switch f {
	case "sum", "total":
		for _, av := range values {
			rv += av
		}

	case "avg":
		for _, av := range values {
			rv += av
		}
		rv /= float64(len(values))
	case "max":
		rv = math.Inf(-1)
		for _, av := range values {
			if av > rv {
				rv = av
			}
		}
	case "min":
		rv = math.Inf(1)
		for _, av := range values {
			if av < rv {
				rv = av
			}
		}
	case "last":
		if len(values) > 0 {
			rv = values[len(values)-1]
		}

	default:
		f = strings.Split(f, "p")[1]
		percent, err := strconv.ParseFloat(f, 64)
		if err == nil {
			rv = Percentile(values, percent, true)
		}
	}

	return rv
}

// ExtractMetric extracts metric out of function list
func ExtractMetric(s string) string {

	// search for a metric name in 's'
	// metric name is defined to be a Series of name characters terminated by a ',' or ')'
	// work sample: bla(bla{bl,a}b[la,b]la) => bla{bl,a}b[la

	var (
		start, braces, i, w int
		r                   rune
	)

FOR:
	for braces, i, w = 0, 0, 0; i < len(s); i += w {

		w = 1
		if parser.IsNameChar(s[i]) {
			continue
		}

		switch s[i] {
		case '{':
			braces++
		case '}':
			if braces == 0 {
				break FOR
			}
			braces--
		case ',':
			if braces == 0 {
				break FOR
			}
		case ')':
			break FOR
		default:
			r, w = utf8.DecodeRuneInString(s[i:])
			if unicode.In(r, parser.RangeTables...) {
				continue
			}
			start = i + 1
		}

	}

	return s[start:i]
}

// Contains check if slice 'a' contains value 'i'
func Contains(a []int, i int) bool {
	for _, aa := range a {
		if aa == i {
			return true
		}
	}
	return false
}

// Percentile returns percent-th percentile. Can interpolate if needed
func Percentile(data []float64, percent float64, interpolate bool) float64 {
	if len(data) == 0 || percent < 0 || percent > 100 {
		return math.NaN()
	}
	if len(data) == 1 {
		return data[0]
	}

	k := (float64(len(data)-1) * percent) / 100
	length := int(math.Ceil(k)) + 1
	quickselect.Float64QuickSelect(data, length)
	top, secondTop := math.Inf(-1), math.Inf(-1)
	for _, val := range data[0:length] {
		if val > top {
			secondTop = top
			top = val
		} else if val > secondTop {
			secondTop = val
		}
	}
	remainder := k - float64(int(k))
	if remainder == 0 || !interpolate {
		return top
	}
	return (top * remainder) + (secondTop * (1 - remainder))
}

// MaxValue returns maximum from the list
func MaxValue(f64s []float64, absent []bool) float64 {
	m := math.Inf(-1)
	for i, v := range f64s {
		if absent[i] {
			continue
		}
		if v > m {
			m = v
		}
	}
	return m
}

// MinValue returns minimal from the list
func MinValue(f64s []float64, absent []bool) float64 {
	m := math.Inf(1)
	for i, v := range f64s {
		if absent[i] {
			continue
		}
		if v < m {
			m = v
		}
	}
	return m
}

// AvgValue returns average of list of values
func AvgValue(f64s []float64, absent []bool) float64 {
	var t float64
	var elts int
	for i, v := range f64s {
		if absent[i] {
			continue
		}
		elts++
		t += v
	}
	return t / float64(elts)
}

// CurrentValue returns last non-absent value (if any), otherwise returns NaN
func CurrentValue(f64s []float64, absent []bool) float64 {
	for i := len(f64s) - 1; i >= 0; i-- {
		if !absent[i] {
			return f64s[i]
		}
	}

	return math.NaN()
}

// VarianceValue gets variances of list of values
func VarianceValue(f64s []float64, absent []bool) float64 {
	var squareSum float64
	var elts int

	mean := AvgValue(f64s, absent)
	if math.IsNaN(mean) {
		return mean
	}

	for i, v := range f64s {
		if absent[i] {
			continue
		}
		elts++
		squareSum += (mean - v) * (mean - v)
	}
	return squareSum / float64(elts)
}

// Vandermonde creates a Vandermonde matrix
func Vandermonde(absent []bool, deg int) *mat.Dense {
	e := []float64{}
	for i := range absent {
		if absent[i] {
			continue
		}
		v := 1
		for j := 0; j < deg+1; j++ {
			e = append(e, float64(v))
			v *= i
		}
	}
	return mat.NewDense(len(e)/(deg+1), deg+1, e)
}

// Poly computes polynom with specified coefficients
func Poly(x float64, coeffs ...float64) float64 {
	y := coeffs[0]
	v := 1.0
	for _, c := range coeffs[1:] {
		v *= x
		y += c * v
	}
	return y
}

// GCD computes the Greatest Common Divisor of two numbers
func GCD(a, b int32) int32 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// LCM computes the Least Commong Multiple of two numbers
func LCM(a, b int32) int32 {
	return a * (b / GCD(a, b))
}
