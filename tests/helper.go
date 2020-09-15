package tests

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/metadata"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
)

type FuncEvaluator struct {
	eval func(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error)
}

func (evaluator *FuncEvaluator) EvalExpr(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	if e.IsName() {
		return values[parser.MetricRequest{Metric: e.Target(), From: from, Until: until}], nil
	} else if e.IsConst() {
		p := types.MetricData{
			Metric: dataTypes.Metric{
				Name:   e.Target(),
				Values: []float64{e.FloatValue()},
			}}
		return []*types.MetricData{&p}, nil
	}
	// evaluate the function

	// all functions have arguments -- check we do too
	if len(e.Args()) == 0 {
		return nil, parser.ErrMissingArgument
	}

	return evaluator.eval(ctx, e, from, until, values, getTargetData)
}

func EvaluatorFromFunc(function interfaces.Function) interfaces.Evaluator {
	e := &FuncEvaluator{
		eval: function.Do,
	}

	return e
}

func EvaluatorFromFuncWithMetadata(metadata map[string]interfaces.Function) interfaces.Evaluator {
	e := &FuncEvaluator{
		eval: func(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
			if f, ok := metadata[e.Target()]; ok {
				return f.Do(ctx, e, from, until, values, getTargetData)
			}
			return nil, fmt.Errorf("unknown function: %v", e.Target())
		},
	}
	return e
}

func DeepClone(original map[parser.MetricRequest][]*types.MetricData) map[parser.MetricRequest][]*types.MetricData {
	clone := map[parser.MetricRequest][]*types.MetricData{}
	for key, originalMetrics := range original {
		copiedMetrics := []*types.MetricData{}
		for _, originalMetric := range originalMetrics {
			copiedMetric := types.MetricData{
				Metric: dataTypes.Metric{
					Name:      originalMetric.Name,
					StartTime: originalMetric.StartTime,
					StopTime:  originalMetric.StopTime,
					StepTime:  originalMetric.StepTime,
					Values:    make([]float64, len(originalMetric.Values)),
					IsAbsent:  make([]bool, len(originalMetric.IsAbsent)),
				},
			}

			copy(copiedMetric.Values, originalMetric.Values)
			copy(copiedMetric.IsAbsent, originalMetric.IsAbsent)
			copiedMetrics = append(copiedMetrics, &copiedMetric)
		}

		clone[key] = copiedMetrics
	}

	return clone
}

func DeepEqual(t *testing.T, target string, original, modified map[parser.MetricRequest][]*types.MetricData) {
	for key := range original {
		if len(original[key]) == len(modified[key]) {
			for i := range original[key] {
				if !reflect.DeepEqual(original[key][i], modified[key][i]) {
					t.Errorf(
						"%s: source data was modified key %v index %v original:\n%v\n modified:\n%v",
						target,
						key,
						i,
						original[key][i],
						modified[key][i],
					)
				}
			}
		} else {
			t.Errorf(
				"%s: source data was modified key %v original length %d, new length %d",
				target,
				key,
				len(original[key]),
				len(modified[key]),
			)
		}
	}
}

const eps = 0.0000000001

func NearlyEqual(a []float64, absent []bool, b []float64) bool {

	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		// "same"
		if absent[i] && math.IsNaN(b[i]) {
			continue
		}
		if absent[i] || math.IsNaN(b[i]) {
			// unexpected NaN
			return false
		}
		// "close enough"
		if math.Abs(v-b[i]) > eps {
			return false
		}
	}

	return true
}

func NearlyEqualMetrics(a, b *types.MetricData) bool {

	if len(a.IsAbsent) != len(b.IsAbsent) {
		return false
	}

	for i := range a.IsAbsent {
		if a.IsAbsent[i] != b.IsAbsent[i] {
			return false
		}
		// "close enough"
		if math.Abs(a.Values[i]-b.Values[i]) > eps {
			return false
		}
	}

	return true
}

type SummarizeEvalTestItem struct {
	Target string
	M      map[parser.MetricRequest][]*types.MetricData
	W      []float64
	Name   string
	Step   int32
	Start  int32
	Stop   int32
}

func InitTestSummarize() (int32, int32, int32) {
	t0, err := time.Parse(time.UnixDate, "Wed Sep 10 10:32:00 CEST 2014")
	if err != nil {
		panic(err)
	}

	tenThirtyTwo := int32(t0.Unix())

	t0, err = time.Parse(time.UnixDate, "Wed Sep 10 10:59:00 CEST 2014")
	if err != nil {
		panic(err)
	}

	tenFiftyNine := int32(t0.Unix())

	t0, err = time.Parse(time.UnixDate, "Wed Sep 10 10:30:00 CEST 2014")
	if err != nil {
		panic(err)
	}

	tenThirty := int32(t0.Unix())

	return tenThirtyTwo, tenFiftyNine, tenThirty
}

func noopGetTargetData(ctx context.Context, exp parser.Expr, from, until int32, metricMap map[parser.MetricRequest][]*types.MetricData) (error, int) {
	return nil, 0
}

func TestSummarizeEvalExpr(t *testing.T, tt *SummarizeEvalTestItem) {
	evaluator := metadata.GetEvaluator()

	t.Run(tt.Name, func(t *testing.T) {
		originalMetrics := DeepClone(tt.M)
		exp, _, _ := parser.ParseExpr(tt.Target)

		ctx := context.Background()
		g, err := evaluator.EvalExpr(ctx, exp, 0, 1, tt.M, noopGetTargetData)
		if err != nil {
			t.Errorf("failed to eval %v: %+v", tt.Name, err)
			return
		}
		DeepEqual(t, g[0].Name, originalMetrics, tt.M)
		if g[0].StepTime != tt.Step {
			t.Errorf("bad Step for %s:\ngot  %d\nwant %d", g[0].Name, g[0].StepTime, tt.Step)
		}
		if g[0].StartTime != tt.Start {
			t.Errorf("bad Start for %s: got %s want %s", g[0].Name, time.Unix(int64(g[0].StartTime), 0).Format(time.StampNano), time.Unix(int64(tt.Start), 0).Format(time.StampNano))
		}
		if g[0].StopTime != tt.Stop {
			t.Errorf("bad Stop for %s: got %s want %s", g[0].Name, time.Unix(int64(g[0].StopTime), 0).Format(time.StampNano), time.Unix(int64(tt.Stop), 0).Format(time.StampNano))
		}

		if !NearlyEqual(g[0].Values, g[0].IsAbsent, tt.W) {
			t.Errorf("failed: %s:\ngot  %+v,\nwant %+v", g[0].Name, g[0].Values, tt.W)
		}
		if g[0].Name != tt.Name {
			t.Errorf("bad Name for %+v: got %v, want %v", g, g[0].Name, tt.Name)
		}
	})
}

type MultiReturnEvalTestItem struct {
	Target  string
	M       map[parser.MetricRequest][]*types.MetricData
	Name    string
	Results map[string][]*types.MetricData
}

func TestMultiReturnEvalExpr(t *testing.T, tt *MultiReturnEvalTestItem) {
	evaluator := metadata.GetEvaluator()

	originalMetrics := DeepClone(tt.M)
	exp, e, err := parser.ParseExpr(tt.Target)
	if err != nil {
		t.Errorf("failed to parse expr %v: %+v. Parsed so far: %s", tt.Name, err, e)
		return
	}
	ctx := context.Background()
	g, err := evaluator.EvalExpr(ctx, exp, 0, 1, tt.M, noopGetTargetData)
	if err != nil {
		t.Errorf("failed to eval %v: %+v", tt.Name, err)
		return
	}
	DeepEqual(t, tt.Name, originalMetrics, tt.M)
	if len(g) == 0 {
		t.Errorf("returned no data %v", tt.Name)
		return
	}
	if g[0] == nil {
		t.Errorf("returned no value %v", tt.Name)
		return
	}
	if g[0].StepTime == 0 {
		t.Errorf("missing Step for %+v", g)
	}
	if len(g) != len(tt.Results) {
		t.Errorf("unexpected results len: got %d, want %d for %s", len(g), len(tt.Results), tt.Target)
	}
	for _, gg := range g {
		r, ok := tt.Results[gg.Name]
		if !ok {
			t.Errorf("missing result Name: %v", gg.Name)
			continue
		}
		if r[0].Name != gg.Name {
			t.Errorf("result Name mismatch, got\n%#v,\nwant\n%#v", gg.Name, r[0].Name)
		}
		if !reflect.DeepEqual(r[0].Values, gg.Values) || !reflect.DeepEqual(r[0].IsAbsent, gg.IsAbsent) ||
			r[0].StartTime != gg.StartTime ||
			r[0].StopTime != gg.StopTime ||
			r[0].StepTime != gg.StepTime {
			t.Errorf("result mismatch, got\n%#v,\nwant\n%#v", gg, r)
		}
	}
}

type EvalTestItem struct {
	//E    parser.Expr
	Target string
	M      map[parser.MetricRequest][]*types.MetricData
	Want   []*types.MetricData
}

func TestEvalExpr(t *testing.T, tt *EvalTestItem) {
	evaluator := metadata.GetEvaluator()
	originalMetrics := DeepClone(tt.M)
	testName := tt.Target
	exp, _, err := parser.ParseExpr(tt.Target)
	ctx := context.Background()
	g, err := evaluator.EvalExpr(ctx, exp, 0, 1, tt.M, noopGetTargetData)

	if err != nil {
		t.Errorf("failed to eval %s: %+v", testName, err)
		return
	}
	if len(g) != len(tt.Want) {
		t.Errorf("%s returned a different number of metrics, actual %v, Want %v", testName, len(g), len(tt.Want))
		return

	}
	DeepEqual(t, testName, originalMetrics, tt.M)

	for i, want := range tt.Want {
		actual := g[i]
		if actual == nil {
			t.Errorf("returned no value %v", tt.Target)
			return
		}
		if actual.StepTime == 0 {
			t.Errorf("missing Step for %+v", g)
		}
		if actual.Name != want.Name {
			t.Errorf("bad Name for %s metric %d: got %s, Want %s", testName, i, actual.Name, want.Name)
		}
		if !NearlyEqualMetrics(actual, want) {
			t.Errorf("different values for %s metric %s: got %v, Want %v", testName, actual.Name, actual.Values, want.Values)
			return
		}
	}
}
