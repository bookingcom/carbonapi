package expr

import (
	// Import all known functions
	"context"
	"fmt"

	_ "github.com/bookingcom/carbonapi/pkg/expr/functions"
	"github.com/bookingcom/carbonapi/pkg/expr/helper"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/metadata"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
)

type evaluator struct{}

// EvalExpr evalualtes expressions
func (eval evaluator) EvalExpr(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return EvalExpr(ctx, e, from, until, values, getTargetData)
}

var _evaluator = evaluator{}

// nolint:gochecknoinits
func init() {
	helper.SetEvaluator(_evaluator)
	metadata.SetEvaluator(_evaluator)
}

// EvalExpr is the main expression evaluator
func EvalExpr(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	// Check if reached context deadline
	// We are doing this check here because evaluating expression can be a slow operation and
	// this place is called for each argument and function evaluation
	select {
	case <-ctx.Done():
		return []*types.MetricData{}, ctx.Err()
	default:
	}

	if e.IsName() {
		val := values[parser.MetricRequest{Metric: e.Target(), From: from, Until: until}]
		if val == nil {
			return nil, parser.ErrSeriesDoesNotExist
		}

		return val, nil
	} else if e.IsConst() {
		p := types.MetricData{
			Metric: dataTypes.Metric{
				Name:   e.Target(),
				Values: []float64{e.FloatValue()},
			},
		}
		return []*types.MetricData{&p}, nil
	}
	// evaluate the function

	// all functions have arguments -- check we do too
	if len(e.Args()) == 0 {
		return nil, parser.ErrMissingArgument
	}

	metadata.FunctionMD.RLock()
	f, ok := metadata.FunctionMD.Functions[e.Target()]
	metadata.FunctionMD.RUnlock()
	if ok {
		return f.Do(ctx, e, from, until, values, getTargetData)
	}

	return nil, fmt.Errorf("%w: %s", helper.ErrUnknownFunction, e.Target())
}
