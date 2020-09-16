package interfaces

import (
	"context"

	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type GetTargetData func(ctx context.Context, exp parser.Expr, from, until int32, metricMap map[parser.MetricRequest][]*types.MetricData) (error, int)

// FunctionBase is a set of base methods that partly satisfy Function interface and most probably nobody will modify
type FunctionBase struct {
	Evaluator Evaluator
}

// SetEvaluator sets evaluator
func (b *FunctionBase) SetEvaluator(evaluator Evaluator) {
	b.Evaluator = evaluator
}

// GetEvaluator returns evaluator
func (b *FunctionBase) GetEvaluator() Evaluator {
	return b.Evaluator
}

// Evaluator is a interface for any existing expression parser
type Evaluator interface {
	EvalExpr(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData GetTargetData) ([]*types.MetricData, error)
}

type Order int

const (
	Any Order = iota
	Last
)

type FunctionMetadata struct {
	Name  string
	Order Order
	F     Function
}

// Function is interface that all graphite functions should follow
type Function interface {
	SetEvaluator(evaluator Evaluator)
	GetEvaluator() Evaluator
	Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData GetTargetData) ([]*types.MetricData, error)
	Description() map[string]types.FunctionDescription
}
