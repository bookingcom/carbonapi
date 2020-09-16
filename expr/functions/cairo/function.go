package cairo

import (
	"context"

	"github.com/bookingcom/carbonapi/expr/functions/cairo/png"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type cairo struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &cairo{}
	functions := []string{"color", "stacked", "areaBetween", "alpha", "dashed", "drawAsInfinite", "secondYAxis", "lineWidth", "threshold"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *cairo) Do(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return png.EvalExprGraph(ctx, e, from, until, values, getTargetData)
}

func (f *cairo) Description() map[string]types.FunctionDescription {
	return png.Description()
}
