// +build !cairo

package png

import (
	"context"
	"net/http"

	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

const HaveGraphSupport = false

func EvalExprGraph(ctx context.Context, e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData, getTargetData interfaces.GetTargetData) ([]*types.MetricData, error) {
	return nil, nil
}

func MarshalPNG(params PictureParams, results []*types.MetricData) ([]byte, error) {
	return nil, nil
}

func MarshalSVG(params PictureParams, results []*types.MetricData) ([]byte, error) {
	return nil, nil
}

func MarshalPNGRequest(r *http.Request, results []*types.MetricData, templateName string) ([]byte, error) {
	return nil, nil
}

func MarshalPNGRequestErr(r *http.Request, errStr string, templateName string) ([]byte, error) {
	return nil, nil
}

func MarshalSVGRequest(r *http.Request, results []*types.MetricData, templateName string) ([]byte, error) {
	return nil, nil
}

func Description() map[string]types.FunctionDescription {
	return nil
}
