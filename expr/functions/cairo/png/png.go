// +build !cairo

package png

import (
	"net/http"

	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

const HaveGraphSupport = false

func EvalExprGraph(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	return nil, nil
}

func MarshalPNG(params PictureParams, results []*types.MetricData) []byte {
	return nil
}

func MarshalSVG(params PictureParams, results []*types.MetricData) []byte {
	return nil
}

func MarshalPNGRequest(r *http.Request, results []*types.MetricData, templateName string) []byte {
	return nil
}

func MarshalPNGRequestErr(r *http.Request, errStr string, templateName string) []byte {
	return nil
}

func MarshalSVGRequest(r *http.Request, results []*types.MetricData, templateName string) []byte {
	return nil
}

func Description() map[string]types.FunctionDescription {
	return nil
}
