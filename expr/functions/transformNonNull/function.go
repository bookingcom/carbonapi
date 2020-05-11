package transformNonNull

import (
	"fmt"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

type transformNonNull struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &transformNonNull{}
	functions := []string{"transformNonNull"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

// transformNonNull(seriesList, default=0)
func (f *transformNonNull) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	arg, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}
	defv, err := e.GetFloatNamedOrPosArgDefault("default", 1, 1)
	if err != nil {
		return nil, err
	}

	_, ok := e.NamedArgs()["default"]
	if !ok {
		ok = len(e.Args()) > 1
	}

	var results []*types.MetricData
	for _, a := range arg {
		var name string
		if ok {
			name = fmt.Sprintf("transformNonNull(%s,%g)", a.Name, defv)
		} else {
			name = fmt.Sprintf("transformNonNull(%s)", a.Name)
		}

		r := *a
		r.Name = name
		r.Values = make([]float64, len(a.Values))
		r.IsAbsent = make([]bool, len(a.Values))

		for i, v := range a.Values {
			if !a.IsAbsent[i] {
				v = defv
			}
			r.Values[i] = v
			r.IsAbsent[i] = a.IsAbsent[i]
		}

		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *transformNonNull) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"transformNonNull": {
			Description: "Takes a metric or wildcard seriesList and replaces non-null values with the value\nspecified by `default`.  The value 1 used if not specified.\n\nExample:\n\n.. code-block:: none\n\n  &target=transformNonNull(webapp.pages.*.views, 1)\n\nThis would take any data point with non-null values and replace the value with 1.\nAny other numeric value may be used as well.",
			Function:    "transformNonNull(seriesList, default=1)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "transformNonNull",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Default: types.NewSuggestion(1),
					Name:    "default",
					Type:    types.Float,
				},
			},
		},
	}
}
