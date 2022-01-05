package parser

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestParseExpr(t *testing.T) {

	tests := []struct {
		s   string
		e   *expr
		err error
	}{
		{
			s: "metric",
			e: &expr{target: "metric"},
		},
		{
			s: "metric.foo",
			e: &expr{target: "metric.foo"},
		},
		{
			s: "metric.*.foo",
			e: &expr{target: "metric.*.foo"},
		},
		{
			s: "func(metric)",
			e: &expr{
				target:    "func",
				etype:     EtFunc,
				args:      []*expr{{target: "metric"}},
				argString: "metric",
			},
		},
		{
			s: "func(metric1,metric2,metric3)",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{target: "metric2"},
					{target: "metric3"}},
				argString: "metric1,metric2,metric3",
			},
		},
		{
			s: "func1(metric1,func2(metricA, metricB),metric3)",
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{target: "func2",
						etype:     EtFunc,
						args:      []*expr{{target: "metricA"}, {target: "metricB"}},
						argString: "metricA, metricB",
					},
					{target: "metric3"}},
				argString: "metric1,func2(metricA, metricB),metric3",
			},
		},

		{
			s: "3",
			e: &expr{val: 3, etype: EtConst},
		},
		{
			s: "3.1",
			e: &expr{val: 3.1, etype: EtConst},
		},
		{
			s: "func1(metric1, 3, 1e2, 2e-3)",
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{val: 3, etype: EtConst},
					{val: 100, etype: EtConst},
					{val: 0.002, etype: EtConst},
				},
				argString: "metric1, 3, 1e2, 2e-3",
			},
		},
		{
			s: "func1(metric1, 'stringconst')",
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{valStr: "stringconst", etype: EtString},
				},
				argString: "metric1, 'stringconst'",
			},
		},
		{
			s: `func1(metric1, "stringconst")`,
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{valStr: "stringconst", etype: EtString},
				},
				argString: `metric1, "stringconst"`,
			},
		},
		{
			s: "func1(metric1, -3)",
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{val: -3, etype: EtConst},
				},
				argString: "metric1, -3",
			},
		},

		{
			s: "func1(metric1, -3 , 'foo' )",
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric1"},
					{val: -3, etype: EtConst},
					{valStr: "foo", etype: EtString},
				},
				argString: "metric1, -3 , 'foo' ",
			},
		},

		{
			s: "func(metric, key='value')",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: EtString, valStr: "value"},
				},
				argString: "metric, key='value'",
			},
		},
		{
			s: "func(metric, key=true)",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: EtString, target: "true", valStr: "true"},
				},
				argString: "metric, key=true",
			},
		},
		{
			s: "func(metric, key=1)",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: EtConst, val: 1},
				},
				argString: "metric, key=1",
			},
		},
		{
			s: "func(metric, key=0.1)",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: EtConst, val: 0.1},
				},
				argString: "metric, key=0.1",
			},
		},

		{
			s: "func(metric, 1, key='value')",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
					{etype: EtConst, val: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: EtString, valStr: "value"},
				},
				argString: "metric, 1, key='value'",
			},
		},
		{
			s: "func(metric, key='value', 1)",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
					{etype: EtConst, val: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: EtString, valStr: "value"},
				},
				argString: "metric, key='value', 1",
			},
		},
		{
			s: "func(metric, key1='value1', key2='value2')",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key1": {etype: EtString, valStr: "value1"},
					"key2": {etype: EtString, valStr: "value2"},
				},
				argString: "metric, key1='value1', key2='value2'",
			},
		},
		{
			s: "func(metric, key2='value2', key1='value1')",
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key2": {etype: EtString, valStr: "value2"},
					"key1": {etype: EtString, valStr: "value1"},
				},
				argString: "metric, key2='value2', key1='value1'",
			},
		},
		{
			s: `foo.b[0-9].qux`,
			e: &expr{
				target: "foo.b[0-9].qux",
				etype:  EtName,
			},
		},
		{
			s: `virt.v1.*.text-match:<foo.bar.qux>`,
			e: &expr{
				target: "virt.v1.*.text-match:<foo.bar.qux>",
				etype:  EtName,
			},
		},
		{
			s: "func2(metricA, metricB)|func1(metric1,metric3)",
			e: &expr{
				target: "func1",
				etype:  EtFunc,
				args: []*expr{
					{target: "func2",
						etype:     EtFunc,
						args:      []*expr{{target: "metricA"}, {target: "metricB"}},
						argString: "metricA, metricB",
					},
					{target: "metric1"},
					{target: "metric3"}},
				argString: "func2(metricA, metricB),metric1,metric3",
			},
		},
		{
			s: `movingAverage(company.server*.applicationInstance.requestsHandled|aliasByNode(1),"5min")`,
			e: &expr{
				target: "movingAverage",
				etype:  EtFunc,
				args: []*expr{
					{target: "aliasByNode",
						etype: EtFunc,
						args: []*expr{
							{target: "company.server*.applicationInstance.requestsHandled"},
							{val: 1, etype: EtConst},
						},
						argString: "company.server*.applicationInstance.requestsHandled,1",
					},
					{etype: EtString, valStr: "5min"},
				},
				argString: `aliasByNode(company.server*.applicationInstance.requestsHandled,1),"5min"`,
			},
		},
		{
			s: `aliasByNode(company.server*.applicationInstance.requestsHandled,1)|movingAverage("5min")`,
			e: &expr{
				target: "movingAverage",
				etype:  EtFunc,
				args: []*expr{
					{target: "aliasByNode",
						etype: EtFunc,
						args: []*expr{
							{target: "company.server*.applicationInstance.requestsHandled"},
							{val: 1, etype: EtConst},
						},
						argString: "company.server*.applicationInstance.requestsHandled,1",
					},
					{etype: EtString, valStr: "5min"},
				},
				argString: `aliasByNode(company.server*.applicationInstance.requestsHandled,1),"5min"`,
			},
		},
		{
			s: `company.server*.applicationInstance.requestsHandled|aliasByNode(1)|movingAverage("5min")`,
			e: &expr{
				target: "movingAverage",
				etype:  EtFunc,
				args: []*expr{
					{target: "aliasByNode",
						etype: EtFunc,
						args: []*expr{
							{target: "company.server*.applicationInstance.requestsHandled"},
							{val: 1, etype: EtConst},
						},
						argString: "company.server*.applicationInstance.requestsHandled,1",
					},
					{etype: EtString, valStr: "5min"},
				},
				argString: `aliasByNode(company.server*.applicationInstance.requestsHandled,1),"5min"`,
			},
		},
		{
			s: `company.server*.applicationInstance.requestsHandled|keepLastValue()`,
			e: &expr{
				target: "keepLastValue",
				etype:  EtFunc,
				args: []*expr{
					{target: "company.server*.applicationInstance.requestsHandled"},
				},
				argString: `company.server*.applicationInstance.requestsHandled`,
			},
		},
		{
			s: "hello&world",
			e: &expr{target: "hello&world"},
		},

		{
			s: `foo.{bar,baz}.qux`,
			e: &expr{
				target: "foo.{bar,baz}.qux",
				etype:  EtName,
			},
		},
		{
			s:   `func(foo.{bar, baz}.qux)`,
			err: ErrSpacesInBraces,
		},
		{
			s:   `func(foo.[{a-z}].qux)`,
			err: ErrBraceInBrackets,
		},
		{
			s: `func(foo.{bar,baz[0-9]}.qux)`,
			e: &expr{
				target: "func",
				etype:  EtFunc,
				args: []*expr{
					{target: "foo.{bar,baz[0-9]}.qux"},
				},
				argString: "foo.{bar,baz[0-9]}.qux",
			},
		},
		{
			s:   `func(a.b.c.[d, e].count )`,
			err: ErrCommaInBrackets,
		},
		{
			s:   `func(foo.[abc ].qux)`,
			err: ErrSpacesInBrackets,
		},
		{
			s:   `func(foo.[[abc]].qux)`,
			err: ErrNestedBrackets,
		},
		{
			s: "  \nfunc2\t  (  \rfoo.b[09].qux  ,   \ns\t)    ",
			e: &expr{
				target: "func2",
				etype:  EtFunc,
				args: []*expr{
					{target: "foo.b[09].qux"},
					{target: "s"},
				},
				argString: "  \rfoo.b[09].qux  ,   \ns\t",
			},
		},
		{
			s: "  \nfunc2\t  (  \rfunc \n (\ng\r)  ,   \ns\t)    ",
			e: &expr{
				target: "func2",
				etype:  EtFunc,
				args: []*expr{
					{
						target: "func",
						etype:  EtFunc,
						args: []*expr{
							{target: "g"},
						},
						argString: "\ng\r",
					},
					{target: "s"},
				},
				// The reason that func is without any whitespaces is that we behave argString of funcs exceptionally.
				argString: "func(\ng\r),   \ns\t",
			},
		},
	}

	for _, ttr := range tests {
		// for fixing golangci-lint: Using the variable on range scope `tt` in function literal
		tt := ttr
		t.Run(tt.s, func(t *testing.T) {
			t.Logf("run case: go test -run 'TestParseExpr/%s'", regexp.QuoteMeta(tt.s))

			e, _, err := ParseExpr(tt.s)
			if err != tt.err {
				t.Errorf(`parse for %+v expects error "%v" but received "%v"`, tt.s, tt.err, err)
			}
			if err == nil && !reflect.DeepEqual(e, tt.e) {
				t.Errorf("parse for %+v failed:\ngot  %+s\nwant %+v", tt.s, spew.Sdump(e), spew.Sdump(tt.e))
			}
		})
	}
}
