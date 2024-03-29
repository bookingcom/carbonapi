package parser

import (
	"fmt"
	"strings"
)

// MetricRequest contains all necessary data to request a metric.
type MetricRequest struct {
	Metric string
	From   int32
	Until  int32
}

// ExprType defines a type for expression types constants (e.x. functions, values, constants, parameters, strings)
type ExprType int

const (
	// EtName is a const for 'Series Name' type expression
	EtName ExprType = iota
	// EtFunc is a const for 'Function' type expression
	EtFunc
	// EtConst is a const for 'Constant' type expression
	EtConst
	// EtString is a const for 'String' type expression
	EtString
)

var (
	// ErrMissingExpr is a parse error returned when an expression is missing.
	ErrMissingExpr = ParseError("missing expression")
	// ErrMissingComma is a parse error returned when an expression is missing a comma.
	ErrMissingComma = ParseError("missing comma")
	// ErrMissingQuote is a parse error returned when an expression is missing a quote.
	ErrMissingQuote = ParseError("missing quote")
	// ErrUnexpectedCharacter is a parse error returned when an expression contains an unexpected character.
	ErrUnexpectedCharacter = ParseError("unexpected character")
	// ErrMissingBracket is a parse error returned when an expression is missing an opening or closing bracket.
	ErrMissingBracket = ParseError("missing opening or closing bracket []")
	// ErrMissingBrace is a parse error returned when an expression is missing an opening or closing brace.
	ErrMissingBrace = ParseError("missing opening or closing brace {}")
	// ErrCommaInBrackets is a parse error returned when an expression has comma within brackets.
	ErrCommaInBrackets = ParseError("comma within brackets")
	// ErrSpacesInMetricName is a parse error returned when an expression has space in metric name.
	ErrSpacesInMetricName = ParseError("space in metric name")
	// ErrSpacesInBraces is a parse error returned when an expression has space in braces.
	ErrSpacesInBraces = ParseError("space in braces")
	// ErrSpacesInBrackets is a parse error returned when an expression has space in brackets.
	ErrSpacesInBrackets = ParseError("space in brackets")
	// ErrBraceInBrackets is a parse error returned when an expression has brace within brackets.
	ErrBraceInBrackets = ParseError("brace within brackets")
	// ErrNestedBrackets is a parse error returned when an expression has nested brackets.
	ErrNestedBrackets = ParseError("nested brackets")
	// ErrBadType is an eval error returned when a argument has wrong type.
	ErrBadType = ParseError("bad type")
	// ErrMissingArgument is an eval error returned when a argument is missing.
	ErrMissingArgument = ParseError("missing argument")
	// ErrMissingTimeseries is an eval error returned when a time series argument is missing.
	ErrMissingTimeseries = ParseError("missing time series argument")
	// ErrSeriesDoesNotExist is an eval error returned when a requested time series argument does not exist.
	ErrSeriesDoesNotExist = ParseError("no timeseries with that name")
	// ErrUnknownTimeUnits is an eval error returned when a time unit is unknown to system
	ErrUnknownTimeUnits = ParseError("unknown time units")
	// ErrDifferentCountMetrics is an eval error returned when a function that works on pairs of metrics receives arguments having different number of metrics.
	ErrDifferentCountMetrics = ParseError("both arguments must have the same number of metrics")
	// ErrInvalidArgumentValue is an eval error returned when a function received an argument that has the right type but invalid value
	ErrInvalidArgumentValue = ParseError("invalid function argument value")
	// ErrMovingWindowSizeLessThanRetention is an eval error returned when a function received a moving window size bigger than the metrics retention
	ErrMovingWindowSizeLessThanRetention = ParseError("windowSize function param exceeds metric retention")
)

// ParseError is a type of errors returned from the parser
type ParseError string

func (p ParseError) Error() string {
	return string(p)
}

// Expr defines an interface to talk with expressions
type Expr interface {
	// IsName checks if Expression is 'Series Name' expression
	IsName() bool
	// IsFunc checks if Expression is 'Function' expression
	IsFunc() bool
	// IsConst checks if Expression is 'Constant' expression
	IsConst() bool
	// IsString checks if Expression is 'String' expression
	IsString() bool
	// Type returns type of the expression
	Type() ExprType
	// Target returns target value for expression
	Target() string
	// SetTarget changes target for the expression
	SetTarget(string)
	// MutateTarget changes target for the expression and returns new interface. Please note that it doesn't copy object yet
	MutateTarget(string) Expr
	// ToString returns string representation of expression
	ToString() string

	// FloatValue returns float value for expression.
	FloatValue() float64

	// StringValue returns value of String-typed expression (will return empty string for ConstExpr for example).
	StringValue() string
	// SetValString changes value of String-typed expression
	SetValString(string)
	// MutateValString changes ValString for the expression and returns new interface. Please note that it doesn't copy object yet
	MutateValString(string) Expr

	// Args returns slice of arguments (parsed, as Expr interface as well)
	Args() []Expr
	// NamedArgs returns map of named arguments. E.x. for nonNegativeDerivative(metric1,maxValue=32) it will return map{"maxValue": constExpr(32)}
	NamedArgs() map[string]Expr
	// RawArgs returns string that contains all arguments of expression exactly the same order they appear
	RawArgs() string
	// SetRawArgs changes raw argument list for current expression.
	SetRawArgs(args string)
	// MutateRawArgs changes raw argument list for the expression and returns new interface. Please note that it doesn't copy object yet
	MutateRawArgs(args string) Expr

	// Metrics returns list of metric requests
	Metrics() []MetricRequest

	// GetIntervalArg returns interval typed argument.
	GetIntervalArg(n int, defaultSign int) (int32, error)

	// GetIntervalArg returns n-th argument as string.
	GetStringArg(n int) (string, error)
	// GetIntervalArg returns n-th argument as string. It will replace it with Default value if none present.
	GetStringArgDefault(n int, s string) (string, error)
	// GetStringNamedOrPosArgDefault returns specific positioned string-typed argument or replace it with default if none found.
	GetStringNamedOrPosArgDefault(k string, n int, s string) (string, error)

	// GetFloatArg returns n-th argument as float-typed (if it's convertible to float)
	GetFloatArg(n int) (float64, error)
	// GetFloatArgDefault returns n-th argument as float. It will replace it with Default value if none present.
	GetFloatArgDefault(n int, v float64) (float64, error)
	// GetFloatNamedOrPosArgDefault returns specific positioned float64-typed argument or replace it with default if none found.
	GetFloatNamedOrPosArgDefault(k string, n int, v float64) (float64, error)

	// GetIntArg returns n-th argument as int-typed
	GetIntArg(n int) (int, error)
	// GetIntArgs returns n-th argument as slice of ints
	GetIntArgs(n int) ([]int, error)
	// GetIntArgDefault returns n-th argument as int. It will replace it with Default value if none present.
	GetIntArgDefault(n int, d int) (int, error)
	// GetIntNamedOrPosArgDefault returns specific positioned int-typed argument or replace it with default if none found.
	GetIntNamedOrPosArgDefault(k string, n int, d int) (int, error)

	GetNamedArg(name string) Expr

	// GetBoolArgDefault returns n-th argument as bool. It will replace it with Default value if none present.
	GetBoolArgDefault(n int, b bool) (bool, error)
	// GetBoolNamedOrPosArgDefault returns specific positioned bool-typed argument or replace it with default if none found.
	GetBoolNamedOrPosArgDefault(k string, n int, b bool) (bool, error)

	toExpr() interface{}
}

var _ Expr = &expr{}

// NewTargetExpr Creates new expression with specified target only.
func NewTargetExpr(target string) Expr {
	e := &expr{
		target:    target,
		argString: target,
	}
	return e
}

// NewNameExpr Creates new expression with specified name only.
func NewNameExpr(name string) Expr {
	e := &expr{
		target:    name,
		etype:     EtName,
		argString: name,
	}
	return e
}

// NewConstExpr Creates new Constant expression.
func NewConstExpr(value float64) Expr {
	e := &expr{
		val:       value,
		etype:     EtConst,
		argString: fmt.Sprintf("%v", value),
	}
	return e
}

// NewValueExpr Creates new Value expression.
func NewValueExpr(value string) Expr {
	e := &expr{
		valStr:    value,
		etype:     EtString,
		argString: value,
	}
	return e
}

// ArgName is a type for Name Argument
type ArgName string

// ArgValue is a type for Value Argument
type ArgValue string

// NamedArgs is a type for Hashmap of Named Arguments.
type NamedArgs map[string]interface{}

// NewExpr creates a new expression with specified target and arguments. It will do best it can to identify type of argument
func NewExpr(target string, vaArgs ...interface{}) Expr {
	var nArgsFinal map[string]*expr
	args, nArgs := sliceExpr(vaArgs)
	if args == nil {
		panic(fmt.Sprintf("unsupported argument list for target=%v\n", target))
	}

	var a []*expr
	var argStrs []string
	for _, arg := range args {
		argStrs = append(argStrs, arg.RawArgs())
		a = append(a, arg)
	}

	if nArgs != nil {
		nArgsFinal = make(map[string]*expr)
		for k, v := range nArgs {
			nArgsFinal[k] = v
			argStrs = append(argStrs, k+"="+v.RawArgs())
		}
	}

	e := &expr{
		target:    target,
		etype:     EtFunc,
		args:      a,
		argString: strings.Join(argStrs, ","),
	}

	if nArgsFinal != nil {
		e.namedArgs = nArgsFinal
	}

	return e
}

// NewExprTyped creates a new expression with specified target and arguments. Strictly typed one.
func NewExprTyped(target string, args []Expr) Expr {
	var a []*expr
	var argStrs []string
	for _, arg := range args {
		argStrs = append(argStrs, arg.Target())
		a = append(a, arg.toExpr().(*expr))
	}

	e := &expr{
		target:    target,
		etype:     EtFunc,
		args:      a,
		argString: strings.Join(argStrs, ","),
	}

	return e
}
