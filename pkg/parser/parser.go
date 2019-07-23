package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// expression parser

type expr struct {
	target    string
	etype     ExprType
	val       float64
	valStr    string
	args      []*expr // positional
	namedArgs map[string]*expr
	argString string
}

func (e *expr) IsName() bool {
	return e.etype == EtName
}

func (e *expr) IsFunc() bool {
	return e.etype == EtFunc
}

func (e *expr) IsConst() bool {
	return e.etype == EtConst
}

func (e *expr) IsString() bool {
	return e.etype == EtString
}

func (e *expr) Type() ExprType {
	return e.etype
}

func (e *expr) ToString() string {
	switch e.etype {
	case EtFunc:
		return fmt.Sprintf("%s(%s)", e.target, e.argString)
	case EtConst:
		return fmt.Sprint(e.val)
	case EtString:
		s := e.valStr
		s = strings.Replace(s, `\`, `\\`, -1)
		s = strings.Replace(s, `'`, `\'`, -1)
		return "'" + s + "'"
	}

	return e.target
}

func (e *expr) SetTarget(target string) {
	e.target = target
}

func (e *expr) MutateTarget(target string) Expr {
	e.SetTarget(target)
	return e
}

func (e *expr) Target() string {
	return e.target
}

func (e *expr) FloatValue() float64 {
	return e.val
}

func (e *expr) StringValue() string {
	return e.valStr
}

func (e *expr) SetValString(value string) {
	e.valStr = value
}

func (e *expr) MutateValString(value string) Expr {
	e.SetValString(value)
	return e
}

func (e *expr) RawArgs() string {
	return e.argString
}

func (e *expr) SetRawArgs(args string) {
	e.argString = args
}

func (e *expr) MutateRawArgs(args string) Expr {
	e.SetRawArgs(args)
	return e
}

func (e *expr) Args() []Expr {
	ret := make([]Expr, len(e.args))
	for i := 0; i < len(e.args); i++ {
		ret[i] = e.args[i]
	}
	return ret
}

func (e *expr) NamedArgs() map[string]Expr {
	ret := make(map[string]Expr)
	for k, v := range e.namedArgs {
		ret[k] = v
	}
	return ret
}

func (e *expr) Metrics() []MetricRequest {
	switch e.etype {
	case EtName:
		return []MetricRequest{{Metric: e.target}}
	case EtConst, EtString:
		return nil
	case EtFunc:
		var r []MetricRequest
		for _, a := range e.args {
			r = append(r, a.Metrics()...)
		}

		switch e.target {
		case "timeShift":
			offs, err := e.GetIntervalArg(1, -1)
			if err != nil {
				return nil
			}
			for i := range r {
				r[i].From += offs
				r[i].Until += offs
			}
		case "timeStack":
			offs, err := e.GetIntervalArg(1, -1)
			if err != nil {
				return nil
			}

			start, err := e.GetIntArg(2)
			if err != nil {
				return nil
			}

			end, err := e.GetIntArg(3)
			if err != nil {
				return nil
			}

			var r2 []MetricRequest
			for _, v := range r {
				for i := int32(start); i < int32(end); i++ {
					r2 = append(r2, MetricRequest{
						Metric: v.Metric,
						From:   v.From + (i * offs),
						Until:  v.Until + (i * offs),
					})
				}
			}

			return r2
		case "holtWintersForecast", "holtWintersConfidenceBands", "holtWintersAberration":
			for i := range r {
				r[i].From -= 7 * 86400 // starts -7 days from where the original starts
			}
		case "movingAverage", "movingMedian", "movingMin", "movingMax", "movingSum":
			switch e.args[1].etype {
			case EtString:
				offs, err := e.GetIntervalArg(1, 1)
				if err != nil {
					return nil
				}
				for i := range r {
					r[i].From -= offs
				}
			}
		}
		return r
	}

	return nil
}

func (e *expr) GetIntervalArg(n int, defaultSign int) (int32, error) {
	if len(e.args) <= n {
		return 0, ErrMissingArgument
	}

	if e.args[n].etype != EtString {
		return 0, ErrBadType
	}

	seconds, err := IntervalString(e.args[n].valStr, defaultSign)
	if err != nil {
		return 0, ErrBadType
	}

	return seconds, nil
}

func (e *expr) GetStringArg(n int) (string, error) {
	if len(e.args) <= n {
		return "", ErrMissingArgument
	}

	return e.args[n].doGetStringArg()
}

func (e *expr) GetStringArgs(n int) ([]string, error) {
	if len(e.args) <= n {
		return nil, ErrMissingArgument
	}

	var strs []string

	for i := n; i < len(e.args); i++ {
		a, err := e.GetStringArg(i)
		if err != nil {
			return nil, err
		}
		strs = append(strs, a)
	}

	return strs, nil
}

func (e *expr) GetStringArgDefault(n int, s string) (string, error) {
	if len(e.args) <= n {
		return s, nil
	}

	return e.args[n].doGetStringArg()
}

func (e *expr) GetStringNamedOrPosArgDefault(k string, n int, s string) (string, error) {
	if a := e.getNamedArg(k); a != nil {
		return a.doGetStringArg()
	}

	return e.GetStringArgDefault(n, s)
}

func (e *expr) GetFloatArg(n int) (float64, error) {
	if len(e.args) <= n {
		return 0, ErrMissingArgument
	}

	return e.args[n].doGetFloatArg()
}

func (e *expr) GetFloatArgDefault(n int, v float64) (float64, error) {
	if len(e.args) <= n {
		return v, nil
	}

	return e.args[n].doGetFloatArg()
}

func (e *expr) GetFloatNamedOrPosArgDefault(k string, n int, v float64) (float64, error) {
	if a := e.getNamedArg(k); a != nil {
		return a.doGetFloatArg()
	}

	return e.GetFloatArgDefault(n, v)
}

func (e *expr) GetIntArg(n int) (int, error) {
	if len(e.args) <= n {
		return 0, ErrMissingArgument
	}

	return e.args[n].doGetIntArg()
}

func (e *expr) GetIntArgs(n int) ([]int, error) {

	if len(e.args) <= n {
		return nil, ErrMissingArgument
	}

	var ints []int

	for i := n; i < len(e.args); i++ {
		a, err := e.GetIntArg(i)
		if err != nil {
			return nil, err
		}
		ints = append(ints, a)
	}

	return ints, nil
}

func (e *expr) GetIntArgDefault(n int, d int) (int, error) {
	if len(e.args) <= n {
		return d, nil
	}

	return e.args[n].doGetIntArg()
}

func (e *expr) GetIntNamedOrPosArgDefault(k string, n int, d int) (int, error) {
	if a := e.getNamedArg(k); a != nil {
		return a.doGetIntArg()
	}

	return e.GetIntArgDefault(n, d)
}

func (e *expr) GetNamedArg(name string) Expr {
	return e.getNamedArg(name)
}

func (e *expr) GetBoolNamedOrPosArgDefault(k string, n int, b bool) (bool, error) {
	if a := e.getNamedArg(k); a != nil {
		return a.doGetBoolArg()
	}

	return e.GetBoolArgDefault(n, b)
}

func (e *expr) GetBoolArgDefault(n int, b bool) (bool, error) {
	if len(e.args) <= n {
		return b, nil
	}

	return e.args[n].doGetBoolArg()
}

func (e *expr) GetNodeOrTagArgs(n int) ([]NodeOrTag, error) {
	if len(e.args) <= n {
		return nil, ErrMissingArgument
	}

	var nodeTags []NodeOrTag

	var err error
	for i := n; i < len(e.args); i++ {
		var nodeTag NodeOrTag
		nodeTag.Value, err = e.GetIntArg(i)
		if err != nil {
			// Try to parse it as String
			nodeTag.Value, err = e.GetStringArg(i)
			if err != nil {
				return nil, err
			}
			nodeTag.IsTag = true
		}
		nodeTags = append(nodeTags, nodeTag)
	}

	return nodeTags, nil
}

func (e *expr) insertFirstArg(exp *expr) error {
	if e.etype != EtFunc {
		return fmt.Errorf("pipe to not a function")
	}

	newArgs := []*expr{exp}
	e.args = append(newArgs, e.args...)

	if e.argString == "" {
		e.argString = exp.ToString()
	} else {
		e.argString = exp.ToString() + "," + e.argString
	}

	return nil
}

func parseExprWithoutPipe(e string) (Expr, string, error) {
	// skip whitespace
	for len(e) > 1 && e[0] == ' ' {
		e = e[1:]
	}

	if len(e) == 0 {
		return nil, "", ErrMissingExpr
	}

	if '0' <= e[0] && e[0] <= '9' || e[0] == '-' || e[0] == '+' {
		val, e, err := parseConst(e)
		r, _ := utf8.DecodeRuneInString(e)
		if !unicode.IsLetter(r) {
			return &expr{val: val, etype: EtConst}, e, err
		}
	}

	if e[0] == '\'' || e[0] == '"' {
		val, e, err := parseString(e)
		return &expr{valStr: val, etype: EtString}, e, err
	}

	name, e := parseName(e)

	if strings.ToLower(name) == "false" || strings.ToLower(name) == "true" {
		return &expr{valStr: name, etype: EtString, target: name}, e, nil
	}
	if name == "" {
		return nil, e, ErrMissingArgument
	}

	if e != "" && e[0] == '(' {
		// TODO(civil): Tags: make it a proper Expression
		if name == "seriesByTag" {
			argString, _, _, e, err := parseArgList(e)
			return &expr{target: name + "(" + argString + ")", etype: EtName}, e, err
		}

		exp := &expr{target: name, etype: EtFunc}

		argString, posArgs, namedArgs, e, err := parseArgList(e)
		exp.argString = argString
		exp.args = posArgs
		exp.namedArgs = namedArgs

		return exp, e, err
	}

	return &expr{target: name}, e, nil
}

// ParseExpr actually do all the parsing. It returns expression, original string and error (if any)
func ParseExpr(e string) (Expr, string, error) {
	exp, e, err := parseExprWithoutPipe(e)
	if err != nil {
		return exp, e, err
	}
	return pipe(exp.(*expr), e)
}

func pipe(exp *expr, e string) (*expr, string, error) {
	for len(e) > 1 && e[0] == ' ' {
		e = e[1:]
	}

	if e == "" || e[0] != '|' {
		return exp, e, nil
	}

	wr, e, err := parseExprWithoutPipe(e[1:])
	if err != nil {
		return exp, e, err
	}
	if wr == nil {
		return exp, e, nil
	}

	err = wr.(*expr).insertFirstArg(exp)
	if err != nil {
		return exp, e, err
	}
	exp = wr.(*expr)

	return pipe(exp, e)
}

// IsNameChar checks if specified char is actually a valid (from graphite's protocol point of view)
func IsNameChar(r byte) bool {
	return false ||
		'a' <= r && r <= 'z' ||
		'A' <= r && r <= 'Z' ||
		'0' <= r && r <= '9' ||
		r == '.' || r == '_' ||
		r == '-' || r == '*' ||
		r == '?' || r == ':' ||
		r == '[' || r == ']' ||
		r == '^' || r == '$' ||
		r == '<' || r == '>' ||
		r == '&'
}

func isDigit(r byte) bool {
	return '0' <= r && r <= '9'
}

func parseArgList(e string) (string, []*expr, map[string]*expr, string, error) {

	var (
		posArgs   []*expr
		namedArgs map[string]*expr
	)

	if e[0] != '(' {
		panic("arg list should start with paren")
	}

	var argStringBuffer bytes.Buffer

	e = e[1:]

	// check for empty args
	t := strings.TrimLeft(e, " ")
	if t != "" && t[0] == ')' {
		return "", posArgs, namedArgs, t[1:], nil
	}

	for {
		var arg Expr
		var err error

		argString := e
		arg, e, err = ParseExpr(e)
		if err != nil {
			return "", nil, nil, e, err
		}

		if e == "" {
			return "", nil, nil, "", ErrMissingComma
		}

		// we now know we're parsing a key-value pair
		if arg.IsName() && e[0] == '=' {
			e = e[1:]
			argCont, eCont, errCont := ParseExpr(e)
			if errCont != nil {
				return "", nil, nil, eCont, errCont
			}

			if eCont == "" {
				return "", nil, nil, "", ErrMissingComma
			}

			if !argCont.IsConst() && !argCont.IsName() && !argCont.IsString() {
				return "", nil, nil, eCont, ErrBadType
			}

			if namedArgs == nil {
				namedArgs = make(map[string]*expr)
			}

			exp := &expr{
				etype:  argCont.Type(),
				val:    argCont.FloatValue(),
				valStr: argCont.StringValue(),
				target: argCont.Target(),
			}
			namedArgs[arg.Target()] = exp

			e = eCont
			if argStringBuffer.Len() > 0 {
				argStringBuffer.WriteByte(',')
			}
			argStringBuffer.WriteString(argString[:len(argString)-len(e)])
		} else {
			exp := arg.toExpr().(*expr)
			posArgs = append(posArgs, exp)

			if argStringBuffer.Len() > 0 {
				argStringBuffer.WriteByte(',')
			}
			if exp.IsFunc() {
				argStringBuffer.WriteString(exp.ToString())
			} else {
				argStringBuffer.WriteString(argString[:len(argString)-len(e)])
			}
		}

		// after the argument, trim any trailing spaces
		for len(e) > 0 && e[0] == ' ' {
			e = e[1:]
		}

		if e[0] == ')' {
			return argStringBuffer.String(), posArgs, namedArgs, e[1:], nil
		}

		if e[0] != ',' && e[0] != ' ' {
			return "", nil, nil, "", ErrUnexpectedCharacter
		}

		e = e[1:]
	}
}

func parseConst(s string) (float64, string, error) {

	var i int
	// All valid characters for a floating-point constant
	// Just slurp them all in and let ParseFloat sort 'em out
	for i < len(s) && (isDigit(s[i]) || s[i] == '.' || s[i] == '+' || s[i] == '-' || s[i] == 'e' || s[i] == 'E') {
		i++
	}

	v, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, "", err
	}

	return v, s[i:], err
}

// RangeTables is an array of *unicode.RangeTable
var RangeTables []*unicode.RangeTable

func parseName(s string) (string, string) {

	var (
		braces, i, w int
		r            rune
	)

FOR:
	for braces, i, w = 0, 0, 0; i < len(s); i += w {

		w = 1
		if IsNameChar(s[i]) {
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
		default:
			r, w = utf8.DecodeRuneInString(s[i:])
			if unicode.In(r, RangeTables...) {
				continue
			}
			break FOR
		}

	}

	if i == len(s) {
		return s, ""
	}

	return s[:i], s[i:]
}

func parseString(s string) (string, string, error) {

	if s[0] != '\'' && s[0] != '"' {
		panic("string should start with open quote")
	}

	match := s[0]

	s = s[1:]

	var i int
	for i < len(s) && s[i] != match {
		i++
	}

	if i == len(s) {
		return "", "", ErrMissingQuote

	}

	return s[:i], s[i+1:], nil
}
