package carbonapi

import (
	"errors"
	"testing"

	typ "github.com/bookingcom/carbonapi/pkg/types"
)

func TestGetCompleterQuery(t *testing.T) {
	metricTestCases := []string{"foo.bar", "foo/bar", "foo.b", "foo.", "/", "", "."}
	metricCompleterResponse := []string{"foo.bar*", "foo.bar*", "foo.b*", "foo.*", ".*", ".*", ".*"}

	for i, metricTestCase := range metricTestCases {
		response := getCompleterQuery(metricTestCase)
		if metricCompleterResponse[i] != response {
			t.Error("should be same")
		}
	}
}

func TestFindCompleter(t *testing.T) {
	metricTestCases := []typ.Matches{
		{Name: "foo.bar", Matches: []typ.Match{}},
		{Name: "foo.ba*", Matches: []typ.Match{
			{Path: "foo.bat", IsLeaf: true},
		}},
	}
	metricFindCompleterResponse := []string{
		"{\"metrics\":[]}\n",
		"{\"metrics\":[{\"path\":\"foo.bat\",\"name\":\"bat\",\"is_leaf\":\"1\"}]}\n",
	}

	for i, metricTestCase := range metricTestCases {
		response, _ := findCompleter(metricTestCase)
		if metricFindCompleterResponse[i] != string(response) {
			t.Error("should be same")
		}
	}

}

func TestExpandEncoder(t *testing.T) {
	var tests = []struct {
		name        string
		metricIn    typ.Matches
		metricOut   string
		leavesOnly  bool
		groupByExpr bool
	}{
		{
			name: "test1",
			metricIn: typ.Matches{
				Name: "foo.ba*",
				Matches: []typ.Match{
					{Path: "foo.bar", IsLeaf: false},
					{Path: "foo.bat", IsLeaf: true},
				},
			},
			metricOut:   "{\"results\":[\"foo.bar\",\"foo.bat\"]}\n",
			leavesOnly:  false,
			groupByExpr: false,
		},
		{
			name: "test2",
			metricIn: typ.Matches{
				Name: "foo.ba*",
				Matches: []typ.Match{
					{Path: "foo.bar", IsLeaf: false},
					{Path: "foo.bat", IsLeaf: true},
				},
			},
			metricOut:   "{\"results\":[\"foo.bat\"]}\n",
			leavesOnly:  true,
			groupByExpr: false,
		},
		{
			name: "test3",
			metricIn: typ.Matches{
				Name: "foo.ba*",
				Matches: []typ.Match{
					{Path: "foo.bar", IsLeaf: false},
					{Path: "foo.bat", IsLeaf: true},
				},
			},
			metricOut:   "{\"results\":{\"foo.ba*\":[\"foo.bar\",\"foo.bat\"]}}\n",
			leavesOnly:  false,
			groupByExpr: true,
		},
	}
	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			response, _ := expandEncoder(tst.metricIn, tst.leavesOnly, tst.groupByExpr)
			if tst.metricOut != string(response) {
				t.Errorf("%v should be same as %v", tst.metricOut, string(response))
			}
		})
	}
}

func TestOptimistErrsFanIn(t *testing.T) {
	var tests = []struct {
		name       string
		in         []error
		n          int
		isErr      bool
		isNotFound bool
	}{
		{
			name: "1 err, 1 result",
			in: []error{
				errors.New("some error"),
			},
			n:          1,
			isErr:      true,
			isNotFound: false,
		},
		{
			name: "1 not found err, 1 result",
			in: []error{
				typ.ErrMetricsNotFound,
			},
			n:          1,
			isErr:      true,
			isNotFound: true,
		},
		{
			name:       "no errs, no results",
			in:         []error{},
			n:          0,
			isErr:      false,
			isNotFound: false,
		},
		{
			name: "2 mixed errs, 2 results",
			in: []error{
				errors.New("some error"),
				typ.ErrMetricsNotFound,
			},
			n:          2,
			isErr:      true,
			isNotFound: false,
		},
		{
			name: "1 not found err, 2 results,",
			in: []error{
				typ.ErrMetricsNotFound,
			},
			n:          2,
			isErr:      false,
			isNotFound: false,
		},
		{
			name:       "no errs, many results",
			in:         []error{},
			n:          5,
			isErr:      false,
			isNotFound: false,
		},
		{
			name: "1 arbitrary err, 2 results",
			in: []error{
				errors.New("some err"),
			},
			n:          2,
			isErr:      false,
			isNotFound: false,
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			err, _ := optimistFanIn(tst.in, tst.n, "")

			if err != nil {
				if !tst.isErr {
					t.Fatal("got err, when none expected")
				}

				if _, ok := err.(typ.ErrNotFound); ok != tst.isNotFound {
					t.Fatalf("got err *%v* when not found err expected", err)
				}
			}
		})
	}
}
