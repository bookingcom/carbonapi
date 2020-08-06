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

func TestPessimistErrsFanIn(t *testing.T) {
	tests := []struct {
		name        string
		in          []error
		outNil      bool
		outNotFound bool
	}{
		{
			name:   "none in",
			in:     make([]error, 0),
			outNil: true,
		},
		{
			name:   "nil in",
			in:     nil,
			outNil: true,
		},
		{
			name:        "one in",
			in:          []error{errors.New("some error")},
			outNil:      false,
			outNotFound: false,
		},
		{
			name:        "one not found in",
			in:          []error{typ.ErrMetricsNotFound},
			outNil:      false,
			outNotFound: true,
		},
		{
			name: "many mixed in",
			in: []error{
				errors.New("some error"),
				errors.New("some other error"),
				typ.ErrMetricsNotFound,
				errors.New("some other other error"),
				typ.ErrMetricsNotFound,
			},
			outNil:      false,
			outNotFound: false,
		},
		{
			name: "many generic in",
			in: []error{
				errors.New("error a"),
				errors.New("error b"),
				errors.New("error c"),
			},
			outNil:      false,
			outNotFound: false,
		},
		{
			name: "many not found in",
			in: []error{
				typ.ErrMetricsNotFound,
				typ.ErrMetricsNotFound,
				typ.ErrMetricsNotFound,
			},
			outNil:      false,
			outNotFound: true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			out := pessimistFanIn(tst.in)
			if (out == nil) != tst.outNil {
				t.Fatalf("expected nil = %t, but got %v", tst.outNil, out)
			}
			if _, ok := out.(typ.ErrNotFound); ok != tst.outNotFound {
				t.Fatalf("expected not found = %t, got otherwise", tst.outNotFound)
			}
		})
	}
}
