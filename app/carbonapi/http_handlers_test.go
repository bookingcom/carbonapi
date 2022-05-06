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

func TestGetBrokenGlobs(t *testing.T) {
	var tests = []struct {
		name       string
		metric     string
		glob       *typ.Matches
		newQueries []string
	}{
		{
			name:   "test1",
			metric: "a1.*.*.*",
			glob: &typ.Matches{
				Name: "a1.*.*.*",
				Matches: []typ.Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.*", "a1.b1.c2.*"},
		},
		{
			name:   "test2",
			metric: "a1.b*.c*.d*",
			glob: &typ.Matches{
				Name: "a1.b*.c*.d*",
				Matches: []typ.Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d*", "a1.b1.c2.d*"},
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			newQueries, _ := getBrokenGlobs(tst.metric, tst.glob)
			if !sameMatches(newQueries, tst.newQueries) {
				t.Fatalf("newQueries is different from expected: %+v, %+v", newQueries, tst.newQueries)
			}
		})
	}
}

func sameMatches(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	diff := make(map[string]int, len(x))
	for _, _x := range x {
		diff[_x]++
	}
	for _, _y := range y {
		if _, ok := diff[_y]; !ok {
			return false
		}
		diff[_y] -= 1
		if diff[_y] == 0 {
			delete(diff, _y)
		}
	}
	return len(diff) == 0
}
