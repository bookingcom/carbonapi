package carbonapi

import (
	"net/http"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/pkg/errors"
)

func TestShouldNotBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	rule := Rule{"foo": "block"}

	if shouldBlockRequest(req, []Rule{rule}) {
		t.Error("Should not have blocked this request")
	}
}

func TestShouldNotBlockWithoutRule(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	// no rules are set
	if shouldBlockRequest(req, []Rule{}) {
		t.Error("Req should not be blocked")
	}
}

func TestShouldBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	rule := Rule{"foo": "bar"}
	if !shouldBlockRequest(req, []Rule{rule}) {
		t.Error("Req should be blocked")
	}
}

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
	metricTestCases := []types.Matches{
		{Name: "foo.bar", Matches: []types.Match{}},
		{Name: "foo.ba*", Matches: []types.Match{
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

func TestErrsFanIn(t *testing.T) {
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
				types.ErrMetricsNotFound,
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
				types.ErrMetricsNotFound,
			},
			n:          2,
			isErr:      true,
			isNotFound: false,
		},
		{
			name: "1 not found err, 2 results,",
			in: []error{
				types.ErrMetricsNotFound,
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
			err := errsFanIn(tst.in, tst.n)

			if err != nil {
				if !tst.isErr {
					t.Fatal("got err, when none expected")
				}

				if _, ok := err.(types.ErrNotFound); ok != tst.isNotFound {
					t.Fatalf("got err *%v* when not found err expected", err)
				}
			}
		})
	}
}
