package carbonapi

import (
	"net/http"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/types"
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
