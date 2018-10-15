package carbonapi

import (
	"net/http"
	"testing"

	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"github.com/stretchr/testify/assert"
)

func TestShouldNotBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	rule := Rule{"foo": "block"}
	config.blockHeaderRules = RuleConfig{Rules: []Rule{rule}}

	if shouldBlockRequest(req) {
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
	assert.Equal(t, false, shouldBlockRequest(req), "Req should not be blocked")
}

func TestShouldBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	rule := Rule{"foo": "bar"}
	config.blockHeaderRules = RuleConfig{Rules: []Rule{rule}}
	assert.Equal(t, true, shouldBlockRequest(req), "Req should be blocked")
}

func TestGetCompleterQuery(t *testing.T) {
	metricTestCases := []string{"foo.bar", "foo/bar", "foo.b", "foo.", "/", "", "."}
	metricCompleterResponse := []string{"foo.bar*", "foo.bar*", "foo.b*", "foo.*", ".*", ".*", ".*"}

	for i, metricTestCase := range metricTestCases {
		response := getCompleterQuery(metricTestCase)
		assert.Equal(t, metricCompleterResponse[i], response, "should be same")
	}
}

func TestFindCompleter(t *testing.T) {
	metricTestCases := []pb.GlobResponse{
		{Name: "foo.bar", Matches: []pb.GlobMatch{}},
		{Name: "foo.ba*", Matches: []pb.GlobMatch{
			{Path: "foo.bat", IsLeaf: true},
		}},
	}
	metricFindCompleterResponse := []string{
		"{\"metrics\":[]}\n",
		"{\"metrics\":[{\"path\":\"foo.bat\",\"name\":\"bat\",\"is_leaf\":\"1\"}]}\n",
	}

	for i, metricTestCase := range metricTestCases {
		response, _ := findCompleter(metricTestCase)
		assert.Equal(t, string(metricFindCompleterResponse[i]), string(response), "should be same")
	}

}
