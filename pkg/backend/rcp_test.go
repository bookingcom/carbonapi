package backend

import (
	"net/url"
	"testing"
)

func TestCarbonapiv2RenderEncoder(t *testing.T) {
	u := &url.URL{}

	var from int32 = 100
	var until int32 = 200
	metrics := []string{"foo", "bar"}

	gotURL, gotReader := carbonapiV2RenderEncoder(u, from, until, metrics)
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["from"]; len(got) != 1 || got[0] != "100" {
		t.Errorf("Expected from=100, got %v", got)
	}

	if got := vals["until"]; len(got) != 2 || got[0] != "200" {
		t.Errorf("Expected until=200, got %v", got)
	}

	got := vals["target"]
	if len(got) != 2 || got[0] != "foo" || got[1] != "bar" {
		t.Errorf("Bad target: got %v, expected %v", got, metrics)
	}
}

func TestCarbonapiv2InfoEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL, gotReader := carbonapiV2InfoEncoder(u, "foo")
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["target"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}

func TestCarbonapiv2FindEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL, gotReader := carbonapiV2FindEncoder(u, "foo")
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["query"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}
