package main

import (
	"net/http"
	"testing"
)

func TestShouldNotBlock(t *testing.T) {
	req, err := http.NewRequest("GET", "nothing", nil)
	if err != nil {
		t.Error(err)
	}

	req.Header.Add("foo", "bar")
	rule := Rule{"spam": ""}

	if shouldBlockRequest(req, rule) {
		t.Error("Should not have blocked this request")
	}
}
