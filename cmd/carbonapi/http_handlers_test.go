package main

import (
	"net/http"
	"testing"

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
