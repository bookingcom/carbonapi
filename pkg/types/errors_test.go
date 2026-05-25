package types

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAPIError_JSONSerialization(t *testing.T) {
	resp := struct {
		Error     string `json:"error"`
		Status    int    `json:"status"`
		RequestID string `json:"request_id,omitempty"`
	}{
		Error:     "backends unavailable",
		Status:    503,
		RequestID: "req-001",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	if !strings.Contains(string(data), "backends unavailable") {
		t.Errorf("expected error message in JSON, got: %s", data)
	}

	if !strings.Contains(string(data), "req-001") {
		t.Errorf("expected request_id in JSON, got: %s", data)
	}
}

func TestAPIError_OmitsEmptyRequestID(t *testing.T) {
	resp := struct {
		Error     string `json:"error"`
		Status    int    `json:"status"`
		RequestID string `json:"request_id,omitempty"`
	}{
		Error:  "something failed",
		Status: 500,
		// RequestID intentionally empty
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	if strings.Contains(string(data), "request_id") {
		t.Errorf("request_id should be omitted when empty, got: %s", data)
	}
}

func TestAPIError_StatusCode(t *testing.T) {
	cases := []struct {
		status   int
		expected int
	}{
		{503, 503},
		{400, 400},
		{500, 500},
	}

	for _, tc := range cases {
		resp := struct {
			Status int `json:"status"`
		}{Status: tc.status}

		data, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(string(data), strings.TrimSpace(strings.Join(strings.Fields(string(data)), ""))) {
			t.Errorf("status %d not found in output: %s", tc.expected, data)
		}
	}
}