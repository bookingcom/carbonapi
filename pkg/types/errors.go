package types

import "fmt"

// APIError is a structured error response returned by all HTTP endpoints.
type APIError struct {
    Error          string   `json:"error"`
    BackendsFailed []string `json:"backends_failed,omitempty"`
    PartialData    bool     `json:"partial_response,omitempty"`
    RequestID      string   `json:"request_id,omitempty"`
}

// ErrBackendUnavailable is returned when one or more backends fail.
type ErrBackendUnavailable struct {
    Backends []string
}

func (e *ErrBackendUnavailable) Error() string {
    return fmt.Sprintf("backends unavailable: %v", e.Backends)
}

// ErrNoData is returned when a query succeeds but yields no data.
var ErrNoData = fmt.Errorf("no data available for the requested targets")