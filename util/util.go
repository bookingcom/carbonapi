// Package util provides UUIDs for CarbonAPI and CarbonZipper HTTP requests.
package util

import (
	"context"
	"net/http"
	"github.com/satori/go.uuid"
)

type key int

const (
	ctxHeaderUUID = "X-CTX-CarbonAPI-UUID"

	uuidKey key = iota
	priorityKey
)

// GetPriority returns the current request priority. Less is more
// If not set, returns highest priority(0)
func GetPriority(ctx context.Context) int {
	if p := ctx.Value(priorityKey); p != nil {
		return p.(int)
	}
	return 0
}

// WithPriority returns new context with priority set
func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, priorityKey, priority)
}

// GetUUID gets the Carbon UUID of a request.
func GetUUID(ctx context.Context) string {
	if id := ctx.Value(uuidKey); id != nil {
		return id.(string)
	}

	return ""
}

// MarshalCtx ensures that outgoing HTTP requests have a Carbon UUID.
func MarshalCtx(ctx context.Context, request *http.Request) *http.Request {
	ctx = WithUUID(ctx)
	request.Header.Add(ctxHeaderUUID, GetUUID(ctx))

	return request
}

// WithUUID ensures that a context has a Carbon UUID.
func WithUUID(ctx context.Context) context.Context {
	if id := GetUUID(ctx); id != "" {
		return ctx
	}

	id := uuid.NewV4().String()

	return context.WithValue(ctx, uuidKey, id)
}

type uuidHandler struct {
	handler http.Handler
}

// UUIDHandler is middleware that adds a Carbon UUID to all HTTP requests.
func UUIDHandler(h http.Handler) http.Handler {
	return uuidHandler{handler: h}
}

func (h uuidHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get(ctxHeaderUUID)
	if id == "" {
		id = uuid.NewV4().String()
	}

	ctx := context.WithValue(r.Context(), uuidKey, id)

	h.handler.ServeHTTP(w, r.WithContext(ctx))
}
