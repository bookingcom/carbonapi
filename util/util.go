// Package util provides UUIDs for CarbonAPI and CarbonZipper HTTP requests.
package util

import (
	"context"
	"net/http"

	"github.com/satori/go.uuid"
)

type key int

const (
	ctxHeaderUUID = "X-CTX-Carbon-UUID"

	uuidKey key = 0
)

// GetUUID gets the Carbon UUID of a request.
func GetUUID(ctx context.Context) string {
	if id := ctx.Value(uuidKey); id != nil {
		return id.(string)
	}

	return ""
}

// ParseCtx ensures that every incoming HTTP request has a Carbon UUID assigned
// to it.
func ParseCtx(h http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		id := req.Header.Get(ctxHeaderUUID)
		if id == "" {
			id = uuid.NewV4().String()
		}

		ctx := context.WithValue(req.Context(), uuidKey, id)
		h.ServeHTTP(rw, req.WithContext(ctx))
	})
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
