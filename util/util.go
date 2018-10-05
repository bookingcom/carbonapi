// Package util provides UUIDs for CarbonAPI and CarbonZipper HTTP requests.
package util

import (
	"context"
	"net/http"

	"github.com/satori/go.uuid"
)

const (
	API    = "X-CTX-CarbonAPI-UUID"
	Zipper = "X-CTX-CarbonZipper-UUID"
)

// GetUUID gets the Carbon UUID of a request.
func GetUUID(ctx context.Context, frontend string) string {
	if id := ctx.Value(frontend); id != nil {
		return id.(string)
	}

	return ""
}

// MarshalCtx ensures that outgoing HTTP requests have a Carbon UUID.
func MarshalCtx(ctx context.Context, request *http.Request) *http.Request {
	if id := ctx.Value(API); id != nil {
		request.Header.Add(API, id.(string))
	}

	if id := ctx.Value(Zipper); id != nil {
		request.Header.Add(Zipper, id.(string))
	}

	return request
}

// WithUUID ensures that a context has a Carbon UUID.
func WithUUID(ctx context.Context) context.Context {
	if id := GetUUID(ctx, Zipper); id != "" {
		return ctx
	}

	id := uuid.NewV4().String()

	return context.WithValue(ctx, Zipper, id)
}

type uuidHandler struct {
	handler http.Handler
	uuidKey string
}

// UUIDHandler is middleware that adds a Carbon UUID to all HTTP requests.
func UUIDHandler(h http.Handler, key string) http.Handler {
	return uuidHandler{
		handler: h,
		uuidKey: key,
	}
}

func (h uuidHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get(h.uuidKey)
	if id == "" {
		id = uuid.NewV4().String()
	}

	ctx := context.WithValue(r.Context(), h.uuidKey, id)

	h.handler.ServeHTTP(w, r.WithContext(ctx))
}
