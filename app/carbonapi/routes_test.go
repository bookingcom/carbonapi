package carbonapi

import (
	"github.com/gorilla/mux"
	"net/http"
	"net/http/httptest"
	"testing"
)

//Note: All routes are already validated in the tests for app handlers
func TestRouteMiddleware(t *testing.T) {
	router := mux.NewRouter()
	path := "/foo"
	fakeHandler := func(w http.ResponseWriter, r *http.Request) {}
	router.HandleFunc(path, fakeHandler)
	customRouter := routeMiddleware(router)

	t.Run("pathWithoutTrailingSlash", func(t *testing.T) {
		testRoutingForPath(t, path, customRouter)
	})
	t.Run("pathWithTrailingSlash", func(t *testing.T) {
		testRoutingForPath(t, path + "/", customRouter)
	})
	t.Run("pathWithParams", func(t *testing.T) {
		testRoutingForPath(t, path + "?bar=foo", customRouter)
	})
	t.Run("pathWithTrailingSlashAndParams", func(t *testing.T) {
		testRoutingForPath(t, path + "/?bar=foo", customRouter)
	})
}

func testRoutingForPath(t *testing.T, path string, router http.Handler) {
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code == http.StatusNotFound {
		t.Errorf("Failed to route path: %s", path)
	}
}
