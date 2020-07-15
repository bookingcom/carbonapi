package carbonapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

//Note: All routes are already validated in the tests for app handlers
func TestRouteMiddleware(t *testing.T) {
	testPath := "/version"

	t.Run("pathWithoutTrailingSlash", func(t *testing.T) {
		testRoutingForPath(t, testPath)
	})
	t.Run("pathWithTrailingSlash", func(t *testing.T) {
		testRoutingForPath(t, testPath + "/")
	})
	t.Run("pathWithParams", func(t *testing.T) {
		testRoutingForPath(t, testPath + "?bar=foo")
	})
	t.Run("pathWithTrailingSlashAndParams", func(t *testing.T) {
		testRoutingForPath(t, testPath + "/?bar=foo")
	})
}

func TestCustomRouteForPathNotFound(t *testing.T) {
	t.Run("invalidPath", func(t *testing.T) {
		testRoutingForPath(t, "/invalid")
	})
}

func testRoutingForPath(t *testing.T, path string) {
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	testRouter.ServeHTTP(rr, req)
	if rr.Code == http.StatusNotFound {
		t.Errorf("Failed to route path: %s", path)
	}
}
