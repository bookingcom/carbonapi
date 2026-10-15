package carbonapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

func TestInternalPprofRoutes(t *testing.T) {
	h := initHandlersInternal(&App{}, zap.NewNop())
	for _, path := range []string{
		"/debug/pprof",
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/allocs",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/cmdline",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d", path, rec.Code)
		}
	}
}
