package carbonapi

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInitHandlersInternal(t *testing.T) {
	httpHandleFuncTests := []struct{
		routeVariable string
		shouldPass bool
	}{
		{"/block-headers/", true},
		{"/unblock-headers/", true},
		{"/debug/version", true},
		{"/debug/pprof/", true},
		{ "/debug/pprof/cmdline", true},
		{"/debug/pprof/profile", true},
		{"/debug/pprof/symbol", true},
		{"/debug/pprof/trace", true},
		{"/invalid/", false},
		{"/debug/vars", true},
		{ "/metrics", true},
	}

	router := initHandlersInternal(testApp)
	ts := httptest.NewUnstartedServer(router)
	l, err := net.Listen("tcp", "127.0.0.1:7081")
	if err != nil {
		log.Fatal(err)
	}
	setupTestHTTPServerWithListener(router, l)
	defer ts.Close()

	for _, tc := range httpHandleFuncTests {
		path := fmt.Sprintf("%s", tc.routeVariable)
		req, err := http.NewRequest("GET", path, nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if rr.Code == http.StatusOK && !tc.shouldPass {
			t.Errorf("Handler should have failed for route %s: got %v",
				tc.routeVariable, rr.Code)
		} else if rr.Code != http.StatusOK && tc.shouldPass {
			t.Errorf("Handler should have passed for route %s: got %v want %v",
				tc.routeVariable, rr.Code, http.StatusOK)
		}
	}
}

func TestInitHandlers(t *testing.T) {
	httpHandleFuncTests := []struct{
		routeVariable string
		shouldPass bool
	}{
		{"/render/", true},
		{"/metrics/find/", true},
		{"/info/", true},
		{"/lb_check/", true},
		{ "/version/", true},
		{"/functions/", true},
		{"/tags/autoComplete/tags/", true},
		{"/", true},
		{"/invalid/", false},
	}

	router := initHandlers(testApp)
	ts := httptest.NewUnstartedServer(router)
	l, err := net.Listen("tcp", "127.0.0.1:8081")
	if err != nil {
		log.Fatal(err)
	}
	setupTestHTTPServerWithListener(router, l)
	defer ts.Close()

	t.Logf("server: %s", ts.URL)

	ts.Start()
	for _, tc := range httpHandleFuncTests {
		path := fmt.Sprintf("%s", tc.routeVariable)
		req, err := http.NewRequest("GET", path, nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if rr.Code == http.StatusOK && !tc.shouldPass {
			t.Errorf("Handler should have failed for route %s: got %v",
				tc.routeVariable, rr.Code)
		} else if rr.Code != http.StatusOK && tc.shouldPass {
			t.Errorf("Handler should have passed for route %s: got %v want %v",
				tc.routeVariable, rr.Code, http.StatusOK)
		}
	}
}

func setupTestHTTPServerWithListener(testHandler http.Handler, listener net.Listener) {
	ts := httptest.NewUnstartedServer(testHandler)
	ts.Listener.Close()
	ts.Listener = listener
	defer ts.Close()
	return
}