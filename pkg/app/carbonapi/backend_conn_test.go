package carbonapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

func TestBackendHTTPTransportSelection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		unset bool
		sync  bool
	}{
		{name: "unset", unset: true},
		{name: "empty"},
		{name: "legacy opt-out", value: "0"},
		{name: "explicit opt-in", value: "1", sync: true},
		{name: "true", value: "true"},
		{name: "other number", value: "2"},
		{name: "whitespace", value: " 1 "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CARBONAPI_SYNC_BACKEND_HTTP", tc.value)
			if tc.unset {
				if err := os.Unsetenv("CARBONAPI_SYNC_BACKEND_HTTP"); err != nil {
					t.Fatal(err)
				}
			}
			config := cfg.DefaultZipperConfig()
			config.MaxIdleConnsPerHost = 23
			config.IdleConnTimeout = 43 * time.Second
			transport := newBackendHTTPTransport(config, nil)
			t.Cleanup((&http.Client{Transport: transport}).CloseIdleConnections)
			if tc.sync {
				if _, ok := transport.(*bnet.SyncTransport); !ok {
					t.Fatalf("transport = %T, want *net.SyncTransport", transport)
				}
				return
			}
			stock, ok := transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport = %T, want *http.Transport", transport)
			}
			if stock.MaxIdleConnsPerHost != config.MaxIdleConnsPerHost || stock.IdleConnTimeout != config.IdleConnTimeout || stock.DialContext == nil {
				t.Fatalf("transport did not retain the configured connection pool and dialer: %+v", stock)
			}
		})
	}
}

// Alerts query the same stores every minute. Store connections must outlive
// that gap, or every burst opens them again and a single slow connect drops a
// series.
func TestBackendIdleConnTimeoutDefaultSpansMinutelyBursts(t *testing.T) {
	if d := cfg.DefaultZipperConfig().IdleConnTimeout; d <= time.Minute {
		t.Fatalf("default idleConnTimeout %s does not span a 1m query interval", d)
	}
}

func TestBackendIdleConnTimeoutFromConfig(t *testing.T) {
	for _, value := range []string{"", "0", "1"} {
		t.Run("sync="+value, func(t *testing.T) {
			t.Setenv("CARBONAPI_SYNC_BACKEND_HTTP", value)
			testBackendIdleConnTimeoutFromConfig(t)
		})
	}
}

func testBackendIdleConnTimeoutFromConfig(t *testing.T) {
	t.Helper()
	var dials atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			dials.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	const idle = 300 * time.Millisecond
	z := cfg.DefaultZipperConfig()
	z.Backends = []string{server.URL}
	z.ProtocolBackends = nil
	z.BackendsByCluster = nil
	z.BackendsByDC = nil
	z.IdleConnTimeout = idle
	ms := newPrometheusMetrics(cfg.DefaultAPIConfig())
	bs, err := InitBackends(z, NewZipperPrometheusMetrics(z), &ms, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	render := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := bs[0].Render(ctx, types.NewRenderRequest([]string{"a.b"}, 1, 2)); !errors.Is(err, types.ErrMetricsNotFound) {
			t.Fatalf("render: %v", err)
		}
	}

	render()
	time.Sleep(idle / 6)
	render()
	if n := dials.Load(); n != 1 {
		t.Fatalf("%d connections for two renders within idleConnTimeout, want 1", n)
	}
	time.Sleep(3 * idle)
	render()
	if n := dials.Load(); n != 2 {
		t.Fatalf("%d connections after idleConnTimeout, want 2 (idle connection not closed)", n)
	}
}

// A reloaded process must take traffic with connections it dialed before
// serving, not dial every store under full load.
func TestBackendPrewarmServesFirstRendersWithoutDialing(t *testing.T) {
	for _, value := range []string{"", "1"} {
		t.Run("sync="+value, func(t *testing.T) {
			t.Setenv("CARBONAPI_SYNC_BACKEND_HTTP", value)
			var dials atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.NotFound(w, nil)
			}))
			server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateNew {
					dials.Add(1)
				}
			}
			server.Start()
			defer server.Close()

			z := cfg.DefaultZipperConfig()
			z.Backends = []string{server.URL, server.URL + "/"} // duplicates dial once
			z.ProtocolBackends = nil
			z.BackendsByCluster = nil
			z.BackendsByDC = nil
			z.PrewarmConnsPerBackend = 3
			ms := newPrometheusMetrics(cfg.DefaultAPIConfig())
			bs, warm, err := initBackends(z, NewZipperPrometheusMetrics(z), &ms, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			prewarmBackends(z, warm, zap.NewNop())
			waitDials := func(want int64) {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for dials.Load() != want && time.Now().Before(deadline) {
					time.Sleep(5 * time.Millisecond)
				}
				if n := dials.Load(); n != want {
					t.Fatalf("%d connections, want %d", n, want)
				}
			}
			waitDials(3)
			if warm.Idle() != 3 {
				t.Fatalf("idle prewarmed = %d, want 3", warm.Idle())
			}

			// Three concurrent renders take the three prewarmed connections.
			errs := make(chan error, 3)
			for i := 0; i < 3; i++ {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_, err := bs[0].Render(ctx, types.NewRenderRequest([]string{"a.b"}, 1, 2))
					errs <- err
				}()
			}
			for i := 0; i < 3; i++ {
				if err := <-errs; !errors.Is(err, types.ErrMetricsNotFound) {
					t.Fatalf("render: %v", err)
				}
			}
			if n := dials.Load(); n != 3 {
				t.Fatalf("%d connections after renders on 3 prewarmed, want 3", n)
			}
		})
	}
}

func TestBackendPrewarmAddrs(t *testing.T) {
	z := cfg.DefaultZipperConfig()
	z.Backends = []string{"http://10.0.0.1:8080", "http://10.0.0.1:8080/", "http://store-2", "https://x:1", "::bad"}
	z.ProtocolBackends = nil
	z.BackendsByCluster = nil
	z.BackendsByDC = nil
	got := prewarmAddrs(z)
	want := []string{"10.0.0.1:8080", "store-2:80"}
	if len(got) != len(want) {
		t.Fatalf("prewarmAddrs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prewarmAddrs = %v, want %v", got, want)
		}
	}
	if d := cfg.DefaultZipperConfig(); d.PrewarmConnsPerBackend <= 0 || d.PrewarmTimeout <= 0 {
		t.Fatalf("prewarm is off by default: %d conns, %s", d.PrewarmConnsPerBackend, d.PrewarmTimeout)
	}
}
