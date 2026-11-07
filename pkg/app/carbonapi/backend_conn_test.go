package carbonapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

// Alerts query the same stores every minute. Store connections must outlive
// that gap, or every burst opens them again and a single slow connect drops a
// series.
func TestBackendIdleConnTimeoutDefaultSpansMinutelyBursts(t *testing.T) {
	if d := cfg.DefaultZipperConfig().IdleConnTimeout; d <= time.Minute {
		t.Fatalf("default idleConnTimeout %s does not span a 1m query interval", d)
	}
}

func TestBackendIdleConnTimeoutFromConfig(t *testing.T) {
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
