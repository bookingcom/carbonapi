package carbonapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

// restartingStore fails its first refusals renders, then serves like its fixture.
type restartingStore struct {
	*partialRenderBackend
	refusals int32
	failure  error
	calls    atomic.Int32
}

func (s *restartingStore) Render(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
	if s.calls.Add(1) <= s.refusals {
		return nil, s.failure
	}
	return s.partialRenderBackend.Render(ctx, r)
}

func TestRenderRetriesRestartingStore(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "http://store/render/", Err: &net.OpError{Op: "dial", Net: "tcp",
		Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	afterAccept := &url.Error{Op: "Get", URL: "http://store/render/", Err: io.ErrUnexpectedEOF}
	const forever = 1 << 30
	for _, tc := range []struct {
		name      string
		window    time.Duration
		refusals  int32
		failure   error
		peer      bool // another member of the refusing store's replica group answers
		otherDC   bool // a replica group in another DC holds the same series
		otherDown int32
		wantCalls int32
		wantError bool
		maxTime   time.Duration
	}{
		{name: "refused_then_up", refusals: 3, failure: refused, wantCalls: 4},
		{name: "refused_past_window", refusals: forever, failure: refused, wantError: true},
		{name: "refused_peer_answers", refusals: forever, failure: refused, peer: true, wantCalls: 1},
		{name: "failure_after_accept", refusals: forever, failure: afterAccept, wantCalls: 1, wantError: true},
		// The other DCs already answered: no retry, no wait, full data.
		{name: "refused_other_group_answers", window: 5 * time.Second, refusals: forever, failure: refused,
			otherDC: true, wantCalls: 1, maxTime: 200 * time.Millisecond},
		{name: "all_groups_refused_then_up", refusals: 3, failure: refused, otherDC: true, otherDown: 3},
		{name: "all_groups_refused_past_window", refusals: forever, failure: refused, otherDC: true, otherDown: forever,
			wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := tc.window
			if window == 0 {
				window = 300 * time.Millisecond
			}
			backend.SetConnectRetry(backend.ConnectRetry{Window: window, InitialDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond})
			t.Cleanup(func() { backend.SetConnectRetry(backend.ConnectRetry{}) })
			store := &restartingStore{partialRenderBackend: &partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc1"},
				refusals: tc.refusals, failure: tc.failure}
			stores := []backend.BackendImpl{store, &partialRenderBackend{path: "m.ko", cluster: "cy", dc: "dc1"}}
			if tc.peer {
				stores = append(stores, &partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc1"})
			}
			if tc.otherDC {
				stores = append(stores, &restartingStore{partialRenderBackend: &partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc3"},
					refusals: tc.otherDown, failure: refused})
			}
			app := partialRenderAppOf(t, stores...)
			start := time.Now()
			w := partialRenderRequest(app, "sumSeries(m.ok,m.ko)")
			elapsed := time.Since(start)
			if tc.wantError {
				if w.Code != http.StatusInternalServerError {
					t.Fatalf("HTTP %d, want 500: %s", w.Code, w.Body.String())
				}
			} else {
				assertPartialRenderValue(t, w, 100) // 99 from a replica of m.ok + 1
			}
			if tc.wantCalls != 0 && store.calls.Load() != tc.wantCalls {
				t.Fatalf("store rendered %d times, want %d", store.calls.Load(), tc.wantCalls)
			}
			if tc.refusals == forever && tc.failure == refused && tc.wantError && (elapsed < window-20*time.Millisecond || store.calls.Load() < 3) {
				t.Fatalf("gave up after %v and %d attempts", elapsed, store.calls.Load())
			}
			if tc.maxTime != 0 && elapsed > tc.maxTime {
				t.Fatalf("request took %v, want under %v", elapsed, tc.maxTime)
			}
			if elapsed > window+2*time.Second {
				t.Fatalf("request took %v", elapsed)
			}
		})
	}
}

// The fe-112 canary scenario end to end: one cluster with three replica groups,
// the one-store dc1 group refused, the dc3 and dc2 groups answering
// not-found. The render returns without the retry wait, and the response is
// the one the same stores give with retries disabled. With the series held by
// another cluster it is complete; with the series nowhere, the refusal counts
// as the cluster's not-found, so the render returns the remaining input.
func TestRenderRefusedStoreOtherGroupsNotFound(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "http://store/render/", Err: &net.OpError{Op: "dial", Net: "tcp",
		Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	for _, tc := range []struct {
		name        string
		dataCluster bool
	}{
		{name: "series_in_other_cluster", dataCluster: true},
		{name: "series_nowhere"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := func(window time.Duration) (*restartingStore, int, string, time.Duration) {
				backend.SetConnectRetry(backend.ConnectRetry{Window: window, InitialDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond})
				t.Cleanup(func() { backend.SetConnectRetry(backend.ConnectRetry{}) })
				store := &restartingStore{partialRenderBackend: &partialRenderBackend{path: "m.ok", cluster: "carbon", dc: "dc1"},
					refusals: 1 << 30, failure: refused}
				stores := []backend.BackendImpl{store,
					&partialRenderBackend{path: "m.ok", err: types.ErrMetricsNotFound, cluster: "carbon", dc: "dc3"},
					&partialRenderBackend{path: "m.ok", err: types.ErrMetricsNotFound, cluster: "carbon", dc: "dc2"},
					&partialRenderBackend{path: "m.ko", cluster: "cy", dc: "dc1"}}
				if tc.dataCluster {
					stores = append(stores, &partialRenderBackend{path: "m.ok", cluster: "cy", dc: "dc2"})
				}
				app := partialRenderAppOf(t, stores...)
				start := time.Now()
				w := partialRenderRequest(app, "sumSeries(m.ok,m.ko)")
				return store, w.Code, w.Body.String(), time.Since(start)
			}
			store, code, body, elapsed := run(5 * time.Second)
			if elapsed > 200*time.Millisecond || store.calls.Load() != 1 {
				t.Fatalf("took %v and %d store calls, want no retry wait", elapsed, store.calls.Load())
			}
			_, baseCode, baseBody, _ := run(0)
			if code != baseCode || body != baseBody {
				t.Fatalf("HTTP %d %s, without retries HTTP %d %s", code, body, baseCode, baseBody)
			}
			w := httptest.NewRecorder()
			w.Code = code
			w.Body.WriteString(body)
			if tc.dataCluster {
				assertPartialRenderValue(t, w, 100)
			} else {
				assertPartialRenderValue(t, w, 1) // m.ok not found, m.ko alone
			}
		})
	}
}

// namespaceStore answers the glob a.b.* with its series, if it has one, and
// every other target with not-found.
type namespaceStore struct {
	*partialRenderBackend
	series  string
	failure error
}

func (s *namespaceStore) Render(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
	if s.failure != nil {
		return nil, s.failure
	}
	if s.series == "" || len(r.Targets) != 1 || r.Targets[0] != "a.b.*" {
		return nil, types.ErrMetricsNotFound
	}
	m := types.Metric{Name: s.series, StartTime: r.From, StopTime: r.Until, StepTime: 60}
	for ts := r.From; ts < r.Until; ts += 60 {
		m.Values = append(m.Values, 42)
		m.IsAbsent = append(m.IsAbsent, false)
	}
	return []types.Metric{m}, nil
}

// The fe-112 canary 500s: dashboards pass the literal target "*" as an extra
// argument, as in sumSeries(a.b.*, *). With the one store of the dc1 carbon
// group refused or timing out at dial and the 93 other stores answering
// not-found for "*", the render returns 200 with the a.b.* series instead of
// failing on the unreachable store. If every carbon group is unreachable, the
// failed "*" fetch could hide data, and the render still fails (the required-fetch rule).
func TestRenderStarWithUnreachableSingleStoreGroup(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "http://store/render/", Err: &net.OpError{Op: "dial", Net: "tcp",
		Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	dialTimeout := &url.Error{Op: "Get", URL: "http://store/render/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}}
	for _, tc := range []struct {
		name          string
		failure       error
		allCarbonDown bool
	}{
		{name: "refused", failure: refused},
		{name: "dial_timeout", failure: dialTimeout},
		{name: "every_carbon_group_unreachable", failure: refused, allCarbonDown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// connectRetry 10s as in production: no retry wait either way.
			backend.SetConnectRetry(backend.ConnectRetry{Window: 10 * time.Second, InitialDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond})
			t.Cleanup(func() { backend.SetConnectRetry(backend.ConnectRetry{}) })
			if tc.allCarbonDown {
				backend.SetConnectRetry(backend.ConnectRetry{})
			}
			store := func(name, cluster, dc string) *namespaceStore {
				return &namespaceStore{partialRenderBackend: &partialRenderBackend{path: name, cluster: cluster, dc: dc}}
			}
			stores := []backend.BackendImpl{&namespaceStore{partialRenderBackend: &partialRenderBackend{path: "store-1", cluster: "carbon", dc: "dc1"}, failure: tc.failure}}
			for _, dc := range []string{"dc3", "dc2"} {
				for i := range 3 {
					s := store(fmt.Sprintf("carbon-%s-%d", dc, i), "carbon", dc)
					if tc.allCarbonDown {
						s.failure = tc.failure
					}
					stores = append(stores, s)
				}
			}
			for c := range 29 {
				for _, dc := range []string{"dc1", "dc3", "dc2"} {
					s := store(fmt.Sprintf("other-%d-%s", c, dc), fmt.Sprintf("other-%d", c), dc)
					if c == 0 {
						s.series = "a.b.c" // a.b.* lives in another cluster
					}
					stores = append(stores, s)
				}
			}
			if len(stores) != 94 {
				t.Fatalf("%d stores", len(stores))
			}
			app := partialRenderAppOf(t, stores...)
			start := time.Now()
			w := partialRenderRequest(app, "sumSeries(a.b.*, *)")
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("render took %v", elapsed)
			}
			if tc.allCarbonDown {
				if w.Code != http.StatusInternalServerError {
					t.Fatalf("HTTP %d, want 500 while every carbon copy is unreachable: %s", w.Code, w.Body.String())
				}
				return
			}
			assertPartialRenderValue(t, w, 42)
			_, err := Render(app.TopLevelDomainCache, nil, false, app.Backends, app.ZipperConfig.RenderReplicaMismatchConfig,
				context.Background(), "*", 1794392400, 1794393000, app.ZipperMetrics, zap.NewNop())
			var notFound types.ErrNotFound
			if !errors.As(err, &notFound) {
				t.Fatalf("fetch of * = %v, want not-found", err)
			}
		})
	}
}
