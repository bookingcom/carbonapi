package carbonapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
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
