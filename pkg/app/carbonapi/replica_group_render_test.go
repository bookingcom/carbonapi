package carbonapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"testing"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

func TestRenderReplicaGroupFailure(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	ioTimeout := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ETIMEDOUT}
	for _, tc := range []struct {
		name        string
		failure     error
		cluster     string
		peerCluster string
		peerDC      string
		peerError   error
		peerEmpty   bool
		wantError   bool
		wantData    bool
	}{
		{name: "Vanguard_one_refused_93_not_found", failure: refused, cluster: "cy", peerCluster: "cy", peerDC: "dc1", peerError: types.ErrMetricsNotFound},
		{name: "dial_timeout_and_not_found", failure: dialTimeout, cluster: "cy", peerCluster: "cy", peerDC: "dc1", peerError: types.ErrMetricsNotFound},
		{name: "io_timeout_and_not_found", failure: ioTimeout, cluster: "cy", peerCluster: "cy", peerDC: "dc1", peerError: types.ErrMetricsNotFound},
		{name: "wrapped_not_found", failure: refused, cluster: "cy", peerCluster: "cy", peerDC: "dc1", peerError: fmt.Errorf("store: %w", types.ErrMetricsNotFound)},
		{name: "empty_success", failure: refused, cluster: "cy", peerCluster: "cy", peerDC: "dc1", peerEmpty: true},
		{name: "all_group_members_refused", failure: refused, cluster: "cy", peerCluster: "cy", peerDC: "dc1", peerError: refused, wantError: true},
		{name: "data_replica_wins", failure: refused, cluster: "cy", peerCluster: "cy", peerDC: "dc1", wantData: true},
		{name: "other_group_cannot_recover", failure: refused, cluster: "cy", peerCluster: "infra", peerDC: "dc1", peerError: types.ErrMetricsNotFound, wantError: true},
		// A bare dial error is not an http.Client connect failure (no *url.Error),
		// so the cluster not-found rule does not apply; real refusals are in
		// TestRenderStarWithUnreachableSingleStoreGroup.
		{name: "other_dc_503_data", failure: bnet.ErrHTTPCode(http.StatusServiceUnavailable), cluster: "cy", peerCluster: "cy", peerDC: "dc4", wantData: true},
		{name: "other_dc_503_not_found", failure: bnet.ErrHTTPCode(http.StatusServiceUnavailable), cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound},
		{name: "other_dc_wrapped_503_not_found", failure: fmt.Errorf("store: %w", bnet.ErrHTTPCode(http.StatusServiceUnavailable)), cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound},
		{name: "other_dc_read_timeout_not_found", failure: ioTimeout, cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound},
		{name: "other_dc_deadline_not_found", failure: context.DeadlineExceeded, cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound},
		{name: "other_dc_500_still_fails", failure: bnet.ErrHTTPCode(http.StatusInternalServerError), cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound, wantError: true},
		{name: "other_dc_bad_body_still_fails", failure: errors.New("invalid protobuf"), cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound, wantError: true},
		{name: "unrelated_cluster_cannot_cover_503", failure: bnet.ErrHTTPCode(http.StatusServiceUnavailable), cluster: "cy", peerCluster: "infra", peerDC: "dc4", peerError: types.ErrMetricsNotFound, wantError: true},
		{name: "all_replicas_503_still_fails", failure: bnet.ErrHTTPCode(http.StatusServiceUnavailable), cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: bnet.ErrHTTPCode(http.StatusServiceUnavailable), wantError: true},
		{name: "ungrouped_503_still_fails", failure: bnet.ErrHTTPCode(http.StatusServiceUnavailable), peerError: types.ErrMetricsNotFound, wantError: true},
		{name: "other_dc_cannot_recover", failure: refused, cluster: "cy", peerCluster: "cy", peerDC: "dc4", peerError: types.ErrMetricsNotFound, wantError: true},
		{name: "ungrouped_unchanged", failure: refused, peerError: types.ErrMetricsNotFound, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stores := []*partialRenderBackend{{path: "m.ok", err: tc.failure, cluster: tc.cluster, dc: "dc1"}}
			for range 2 {
				stores = append(stores, &partialRenderBackend{path: "m.ok", err: tc.peerError, empty: tc.peerEmpty, cluster: tc.peerCluster, dc: tc.peerDC})
			}
			// The canary had 94 replies: one refusal and 93 not-found,
			// including the two peers in the refused store's replica group.
			for i := range 91 {
				stores = append(stores, &partialRenderBackend{path: "m.ok", err: types.ErrMetricsNotFound, cluster: fmt.Sprintf("other-%d", i), dc: "dc1"})
			}
			// Even in the same group, success for another target must not hide this
			// failed fetch. Path routing excludes this peer from the m.ok request.
			stores = append(stores, &partialRenderBackend{path: "m.ko", cluster: tc.cluster, dc: "dc1"})
			app := partialRenderApp(t, stores...)
			w := partialRenderRequest(app, "sumSeries(m.ok,m.ko)")
			if tc.wantError {
				if w.Code != http.StatusInternalServerError {
					t.Fatalf("HTTP %d, want 500: %s", w.Code, w.Body.String())
				}
			} else {
				want := 1.0
				if tc.wantData {
					want = 100
				}
				assertPartialRenderValue(t, w, want)
			}

			// Exercise ordinary and grouped fetches separately; replies for another
			// target, group or DC must not establish success for this request.
			metrics, err := Render(app.TopLevelDomainCache, nil, false, app.Backends, app.ZipperConfig.RenderReplicaMismatchConfig,
				context.Background(), "m.ok", 1794392400, 1794393000, app.ZipperMetrics, zap.NewNop())
			checkReplicaGroupFetch(t, err, len(metrics), tc.wantError, tc.wantData)
			replies := sendGroupedRenderRequests(app, context.Background(), []string{"m.ok", "m.ko"}, 1794392400, 1794393000, &carbonapipb.AccessLogDetails{}, zap.NewNop())
			checkReplicaGroupFetch(t, replies[0].error, len(replies[0].data), tc.wantError, tc.wantData)
			if replies[1].error != nil || len(replies[1].data) != 1 {
				t.Fatalf("independent input changed: %+v", replies[1])
			}
		})
	}
}

func checkReplicaGroupFetch(t *testing.T, err error, nMetrics int, wantError, wantData bool) {
	t.Helper()
	var notFound types.ErrNotFound
	fatal := err != nil && !errors.As(err, &notFound)
	if fatal != wantError {
		t.Fatalf("fetch error = %v, want fatal %v", err, wantError)
	}
	if (nMetrics != 0) != wantData {
		t.Fatalf("got %d metrics, want data %v", nMetrics, wantData)
	}
}
