package carbonapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

// lookupStore answers every find and info with its own not-found, or fails
// with err.
type lookupStore struct {
	backend.BackendImpl
	addr, cluster, dc string
	err               error
}

func (s *lookupStore) Find(context.Context, types.FindRequest) (types.Matches, error) {
	if s.err != nil {
		return types.Matches{}, s.err
	}
	return types.Matches{}, types.ErrMatchesNotFound
}

func (s *lookupStore) Info(context.Context, types.InfoRequest) ([]types.Info, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, types.ErrInfoNotFound
}

func (s *lookupStore) Contains([]string) bool                { return false }
func (s *lookupStore) Logger() *zap.Logger                   { return zap.NewNop() }
func (s *lookupStore) GetServerAddress() string              { return s.addr }
func (s *lookupStore) BackendInfo() (string, string, string) { return s.addr, s.cluster, s.dc }

// refusedAt is the error net/http returns for a store that refuses the connection.
func refusedAt(addr string) error {
	return &url.Error{Op: "Get", URL: "http://" + addr + ":8080/metrics/find/", Err: &net.OpError{Op: "dial", Net: "tcp",
		Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
}

// TestFindInfoRefusedStoreNotFound covers finds and infos that match nothing
// while stores refuse connections. A production canary returned find 422 "all
// backends failed with mixed errors: ... connection refused:1 No matches
// found:7" after the required-fetch rule required every error to be a
// not-found. Stopping the retired stores of a resized cluster, one in every
// replica group, then returned 422 again because no group answered in full.
func TestFindInfoRefusedStoreNotFound(t *testing.T) {
	store := func(cluster, dc string, err error) *lookupStore {
		return &lookupStore{addr: cluster + "-" + dc, cluster: cluster, dc: dc, err: err}
	}
	nf := func(cluster, dc string) *lookupStore { return store(cluster, dc, nil) }
	refused := func(cluster, dc string) *lookupStore { return store(cluster, dc, refusedAt(cluster+"-"+dc)) }
	dialTimeout := &url.Error{Op: "Get", URL: "http://cy-dc1a:8080/metrics/find/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}}
	serverError := errors.New("HTTP call failed: 500 Internal Server Error")
	afterAccept := &url.Error{Op: "Get", URL: "http://cy-dc1a:8080/metrics/find/", Err: context.DeadlineExceeded}

	for _, tc := range []struct {
		name     string
		stores   []*lookupStore
		notFound bool // find 200-empty and info 404; otherwise find 422 and info 500
	}{
		{"refused_store_cluster_answered", []*lookupStore{
			refused("cy", "dc1a"), nf("cy", "dc1a"), nf("cy", "dc1a"),
			nf("cy", "dc2b"), nf("cy", "dc2b"), nf("cy", "dc2b"),
			nf("cy", "dc3b"), nf("cy", "dc3b"), nf("cz", "dc1a"),
		}, true},
		{"two_refused_in_one_group", []*lookupStore{
			refused("cy", "dc1a"), refused("cy", "dc1a"), nf("cy", "dc1a"),
			nf("cy", "dc2b"), nf("cy", "dc2b"), nf("cy", "dc3b"),
		}, true},
		{"whole_group_refused", []*lookupStore{
			refused("cy", "dc1a"), refused("cy", "dc3b"), nf("cy", "dc2b"), nf("cy", "dc2b"),
		}, true},
		{"dial_timeout", []*lookupStore{store("cy", "dc1a", dialTimeout), nf("cy", "dc2b")}, true},
		{"refused_in_every_group", []*lookupStore{
			refused("cy", "dc1a"), nf("cy", "dc1a"), nf("cy", "dc1a"),
			refused("cy", "dc2b"), nf("cy", "dc2b"),
			refused("cy", "dc3b"), nf("cy", "dc3b"), nf("cy", "dc3b"), nf("cz", "dc1a"),
		}, true},
		{"refused_in_every_group_dial_timeout", []*lookupStore{
			refused("cy", "dc1a"), nf("cy", "dc1a"), store("cy", "dc2b", dialTimeout), nf("cy", "dc2b"),
		}, true},
		{"one_group_cluster_member_refused", []*lookupStore{refused("cw", "dc1b"), nf("cw", "dc1b"), nf("cy", "dc2b")}, true},
		{"refused_in_every_group_server_error", []*lookupStore{
			refused("cy", "dc1a"), nf("cy", "dc1a"), refused("cy", "dc2b"), store("cy", "dc2b", serverError),
		}, false},
		{"refused_in_every_group_after_accept", []*lookupStore{
			refused("cy", "dc1a"), nf("cy", "dc1a"), refused("cy", "dc2b"), store("cy", "dc2b", afterAccept),
		}, false},
		{"other_cluster_server_error", []*lookupStore{refused("cy", "dc1a"), nf("cy", "dc2b"), store("cz", "dc1a", serverError)}, false},
		{"only_member_of_cluster", []*lookupStore{refused("cw", "dc1b"), nf("cy", "dc2b"), nf("cy", "dc1a")}, false},
		{"cluster_unreachable", []*lookupStore{refused("cy", "dc1a"), refused("cy", "dc2b"), nf("cz", "dc1a")}, false},
		{"server_error", []*lookupStore{store("cy", "dc1a", serverError), nf("cy", "dc2b")}, false},
		{"timeout_after_accept", []*lookupStore{store("cy", "dc1a", afterAccept), nf("cy", "dc2b")}, false},
		{"ungrouped", []*lookupStore{refused("", ""), nf("", "")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			impls := make([]backend.BackendImpl, len(tc.stores))
			for i, s := range tc.stores {
				impls[i] = s
			}
			app := partialRenderAppOf(t, impls...)

			w := httptest.NewRecorder()
			app.findHandler(w, httptest.NewRequest(http.MethodGet, "/metrics/find?format=json&query=x.%2A", nil), zap.NewNop())
			if want := map[bool]int{true: http.StatusOK, false: http.StatusUnprocessableEntity}[tc.notFound]; w.Code != want {
				t.Fatalf("find: HTTP %d, want %d: %s", w.Code, want, w.Body.String())
			}
			if tc.notFound && w.Body.String() != "[]" {
				t.Fatalf("find: body %q, want []", w.Body.String())
			}

			w = httptest.NewRecorder()
			app.infoHandler(w, httptest.NewRequest(http.MethodGet, "/info?format=json&target=x.y", nil), zap.NewNop())
			if want := map[bool]int{true: http.StatusNotFound, false: http.StatusInternalServerError}[tc.notFound]; w.Code != want {
				t.Fatalf("info: HTTP %d, want %d: %s", w.Code, want, w.Body.String())
			}
		})
	}
}
