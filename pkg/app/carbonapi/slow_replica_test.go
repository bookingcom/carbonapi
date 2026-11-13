package carbonapi

import (
	"context"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
)

// stallingStore accepts renders and answers only after stall, like a store
// warming up after a restart.
type stallingStore struct {
	*partialRenderBackend
	stall   time.Duration
	release chan struct{}
}

func (s *stallingStore) Render(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
	select {
	case <-s.release:
	case <-time.After(s.stall):
	}
	return s.partialRenderBackend.Render(ctx, r)
}

// One store of a three-DC cluster stalls for 10 s: the render answers 200 with
// the other DCs' data after the grace instead of waiting for the store.
func TestRenderDoesNotWaitForSlowReplica(t *testing.T) {
	const grace = time.Second
	backend.SetSlowReplicaGrace(backend.SlowReplicaGrace{Grace: grace})
	t.Cleanup(func() { backend.SetSlowReplicaGrace(backend.SlowReplicaGrace{}) })
	slow := &stallingStore{partialRenderBackend: &partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc2"},
		stall: 10 * time.Second, release: make(chan struct{})}
	t.Cleanup(func() { close(slow.release) })
	app := partialRenderAppOf(t, slow,
		&partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc1"},
		&partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc3"},
		&partialRenderBackend{path: "m.ko", cluster: "cy", dc: "dc1"})
	start := time.Now()
	w := partialRenderRequest(app, "sumSeries(m.ok,m.ko)")
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("render took %v, want about the %v grace", elapsed, grace)
	}
	assertPartialRenderValue(t, w, 100) // 99 from a replica of m.ok + 1
}

// One store stalls and the other DCs of its cluster answer not-found: the
// slow store may hold the only copy, so the render waits for it and returns
// its data rather than cutting it after the grace.
func TestRenderWaitsForSlowPossibleOnlyHolder(t *testing.T) {
	const stall = 300 * time.Millisecond
	backend.SetSlowReplicaGrace(backend.SlowReplicaGrace{Grace: 20 * time.Millisecond})
	t.Cleanup(func() { backend.SetSlowReplicaGrace(backend.SlowReplicaGrace{}) })
	slow := &stallingStore{partialRenderBackend: &partialRenderBackend{path: "m.ok", cluster: "cx", dc: "dc2"},
		stall: stall, release: make(chan struct{})}
	t.Cleanup(func() { close(slow.release) })
	app := partialRenderAppOf(t, slow,
		&partialRenderBackend{path: "m.ok", err: types.ErrMetricsNotFound, cluster: "cx", dc: "dc1"},
		&partialRenderBackend{path: "m.ok", err: types.ErrMetricsNotFound, cluster: "cx", dc: "dc3"},
		&partialRenderBackend{path: "m.ko", cluster: "cy", dc: "dc1"})
	start := time.Now()
	w := partialRenderRequest(app, "sumSeries(m.ok,m.ko)")
	if elapsed := time.Since(start); elapsed < stall-20*time.Millisecond {
		t.Fatalf("render returned after %v without the possible only holder", elapsed)
	}
	assertPartialRenderValue(t, w, 100) // 99 from the slow store + 1
}
