package backend

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

const maxRenderBatchURLBytes = 8 << 10

// Opt-in requires one named response per literal target, with omitted targets
// equivalent to ErrMetricsNotFound. Other transport contracts keep single reads.
type targetCoalescer interface{ SupportsTargetCoalescing() bool }

// collectRenderBatch consumes only immediately available requests. Routing has
// already selected this backend separately for every target. One incompatible
// request is carried to the next iteration, never put back into a full queue.
func (b *Backend) collectRenderBatch(first *renderReq) ([]*renderReq, *renderReq) {
	batch := []*renderReq{first}
	transport, supported := b.BackendImpl.(targetCoalescer)
	if !supported || !transport.SupportsTargetCoalescing() {
		return batch, nil
	}
	id, limit := util.RenderBatch(first.Ctx)
	limit = min(limit, b.semaSize)
	if id == nil || limit < 2 || !literalRender(first) {
		return batch, nil
	}
	bytes := encodedRenderTargetBytes(first.Targets[0])
	if bytes > maxRenderBatchURLBytes-128 {
		return batch, nil
	}
	queue := b.fastRenderQ
	slow := util.GetPriority(first.Ctx) >= 10000
	if slow {
		queue = b.slowRenderQ
	}
	for len(batch) < limit {
		if slow && !util.StableRenderBatch(first.Ctx) && b.fastRenderQ.length() != 0 {
			break
		}
		next, ok := queue.tryRecv()
		if !ok {
			return batch, nil
		}
		nextID, _ := util.RenderBatch(next.Ctx)
		if nextID != id || next.From != first.From || next.Until != first.Until || !literalRender(next) {
			return batch, next
		}
		for _, r := range batch {
			if r.Targets[0] == next.Targets[0] {
				return batch, next
			}
		}
		n := encodedRenderTargetBytes(next.Targets[0])
		if bytes+n > maxRenderBatchURLBytes-128 {
			return batch, next
		}
		batch, bytes = append(batch, next), bytes+n
	}
	return batch, nil
}

func literalRender(r *renderReq) bool {
	return len(r.Targets) == 1 && !strings.ContainsAny(r.Targets[0], "*?[]{}\\")
}

func encodedRenderTargetBytes(path string) int { return len("&target=") + len(url.QueryEscape(path)) }

func (b *Backend) renderMetrics(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	if transport, ok := b.BackendImpl.(interface {
		RenderPacked(context.Context, types.RenderRequest) ([]types.Metric, error)
	}); ok {
		return transport.RenderPacked(ctx, request)
	}
	return b.BackendImpl.Render(ctx, request)
}

func (b *Backend) renderOne(r *renderReq) {
	t := prometheus.NewTimer(b.render.duration)
	res, err := b.renderMetrics(r.Ctx, r.RenderRequest)
	t.ObserveDuration()
	if err != nil {
		r.Errors <- err
		return
	}
	b.addSourceMetaToMetrics(res)
	r.Results <- res
}

func (b *Backend) renderBatch(batch []*renderReq) {
	if len(batch) == 1 {
		b.renderOne(batch[0])
		return
	}
	targets := make([]string, len(batch))
	for i, r := range batch {
		targets[i] = r.Targets[0]
	}
	t := prometheus.NewTimer(b.render.duration)
	res, err := b.renderMetrics(batch[0].Ctx, types.NewRenderRequest(targets, batch[0].From, batch[0].Until))
	t.ObserveDuration()
	if err != nil {
		if util.StableRenderBatch(batch[0].Ctx) && batch[0].Ctx.Err() == nil {
			// Preserve each literal's result when one bad target makes a
			// multi-fetch fail. Retries use this group's reserved target slots.
			b.renderIndividually(batch)
			return
		}
		// Each original render keeps its own replica fan-in. In particular, a
		// slow or failed replica must not trigger another whole-query retry.
		for _, r := range batch {
			r.Errors <- err
		}
		return
	}
	b.addSourceMetaToMetrics(res)
	byName := make(map[string][]types.Metric, len(batch))
	for _, path := range targets {
		byName[path] = nil
	}
	for _, m := range res {
		previous, expected := byName[m.Name]
		if !expected || len(previous) != 0 {
			// Preserve backends that return renamed or duplicate series. Each
			// retry owns one of this batch's already reserved metric slots.
			b.renderIndividually(batch)
			return
		}
		byName[m.Name] = []types.Metric{m}
	}
	for _, r := range batch {
		if ms := byName[r.Targets[0]]; len(ms) != 0 {
			r.Results <- ms
		} else {
			// The multi-fetch protocol omits missing series; an individual
			// successful HTTP response with no series is the same not-found.
			r.Errors <- types.ErrMetricsNotFound
		}
	}
}

func (b *Backend) renderIndividually(batch []*renderReq) {
	var wg sync.WaitGroup
	for _, r := range batch {
		wg.Add(1)
		go func(r *renderReq) { defer wg.Done(); b.renderOne(r) }(r)
	}
	wg.Wait()
}
