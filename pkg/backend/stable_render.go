package backend

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/util"
	"go.uber.org/zap"
)

type renderDispatch struct {
	request types.RenderRequest
	results chan []types.Metric
	errors  chan error
}

// GroupedRenderReply preserves the original per-target replica fan-in.
type GroupedRenderReply struct {
	Metrics []types.Metric
	Stats   types.MetricRenderStats
	Errors  []error
}

// RendersGrouped publishes already routed literal reads as complete groups.
// Target order and backend membership are fixed before queue scheduling, so
// repeated queries use the same backend response-cache keys.
func RendersGrouped(ctx context.Context, choices [][]Backend, requests []types.RenderRequest,
	mismatch cfg.RenderReplicaMismatchConfig, lg *zap.Logger) []GroupedRenderReply {
	type group struct {
		backend Backend
		calls   []renderDispatch
	}
	groups := make(map[*requestQueue[*renderReq]]*group)
	results := make([]chan []types.Metric, len(requests))
	errors := make([]chan error, len(requests))
	for i, request := range requests {
		results[i] = make(chan []types.Metric, len(choices[i]))
		errors[i] = make(chan error, len(choices[i]))
		for _, b := range choices[i] {
			g := groups[b.fastRenderQ]
			if g == nil {
				g = &group{backend: b}
				groups[b.fastRenderQ] = g
			}
			g.calls = append(g.calls, renderDispatch{request, results[i], errors[i]})
		}
	}
	for _, g := range groups {
		g.backend.sendRenderGroup(ctx, g.calls)
	}
	replies := make([]GroupedRenderReply, len(requests))
	for i := range requests {
		if len(choices[i]) == 0 {
			continue
		}
		messages := make([][]types.Metric, 0, len(choices[i]))
		for range choices[i] {
			select {
			case metrics := <-results[i]:
				messages = append(messages, metrics)
			case err := <-errors[i]:
				replies[i].Errors = append(replies[i].Errors, err)
			}
		}
		replies[i].Metrics, replies[i].Stats = types.MergeMetrics(messages, mismatch, lg)
	}
	return replies
}

func (b Backend) sendRenderGroup(ctx context.Context, calls []renderDispatch) {
	queue := b.fastRenderQ
	if util.GetPriority(ctx) >= 10000 {
		queue = b.slowRenderQ
	}
	transport, supported := b.BackendImpl.(targetCoalescer)
	if !supported || !transport.SupportsTargetCoalescing() || queue.direct != nil {
		for _, call := range calls {
			b.SendRender(ctx, call.request, call.results, call.errors)
		}
		return
	}
	limit := min(b.semaSize, cap(queue.slots))
	for start := 0; start < len(calls); {
		end, bytes := start, 0
		for end < len(calls) && end-start < limit {
			r := calls[end].request
			if !literalRender(&renderReq{RenderRequest: r}) {
				break
			}
			n := encodedRenderTargetBytes(r.Targets[0])
			if end > start && (r.From != calls[start].request.From || r.Until != calls[start].request.Until || r.Targets[0] == calls[end-1].request.Targets[0] || bytes+n > maxRenderBatchURLBytes-128) {
				break
			}
			if n > maxRenderBatchURLBytes-128 {
				break
			}
			bytes += n
			end++
		}
		if end-start < 2 {
			call := calls[start]
			b.SendRender(ctx, call.request, call.results, call.errors)
			start++
			continue
		}
		group := util.WithStableRenderBatch(ctx, end-start)
		pending := make([]*renderReq, end-start)
		for i, call := range calls[start:end] {
			pending[i] = &renderReq{RenderRequest: call.request, Ctx: group, StartTime: time.Now(), Results: call.results, Errors: call.errors}
		}
		if queue.sendMany(ctx, pending) {
			b.render.inQueue.Add(float64(len(pending)))
			b.render.enqueued.Add(float64(len(pending)))
		} else {
			for _, call := range calls[start:end] {
				call.errors <- ctx.Err()
			}
		}
		start = end
	}
}
