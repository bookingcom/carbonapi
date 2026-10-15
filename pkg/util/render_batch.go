package util

import "context"

type renderBatchContext struct {
	context.Context
	maxTargets int
}

// WithRenderBatch marks one glob expansion for opportunistic backend batching.
// Only requests carrying this exact context can share an HTTP/gRPC request;
// independent deadlines and clients never share a batch.
func WithRenderBatch(ctx context.Context, maxTargets int) context.Context {
	if maxTargets <= 1 {
		return ctx
	}
	return &renderBatchContext{Context: ctx, maxTargets: maxTargets}
}

// RenderBatch identifies the expansion and its configured target limit.
// Wrapping its context again disables coalescing rather than sharing deadlines.
func RenderBatch(ctx context.Context) (id any, maxTargets int) {
	if c, ok := ctx.(*renderBatchContext); ok {
		return c, c.maxTargets
	}
	return nil, 1
}
