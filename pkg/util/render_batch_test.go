package util

import (
	"context"
	"testing"
)

type batchTestKey struct{}

func TestRenderBatchContextIsExactAndPreservesCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(WithPriority(context.Background(), 10000))
	a, b := WithRenderBatch(parent, 32), WithRenderBatch(parent, 32)
	id, limit := RenderBatch(a)
	other, _ := RenderBatch(b)
	if id == nil || id == other || limit != 32 || GetPriority(a) != 10000 {
		t.Fatal("batch context lost identity, size or priority")
	}
	for _, c := range []context.Context{parent, WithRenderBatch(parent, 1), context.WithValue(a, batchTestKey{}, true)} {
		if id, n := RenderBatch(c); id != nil || n != 1 {
			t.Fatal("unmarked/wrapped context was batched")
		}
	}
	cancel()
	if a.Err() != context.Canceled || b.Err() != context.Canceled {
		t.Fatal("cancellation not inherited")
	}
}
