package backend

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"go.uber.org/zap"
)

func TestRenderGroupAnswersStayPerTarget(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		name := "ordinary"
		if grouped {
			name = "coalesced"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var backends []Backend
			for replica := range 3 {
				b := coalesceBackend(func(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
					if replica == 0 {
						return nil, syscall.ECONNREFUSED
					}
					for _, path := range r.Targets {
						if path == "failed" {
							return nil, syscall.ECONNREFUSED
						}
					}
					var metrics []types.Metric
					for _, path := range r.Targets {
						if path == "data" {
							metrics = append(metrics, coalesceMetrics([]string{path})...)
						}
					}
					if len(metrics) == 0 {
						return nil, types.ErrMetricsNotFound
					}
					return metrics, nil
				}, 4)
				b.Proc()
				backends = append(backends, b)
			}
			paths := []string{"missing", "failed", "data"}
			requests := make([]types.RenderRequest, len(paths))
			choices := make([][]Backend, len(paths))
			for i, path := range paths {
				requests[i] = types.NewRenderRequest([]string{path}, 1, 4)
				choices[i] = backends
			}
			var replies []GroupedRenderReply
			if grouped {
				replies = RendersGrouped(ctx, choices, requests, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
			} else {
				for _, request := range requests {
					metrics, stats, errs := Renders(ctx, backends, request, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
					replies = append(replies, GroupedRenderReply{Metrics: metrics, Stats: stats, Errors: errs})
				}
			}
			for i, reply := range replies {
				wantErrors := 3
				if paths[i] == "data" {
					wantErrors = 1
					if len(reply.Metrics) != 1 || reply.Metrics[0].Name != "data" || reply.Metrics[0].Values[1] != 42 {
						t.Fatalf("data lost: %+v", reply)
					}
				} else if len(reply.Metrics) != 0 {
					t.Fatalf("%s acquired another target's data: %+v", paths[i], reply)
				}
				if len(reply.Errors) != wantErrors {
					t.Fatalf("%s errors = %v, want %d", paths[i], reply.Errors, wantErrors)
				}
				for _, err := range reply.Errors {
					var notFound types.ErrNotFound
					if paths[i] == "failed" {
						if !errors.Is(err, syscall.ECONNREFUSED) {
							t.Fatalf("entirely failed group recovered: %v", reply.Errors)
						}
					} else if !errors.As(err, &notFound) {
						t.Fatalf("%s retained replica failure: %v", paths[i], reply.Errors)
					}
				}
			}
		})
	}
}
