package backend

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func slowGrace(t *testing.T, grace time.Duration) prometheus.Counter {
	t.Helper()
	cuts := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_slow_replica_cuts"})
	SetSlowReplicaGrace(SlowReplicaGrace{Grace: grace, Cancelled: cuts})
	t.Cleanup(func() { slowReplicaGrace.Store(nil) })
	return cuts
}

// stallingStore is a store of replica group dc2 that accepts the render, as a
// warming-up store does, and answers with data after stall (or when the test
// ends).
func stallingStore(t *testing.T, cluster string, stall time.Duration) Backend {
	t.Helper()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b, _ := retryStoreFunc("dc2", cluster, 0, nil, func(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
		select {
		case <-release:
		case <-time.After(stall):
		}
		return coalesceMetrics(r.Targets), nil
	})
	return b
}

// renderTimed runs the ordinary and the grouped fan-in and reports each one's
// replies and duration.
func renderTimed(t *testing.T, backends []Backend, mismatch cfg.RenderReplicaMismatchConfig,
	check func(name string, elapsed time.Duration, replies []GroupedRenderReply)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request := types.NewRenderRequest([]string{"m"}, 1, 4)
	start := time.Now()
	reply := GroupedRenderReply{}
	reply.Metrics, reply.Stats, reply.Errors = Renders(ctx, backends, request, mismatch, zap.NewNop())
	check("ordinary", time.Since(start), []GroupedRenderReply{reply})
	start = time.Now()
	replies := RendersGrouped(ctx, [][]Backend{backends, backends},
		[]types.RenderRequest{request, types.NewRenderRequest([]string{"m.2"}, 1, 4)}, mismatch, zap.NewNop())
	check("grouped", time.Since(start), replies)
}

func assertFullData(t *testing.T, name string, reply GroupedRenderReply) {
	t.Helper()
	if len(reply.Metrics) != 1 || reply.Metrics[0].Values[1] != 42 || reply.Metrics[0].IsAbsent[1] {
		t.Fatalf("%s: metrics %+v, want full data", name, reply.Metrics)
	}
}

// Two DCs answer at once and the third accepts and stalls for 10 s, as a
// restarting store does: the render returns the merged data after the grace,
// and the abandoned fetch is a plain error that no longer fails the render.
func TestSlowReplicaCutAfterGrace(t *testing.T) {
	const grace = time.Second
	for _, mode := range []cfg.ReplicaMatchMode{cfg.ReplicaMatchModeNormal, cfg.ReplicaMatchModeMajority} {
		t.Run(string(mode), func(t *testing.T) {
			cuts := slowGrace(t, grace)
			ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
			lhr, _ := retryStoreIn("dc3", "cy", 0, nil, nil)
			fra := stallingStore(t, "cy", 10*time.Second)
			backends := []Backend{fra, ams, lhr}
			renderTimed(t, backends, cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: mode},
				func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
					if elapsed < grace-50*time.Millisecond || elapsed > 1500*time.Millisecond {
						t.Fatalf("%s: took %v, want the %v grace", name, elapsed, grace)
					}
					for _, reply := range replies {
						assertFullData(t, name, reply)
						if len(reply.Errors) != 1 || !errors.Is(reply.Errors[0], errSlowReplica) {
							t.Fatalf("%s: errs %v, want the abandoned fetch", name, reply.Errors)
						}
						// renderErrorsFanIn: fewer errors than backends never fails a render with data.
						if len(reply.Errors) >= len(backends) {
							t.Fatalf("%s: %d errors for %d backends would fail the render", name, len(reply.Errors), len(backends))
						}
					}
				})
			// One ordinary and two grouped fetches.
			if got := counterValue(cuts); got != 3 {
				t.Fatalf("cancelled %v slow fetches, want 3", got)
			}
		})
	}
}

// A slow store whose group-mates answered not-found leaves its group answered:
// the abandoned fetch is recovered as not-found, like the group's other errors.
func TestSlowReplicaCutInAnsweredGroup(t *testing.T) {
	cuts := slowGrace(t, 100*time.Millisecond)
	ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
	lhr, _ := retryStoreIn("dc3", "cy", 0, nil, nil)
	fraPeer, _ := retryStoreIn("dc2", "cy", 0, nil, types.ErrMetricsNotFound)
	fra := stallingStore(t, "cy", 10*time.Second)
	renderTimed(t, []Backend{fraPeer, fra, ams, lhr}, cfg.RenderReplicaMismatchConfig{},
		func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
			if elapsed > 600*time.Millisecond {
				t.Fatalf("%s: took %v", name, elapsed)
			}
			for _, reply := range replies {
				assertFullData(t, name, reply)
				var notFound types.ErrNotFound
				for _, err := range reply.Errors {
					if !errors.As(err, &notFound) {
						t.Fatalf("%s: errs %v, want only not-found", name, reply.Errors)
					}
				}
			}
		})
	if counterValue(cuts) != 3 {
		t.Fatalf("cancelled %v slow fetches, want 3", counterValue(cuts))
	}
}

// A not-found counts as a definitive answer once another group has data: the
// slow group is cut after the grace with one group holding the data and the
// other answering not-found.
func TestSlowReplicaCutWithNotFoundGroup(t *testing.T) {
	const grace = 100 * time.Millisecond
	cuts := slowGrace(t, grace)
	ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
	lhr, _ := retryStoreIn("dc3", "cy", 0, nil, types.ErrMetricsNotFound)
	fra := stallingStore(t, "cy", 10*time.Second)
	renderTimed(t, []Backend{fra, ams, lhr}, cfg.RenderReplicaMismatchConfig{},
		func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
			if elapsed < grace-20*time.Millisecond || elapsed > grace+500*time.Millisecond {
				t.Fatalf("%s: took %v, want the %v grace", name, elapsed, grace)
			}
			for _, reply := range replies {
				assertFullData(t, name, reply)
				var slow bool
				for _, err := range reply.Errors {
					slow = slow || errors.Is(err, errSlowReplica)
				}
				if len(reply.Errors) != 2 || !slow {
					t.Fatalf("%s: errs %v, want the not-found and the abandoned fetch", name, reply.Errors)
				}
			}
		})
	if counterValue(cuts) != 3 {
		t.Fatalf("cancelled %v slow fetches, want 3", counterValue(cuts))
	}
}

// A slow group is awaited in full unless every other group of its cluster gave
// a definitive answer and one of them returned data. If all the others said
// not-found, it may hold the only copy of the series.
func TestSlowReplicaAwaitedWithoutFullCoverage(t *testing.T) {
	const stall = 300 * time.Millisecond
	for _, tc := range []struct {
		name   string
		others func() []Backend
	}{
		{name: "only_copy", others: func() []Backend {
			ams, _ := retryStoreIn("dc1", "cy", 0, nil, types.ErrMetricsNotFound)
			lhr, _ := retryStoreIn("dc3", "cy", 0, nil, types.ErrMetricsNotFound)
			return []Backend{ams, lhr}
		}},
		{name: "other_group_server_error", others: func() []Backend {
			ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
			lhr, _ := retryStoreIn("dc3", "cy", 0, nil, errors.New("upstream HTTP 503"))
			return []Backend{ams, lhr}
		}},
		{name: "other_cluster_data", others: func() []Backend {
			ams, _ := retryStoreIn("dc1", "carbon", 0, nil, nil)
			lhr, _ := retryStoreIn("dc3", "carbon", 0, nil, nil)
			return []Backend{ams, lhr}
		}},
		{name: "ungrouped", others: func() []Backend {
			a, _ := retryStoreIn("dc1", "", 0, nil, nil)
			b, _ := retryStoreIn("dc3", "", 0, nil, nil)
			return []Backend{a, b}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cuts := slowGrace(t, 20*time.Millisecond)
			cluster := "cy"
			if tc.name == "ungrouped" {
				cluster = ""
			}
			fra := stallingStore(t, cluster, stall)
			renderTimed(t, append([]Backend{fra}, tc.others()...), cfg.RenderReplicaMismatchConfig{},
				func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
					if elapsed < stall-20*time.Millisecond {
						t.Fatalf("%s: returned after %v without the slow group", name, elapsed)
					}
					for _, reply := range replies {
						assertFullData(t, name, reply)
						for _, err := range reply.Errors {
							if errors.Is(err, errSlowReplica) {
								t.Fatalf("%s: slow group cut: %v", name, reply.Errors)
							}
						}
					}
				})
			if counterValue(cuts) != 0 {
				t.Fatalf("cancelled %v slow fetches, want 0", counterValue(cuts))
			}
		})
	}
}

// Healthy groups are unaffected: all fast is unchanged, and a group that is
// slower but answers within the grace still heals the merged series.
func TestSlowReplicaWithinGraceStillMerged(t *testing.T) {
	cuts := slowGrace(t, time.Second)
	holey := func(dc string, delay time.Duration, value float64, absent bool) Backend {
		b, _ := retryStoreFunc(dc, "cy", 0, nil, func(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
			time.Sleep(delay)
			ms := coalesceMetrics(r.Targets)
			for i := range ms {
				ms[i].Values[2], ms[i].IsAbsent[2] = value, absent
			}
			return ms, nil
		})
		return b
	}
	for _, tc := range []struct {
		name  string
		delay time.Duration
		max   time.Duration
	}{
		{name: "all_fast", max: 200 * time.Millisecond},
		{name: "slower_within_grace", delay: 150 * time.Millisecond, max: 600 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backends := []Backend{holey("dc1", 0, 0, true), holey("dc3", 0, 0, true), holey("dc2", tc.delay, 7, false)}
			renderTimed(t, backends, cfg.RenderReplicaMismatchConfig{},
				func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
					if elapsed < tc.delay || elapsed > tc.max {
						t.Fatalf("%s: took %v", name, elapsed)
					}
					for _, reply := range replies {
						assertFullData(t, name, reply)
						if len(reply.Errors) != 0 || reply.Metrics[0].IsAbsent[2] || reply.Metrics[0].Values[2] != 7 {
							t.Fatalf("%s: %+v, want fra's point healed in and no errors", name, reply)
						}
					}
				})
		})
	}
	if counterValue(cuts) != 0 {
		t.Fatalf("cancelled %v fetches, want 0", counterValue(cuts))
	}
}

// Grace 0, or no policy, waits for the slow store as before.
func TestSlowReplicaGraceZeroWaits(t *testing.T) {
	const stall = 300 * time.Millisecond
	for _, name := range []string{"zero", "unset"} {
		t.Run(name, func(t *testing.T) {
			var cuts prometheus.Counter
			if name == "zero" {
				cuts = slowGrace(t, 0)
			}
			ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
			lhr, _ := retryStoreIn("dc3", "cy", 0, nil, nil)
			fra := stallingStore(t, "cy", stall)
			renderTimed(t, []Backend{fra, ams, lhr}, cfg.RenderReplicaMismatchConfig{},
				func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
					if elapsed < stall-20*time.Millisecond {
						t.Fatalf("%s: returned after %v without the slow store", name, elapsed)
					}
					for _, reply := range replies {
						assertFullData(t, name, reply)
						if len(reply.Errors) != 0 {
							t.Fatalf("%s: errs %v", name, reply.Errors)
						}
					}
				})
			if cuts != nil && counterValue(cuts) != 0 {
				t.Fatalf("cancelled %v fetches with grace 0", counterValue(cuts))
			}
		})
	}
}

// The refused-store path (connect-retry cancellation) and the slow-store grace compose: a refusal is
// cancelled at once, a stall after the grace, and neither counts the other.
func TestSlowReplicaWithRefusedStore(t *testing.T) {
	const grace = 200 * time.Millisecond
	t.Run("refused_only", func(t *testing.T) {
		_, _, cancels := fastConnectRetryCancels(t, 5*time.Second)
		cuts := slowGrace(t, grace)
		ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
		lhr, _ := retryStoreIn("dc3", "cy", 0, nil, nil)
		fra, _ := retryStoreIn("dc2", "cy", 1<<30, refusedDial, nil)
		renderTimed(t, []Backend{fra, ams, lhr}, cfg.RenderReplicaMismatchConfig{},
			func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
				if elapsed > 150*time.Millisecond {
					t.Fatalf("%s: took %v for a refused store", name, elapsed)
				}
				for _, reply := range replies {
					assertFullData(t, name, reply)
				}
			})
		if counterValue(cancels) != 3 || counterValue(cuts) != 0 {
			t.Fatalf("connect cancels %v slow cuts %v, want 3/0", counterValue(cancels), counterValue(cuts))
		}
	})
	t.Run("refused_and_stalled_in_one_group", func(t *testing.T) {
		retries, _, cancels := fastConnectRetryCancels(t, 5*time.Second)
		cuts := slowGrace(t, grace)
		ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
		lhr, _ := retryStoreIn("dc3", "cy", 0, nil, nil)
		refused, _ := retryStoreIn("dc2", "cy", 1<<30, refusedDial, nil)
		stalled := stallingStore(t, "cy", 10*time.Second)
		renderTimed(t, []Backend{refused, stalled, ams, lhr}, cfg.RenderReplicaMismatchConfig{},
			func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
				if elapsed < grace-20*time.Millisecond || elapsed > grace+500*time.Millisecond {
					t.Fatalf("%s: took %v, want the %v grace", name, elapsed, grace)
				}
				for _, reply := range replies {
					assertFullData(t, name, reply)
					if len(reply.Errors) != 2 {
						t.Fatalf("%s: errs %v, want the refusal and the abandoned fetch", name, reply.Errors)
					}
					var refusal, slow bool
					for _, err := range reply.Errors {
						var u unreachableError
						if errors.As(err, &u) {
							t.Fatalf("%s: internal retry marker leaked", name)
						}
						refusal = refusal || errors.Is(err, syscall.ECONNREFUSED)
						slow = slow || errors.Is(err, errSlowReplica)
					}
					if !refusal || !slow {
						t.Fatalf("%s: errs %v", name, reply.Errors)
					}
				}
			})
		if counterValue(retries) != 0 || counterValue(cancels) != 3 || counterValue(cuts) != 3 {
			t.Fatalf("retries %v connect cancels %v slow cuts %v, want 0/3/3", counterValue(retries), counterValue(cancels), counterValue(cuts))
		}
	})
	// A refusing group gave no definitive answer, so the stalled group is not
	// cut: two bad groups at once wait for the slow one, and then the refusal
	// is cancelled.
	t.Run("refused_group_and_stalled_group", func(t *testing.T) {
		const stall = 300 * time.Millisecond
		_, _, cancels := fastConnectRetryCancels(t, 5*time.Second)
		cuts := slowGrace(t, 20*time.Millisecond)
		ams, _ := retryStoreIn("dc1", "cy", 0, nil, nil)
		lhr, _ := retryStoreIn("dc3", "cy", 1<<30, refusedDial, nil)
		fra := stallingStore(t, "cy", stall)
		renderTimed(t, []Backend{fra, ams, lhr}, cfg.RenderReplicaMismatchConfig{},
			func(name string, elapsed time.Duration, replies []GroupedRenderReply) {
				if elapsed < stall-20*time.Millisecond || elapsed > stall+500*time.Millisecond {
					t.Fatalf("%s: took %v", name, elapsed)
				}
				for _, reply := range replies {
					assertFullData(t, name, reply)
					if len(reply.Errors) != 1 || !errors.Is(reply.Errors[0], syscall.ECONNREFUSED) {
						t.Fatalf("%s: errs %v, want the refusal only", name, reply.Errors)
					}
				}
			})
		if counterValue(cancels) != 3 || counterValue(cuts) != 0 {
			t.Fatalf("connect cancels %v slow cuts %v, want 3/0", counterValue(cancels), counterValue(cuts))
		}
	})
}
