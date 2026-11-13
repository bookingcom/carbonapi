package backend

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// SlowReplicaGrace bounds how long a render waits for a replica group that is
// still silent after every other replica group of the same cluster gave a
// definitive answer for the target (data or not-found) and at least one of
// them returned data, as a store warming up after a restart does: it accepts
// connections but answers only at the backend timeout.
type SlowReplicaGrace struct {
	// Grace is the extra wait granted to the slow group. Zero waits for every
	// backend as before.
	Grace time.Duration
	// Cancelled counts backend fetches no longer awaited after the grace.
	Cancelled prometheus.Counter
}

var slowReplicaGrace atomic.Pointer[SlowReplicaGrace]

// SetSlowReplicaGrace replaces the process-wide slow replica policy.
func SetSlowReplicaGrace(s SlowReplicaGrace) { slowReplicaGrace.Store(&s) }

// errSlowReplica is the error of a fetch abandoned after the grace. Like any
// other failure, it stays an error only when no member of its own group
// answered; the render already has the other groups' data.
var errSlowReplica = errors.New("render not awaited: every other replica group answered, one with data, and this backend did not answer within the slow replica grace")

// cuttable reports whether some cluster of this fetch has several replica
// groups, so that one of them can ever be left behind.
func (f *renderFetch) cuttable() bool {
	seen := make(map[string]struct{}, len(f.groups))
	for i := range f.groups {
		c := f.groups[i].cluster
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			return true
		}
		seen[c] = struct{}{}
	}
	return false
}

// coveredByOthers reports whether the slow group g may be cut after the grace.
// That needs both:
//
//   - every other replica group of g's cluster has received all its replies
//     and gave a definitive answer: data or the backend's own not-found. A
//     group that is still pending, or only refused or failed, says nothing
//     about the target, so g is awaited.
//   - at least one of those groups returned data. If every other group said
//     not-found, g may be the only holder of the series (replicas can diverge,
//     for example for names created while a DC was down), so g is awaited in
//     full. This is where the slow grace differs from the connect retry
//     (connect_retry.go), which also gives up on a not-found cover: a refused
//     store cannot answer at all until it is back, while a slow one is
//     reachable and will deliver its copy.
func (f *renderFetch) coveredByOthers(gi int, pending []int) bool {
	g := &f.groups[gi]
	data := false
	for oi := range f.groups {
		o := &f.groups[oi]
		if oi == gi || o.cluster != g.cluster {
			continue
		}
		if pending[oi] != 0 || !o.answered {
			return false
		}
		data = data || o.data
	}
	return data
}

type fanInReply struct {
	fetch, group int
	metrics      []types.Metric
	err          error
}

// collectRenderFetches waits for every backend reply of every fetch, except
// that a replica group still silent policy.Grace after all other groups of its
// cluster answered, at least one with data (coveredByOthers), is no longer
// awaited. Its outstanding fetches get
// errSlowReplica; their late replies are dropped. The in-flight requests are
// not cancelled: coalesced batches share one context across targets, and
// another target may still need the same backend.
func collectRenderFetches(fetches []renderFetch, lg *zap.Logger) {
	policy := slowReplicaGrace.Load()
	if policy == nil || policy.Grace <= 0 {
		for i := range fetches {
			collectRenderGroups(&fetches[i])
		}
		return
	}
	var watched []int
	for i := range fetches {
		if fetches[i].cuttable() {
			watched = append(watched, i)
		}
	}
	if len(watched) != 0 {
		collectWithGrace(fetches, watched, policy, lg)
	}
	// Fetches that cannot be cut keep the ordinary fan-in. Their replies are
	// buffered, so collecting them afterwards adds no wait.
	for i := range fetches {
		if !fetches[i].cuttable() {
			collectRenderGroups(&fetches[i])
		}
	}
}

func collectWithGrace(fetches []renderFetch, watched []int, policy *SlowReplicaGrace, lg *zap.Logger) {
	pending := make(map[int][]int, len(watched))     // fetch -> group -> replies awaited
	cutAt := make(map[int][]time.Time, len(watched)) // fetch -> group -> grace deadline
	byGroup := make(map[int][][][]types.Metric, len(watched))
	outstanding := 0
	for _, fi := range watched {
		f := &fetches[fi]
		pending[fi] = make([]int, len(f.groups))
		cutAt[fi] = make([]time.Time, len(f.groups))
		byGroup[fi] = make([][][]types.Metric, len(f.groups))
		for gi := range f.groups {
			pending[fi][gi] = f.groups[gi].n
			outstanding += f.groups[gi].n
		}
	}
	// Buffered for every reply, so forwarders of abandoned groups never block.
	replies := make(chan fanInReply, outstanding)
	for _, fi := range watched {
		for gi := range fetches[fi].groups {
			g := &fetches[fi].groups[gi]
			go func(fi, gi, n int, results <-chan []types.Metric, errs <-chan error) {
				for range n {
					metrics, err := nextReply(results, errs)
					replies <- fanInReply{fi, gi, metrics, err}
				}
			}(fi, gi, g.n, g.results, g.errors)
		}
	}

	var timer *time.Timer
	var next time.Time
	arm := func() {
		next = time.Time{}
		for _, fi := range watched {
			for gi, at := range cutAt[fi] {
				if !at.IsZero() && pending[fi][gi] != 0 && (next.IsZero() || at.Before(next)) {
					next = at
				}
			}
		}
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		if !next.IsZero() {
			timer = time.NewTimer(time.Until(next))
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for outstanding > 0 {
		var expired <-chan time.Time
		if timer != nil {
			expired = timer.C
		}
		select {
		case r := <-replies:
			if pending[r.fetch][r.group] == 0 {
				continue // abandoned group
			}
			pending[r.fetch][r.group]--
			outstanding--
			f := &fetches[r.fetch]
			byGroup[r.fetch][r.group], _ = f.groups[r.group].take(r.metrics, r.err, byGroup[r.fetch][r.group])
			// Deadlines only appear; a group's coverage never goes away.
			changed := false
			for gi := range f.groups {
				if pending[r.fetch][gi] != 0 && cutAt[r.fetch][gi].IsZero() && f.groups[gi].cluster != "" && f.coveredByOthers(gi, pending[r.fetch]) {
					cutAt[r.fetch][gi] = time.Now().Add(policy.Grace)
					changed = true
				}
			}
			if changed {
				arm()
			}
		case <-expired:
			now := time.Now()
			for _, fi := range watched {
				f := &fetches[fi]
				for gi := range f.groups {
					n := pending[fi][gi]
					if n == 0 || cutAt[fi][gi].IsZero() || cutAt[fi][gi].After(now) {
						continue
					}
					g := &f.groups[gi]
					for range n {
						g.errs = append(g.errs, errSlowReplica)
					}
					pending[fi][gi] = 0
					outstanding -= n
					if policy.Cancelled != nil {
						policy.Cancelled.Add(float64(n))
					}
					lg.Debug("render stopped waiting for a slow replica group: every other group answered, one with data",
						zap.Strings("targets", f.request.Targets),
						zap.String("cluster", g.cluster),
						zap.Int("fetches", n),
						zap.Duration("grace", policy.Grace))
				}
			}
			timer = nil
			arm()
		}
	}
	// Keep the ordinary fan-in's message order: by group, then by arrival.
	for _, fi := range watched {
		for _, msgs := range byGroup[fi] {
			fetches[fi].messages = append(fetches[fi].messages, msgs...)
		}
	}
}
