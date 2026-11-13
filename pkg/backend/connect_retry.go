package backend

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ConnectRetry keeps render fetches alive across a short backend outage, such
// as a store restart. Only fetches that failed before the backend sent any
// response are repeated; a failure after the backend answered stays final.
type ConnectRetry struct {
	// Window is the total backoff spent on one fetch. Zero disables retries.
	Window time.Duration
	// InitialDelay doubles after every attempt up to MaxDelay.
	InitialDelay, MaxDelay time.Duration

	Retries    prometheus.Counter // repeated fetches
	Recoveries prometheus.Counter // repeated fetches that the backend answered
	// Cancellations counts unreachable fetches given up because another
	// replica group of the same cluster returned data for the target.
	Cancellations prometheus.Counter
}

var connectRetry atomic.Pointer[ConnectRetry]

// SetConnectRetry replaces the process-wide render retry policy.
func SetConnectRetry(c ConnectRetry) {
	if c.InitialDelay <= 0 {
		c.InitialDelay = 250 * time.Millisecond
	}
	if c.MaxDelay < c.InitialDelay {
		c.MaxDelay = max(2*time.Second, c.InitialDelay)
	}
	connectRetry.Store(&c)
}

// unreachableError marks a fetch that never reached a backend response and
// remembers the backend to repeat it against.
type unreachableError struct {
	error
	backend Backend
}

func (e unreachableError) Unwrap() error { return e.error }

func (b *Backend) renderError(err error) error {
	if connectFailure(err) {
		return unreachableError{err, *b}
	}
	return err
}

// connectFailure reports whether err is a refused, reset or timed-out
// connection seen before any response. http.Client.Do wraps exactly those
// failures in *url.Error; body reads and decoding happen after the backend
// answered and never produce one.
func connectFailure(err error) bool {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF)
}

type renderFetch struct {
	request  types.RenderRequest
	groups   []renderGroup
	messages [][]types.Metric
}

// retryUnreachable repeats unreachable fetches that no replica group has
// answered, with one backoff schedule shared by all fetches of a request.
// A fetch is repeated only while no other replica group of its cluster has
// returned data for the target: once one has, the merged result is complete
// without the unreachable store, and waiting for it would only delay the
// response. It returns early when the request deadline cannot fit another
// attempt.
func retryUnreachable(ctx context.Context, fetches []renderFetch, lg *zap.Logger) {
	policy := connectRetry.Load()
	if policy == nil || policy.Window <= 0 {
		return
	}
	cancelCovered(fetches, policy, lg)
	if !anyUnreachable(fetches) {
		return
	}
	start := time.Now()
	delay := policy.InitialDelay
	for attempt := 1; anyUnreachable(fetches); attempt++ {
		wait := min(delay, policy.Window-time.Since(start))
		if wait <= 0 {
			return
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= wait {
			return
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(2*delay, policy.MaxDelay)

		var flights []retryFlight
		for fi := range fetches {
			f := &fetches[fi]
			for gi := range f.groups {
				g := &f.groups[gi]
				if !g.unrecovered() {
					continue
				}
				var sent []unreachableError
				kept := g.errs[:0]
				for _, err := range g.errs {
					u, ok := err.(unreachableError)
					if !ok {
						kept = append(kept, err)
						continue
					}
					lg.Debug("retrying render after backend connect failure",
						zap.String("backend", u.backend.GetServerAddress()),
						zap.Strings("targets", f.request.Targets),
						zap.Int("attempt", attempt),
						zap.Duration("elapsed", time.Since(start)),
						zap.Error(u.error))
					u.backend.SendRender(ctx, f.request, g.results, g.errors)
					sent = append(sent, u)
				}
				clear(g.errs[len(kept):])
				g.errs = kept
				if len(sent) != 0 {
					flights = append(flights, retryFlight{fetch: fi, group: gi, sent: sent})
					if policy.Retries != nil {
						policy.Retries.Add(float64(len(sent)))
					}
				}
			}
		}
		awaitRetries(fetches, flights)
		for _, fl := range flights {
			if fl.answered == 0 {
				continue
			}
			if policy.Recoveries != nil {
				policy.Recoveries.Add(float64(fl.answered))
			}
			lg.Debug("render recovered after backend connect retry",
				zap.Strings("targets", fetches[fl.fetch].request.Targets),
				zap.Int("attempt", attempt),
				zap.Duration("elapsed", time.Since(start)))
		}
		cancelCovered(fetches, policy, lg)
	}
}

// retryFlight is one replica group's repeated fetches within an attempt.
type retryFlight struct {
	fetch, group int
	sent         []unreachableError
	pending      int // replies still awaited
	answered     int
}

type retryReply struct {
	flight  int
	metrics []types.Metric
	err     error
}

// awaitRetries collects the replies of one attempt. As soon as a replica group
// returns data, the same fetch's repeats to other groups of that cluster are no
// longer awaited: they keep their unreachable error for cancelCovered, and
// their late replies are dropped.
func awaitRetries(fetches []renderFetch, flights []retryFlight) {
	outstanding := 0
	for i := range flights {
		flights[i].pending = len(flights[i].sent)
		outstanding += flights[i].pending
	}
	// Buffered for every reply, so abandoned forwarders never block.
	replies := make(chan retryReply, outstanding)
	for i := range flights {
		g := &fetches[flights[i].fetch].groups[flights[i].group]
		go func(flight, n int, results <-chan []types.Metric, errs <-chan error) {
			for range n {
				metrics, err := nextReply(results, errs)
				replies <- retryReply{flight, metrics, err}
			}
		}(i, flights[i].pending, g.results, g.errors)
	}
	for outstanding > 0 {
		r := <-replies
		fl := &flights[r.flight]
		if fl.pending == 0 {
			continue // abandoned
		}
		fl.pending--
		outstanding--
		f := &fetches[fl.fetch]
		g := &f.groups[fl.group]
		var answer bool
		if f.messages, answer = g.take(r.metrics, r.err, f.messages); !answer {
			continue
		}
		fl.answered++
		if !g.data || g.cluster == "" {
			continue
		}
		for j := range flights {
			o := &flights[j]
			if j == r.flight || o.fetch != fl.fetch || o.pending == 0 || f.groups[o.group].cluster != g.cluster {
				continue
			}
			og := &f.groups[o.group]
			for _, u := range o.sent[len(o.sent)-o.pending:] {
				og.errs = append(og.errs, u)
			}
			outstanding -= o.pending
			o.pending = 0
		}
	}
}

// cancelCovered gives up the unreachable fetches of every replica group whose
// target another group of the same cluster has returned data for. Their
// errors become final, so the required-fetch and replica-group rules decide the render exactly as without
// retries. Ungrouped backends are never covered: they never recover each
// other's errors.
func cancelCovered(fetches []renderFetch, policy *ConnectRetry, lg *zap.Logger) {
	for fi := range fetches {
		f := &fetches[fi]
		for gi := range f.groups {
			g := &f.groups[gi]
			if !g.unrecovered() || !f.covered(g.cluster) {
				continue
			}
			n := 0
			for i, err := range g.errs {
				if u, ok := err.(unreachableError); ok {
					g.errs[i] = u.error
					n++
				}
			}
			if n == 0 {
				continue
			}
			if policy.Cancellations != nil {
				policy.Cancellations.Add(float64(n))
			}
			lg.Debug("render connect retry cancelled: another replica group answered",
				zap.Strings("targets", f.request.Targets),
				zap.String("cluster", g.cluster),
				zap.Int("fetches", n))
		}
	}
}

// covered reports whether a replica group of cluster returned data for this
// fetch.
func (f *renderFetch) covered(cluster string) bool {
	if cluster == "" {
		return false
	}
	for gi := range f.groups {
		if f.groups[gi].cluster == cluster && f.groups[gi].data {
			return true
		}
	}
	return false
}

func anyUnreachable(fetches []renderFetch) bool {
	for fi := range fetches {
		for gi := range fetches[fi].groups {
			g := &fetches[fi].groups[gi]
			if !g.unrecovered() {
				continue
			}
			for _, err := range g.errs {
				if _, ok := err.(unreachableError); ok {
					return true
				}
			}
		}
	}
	return false
}
