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

// retryUnreachable repeats unreachable fetches that no replica group member
// has recovered, with one backoff schedule shared by all fetches of a request.
// It returns early when the request deadline cannot fit another attempt.
func retryUnreachable(ctx context.Context, fetches []renderFetch, lg *zap.Logger) {
	policy := connectRetry.Load()
	if policy == nil || policy.Window <= 0 || !anyUnreachable(fetches) {
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

		resent := make([][]int, len(fetches))
		for fi := range fetches {
			f := &fetches[fi]
			for gi := range f.groups {
				g := &f.groups[gi]
				if !g.unrecovered() {
					continue
				}
				n := 0
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
					n++
				}
				clear(g.errs[len(kept):])
				g.errs = kept
				if n != 0 {
					if resent[fi] == nil {
						resent[fi] = make([]int, len(f.groups))
					}
					resent[fi][gi] = n
					if policy.Retries != nil {
						policy.Retries.Add(float64(n))
					}
				}
			}
		}
		for fi, counts := range resent {
			for gi, n := range counts {
				if n == 0 {
					continue
				}
				var answered int
				fetches[fi].messages, answered = fetches[fi].groups[gi].receive(n, fetches[fi].messages)
				if answered != 0 {
					if policy.Recoveries != nil {
						policy.Recoveries.Add(float64(answered))
					}
					lg.Debug("render recovered after backend connect retry",
						zap.Strings("targets", fetches[fi].request.Targets),
						zap.Int("attempt", attempt),
						zap.Duration("elapsed", time.Since(start)))
				}
			}
		}
	}
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
