package carbonapi

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"go.uber.org/zap/zapcore"
)

type renderResult struct {
	body      []byte
	status    int
	message   string
	logLevel  zapcore.Level
	partial   bool
	fromCache bool
	details   carbonapipb.AccessLogDetails
}

func (r renderResult) applyLog(log *carbonapipb.AccessLogDetails, leader bool) {
	// Keep each caller's identity, URL, headers and parsed form. Only execution
	// details are shared; followers did not issue any upstream requests.
	log.TotalMetricCount = r.details.TotalMetricCount
	log.Clusters = r.details.Clusters
	log.CarbonapiResponseSizeBytes = int64(len(r.body))
	log.FromCache = r.fromCache
	log.HaveNonFatalErrors = r.details.HaveNonFatalErrors
	if !r.fromCache {
		log.SendGlobs = r.details.SendGlobs
	}
	log.CacheErrs += r.details.CacheErrs
	if leader {
		log.ZipperRequests = r.details.ZipperRequests
		log.CarbonzipperResponseSizeBytes = r.details.CarbonzipperResponseSizeBytes
	}
}

type renderCall struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	result  renderResult
}

// renderFlights only retains active calls, never completed results or errors.
// Its zero value is ready for use. Keys are the unmodified response-cache keys.
type renderFlights struct {
	mu    sync.Mutex
	calls map[string]*renderCall
}

type renderJoin struct {
	call   *renderCall
	leader bool
}

func (g *renderFlights) join(key string, ctx context.Context, timeout time.Duration, run func(context.Context) renderResult) renderJoin {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.calls[key]; c != nil {
		c.waiters++
		return renderJoin{call: c}
	}
	if g.calls == nil {
		g.calls = make(map[string]*renderCall)
	}
	// Every caller, including the leader, waits independently. Keep context
	// values (timezone, tracing, UUID), but give the work its own bounded life.
	workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	c := &renderCall{done: make(chan struct{}), cancel: cancel, waiters: 1}
	g.calls[key] = c
	go func() {
		result := runRender(workCtx, run)
		cancel()
		g.mu.Lock()
		defer g.mu.Unlock()
		c.result = result
		if g.calls[key] == c {
			delete(g.calls, key)
		}
		close(c.done)
	}()
	return renderJoin{call: c, leader: true}
}

func (g *renderFlights) wait(ctx context.Context, key string, c *renderCall) renderResult {
	select {
	case <-c.done:
		return c.result
	case <-ctx.Done():
		g.mu.Lock()
		c.waiters--
		if c.waiters == 0 {
			c.cancel()
			if g.calls[key] == c {
				delete(g.calls, key)
			}
		}
		g.mu.Unlock()
		if ctx.Err() == context.DeadlineExceeded {
			return renderResult{status: http.StatusUnprocessableEntity, message: "request too complex", logLevel: zapcore.ErrorLevel}
		}
		return renderResult{status: 499, message: ctx.Err().Error(), logLevel: zapcore.WarnLevel}
	}
}

// A panic in detached work must release waiters, just as HTTP recovery releases
// an ordinary handler. Expression evaluation already has its own recovery.
func runRender(ctx context.Context, run func(context.Context) renderResult) (result renderResult) {
	defer func() {
		if p := recover(); p != nil {
			result = renderResult{status: http.StatusInternalServerError, message: fmt.Sprintf("panic during render: %v", p), logLevel: zapcore.ErrorLevel}
		}
	}()
	return run(ctx)
}
