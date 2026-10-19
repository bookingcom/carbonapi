package net

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// SyncTransport runs the backend's bodyless HTTP reads in the requesting
// goroutine. Each checked-out connection has one owner through Body.Close;
// unlike http.Transport it needs no permanent reader/writer goroutines.
// TLS, proxying and other HTTP methods retain the standard transport.
type SyncTransport struct {
	fallback *http.Transport
	pools    sync.Map // address -> *syncConnPool
	closed   atomic.Bool
	done     chan struct{}
}

type syncConnPool struct {
	mu   sync.Mutex
	idle []*syncConn
}

type syncConn struct {
	net.Conn
	r         *bufio.Reader
	w         *bufio.Writer
	idleSince time.Time
}

func NewSyncTransport(fallback *http.Transport) *SyncTransport {
	t := &SyncTransport{fallback: fallback, done: make(chan struct{})}
	if fallback.IdleConnTimeout > 0 {
		go func() {
			ticker := time.NewTicker(max(10*time.Millisecond, fallback.IdleConnTimeout/2))
			defer ticker.Stop()
			for {
				select {
				case <-t.done:
					return
				case <-ticker.C:
					t.pools.Range(func(_, value any) bool {
						p := value.(*syncConnPool)
						p.mu.Lock()
						keep := p.idle[:0]
						for _, c := range p.idle {
							if time.Since(c.idleSince) >= fallback.IdleConnTimeout {
								c.Close()
							} else {
								keep = append(keep, c)
							}
						}
						clear(p.idle[len(keep):])
						p.idle = keep
						p.mu.Unlock()
						return true
					})
				}
			}
		}()
	}
	return t
}

func (t *SyncTransport) take(ctx context.Context, addr string, p *syncConnPool) (*syncConn, bool, error) {
	p.mu.Lock()
	for len(p.idle) != 0 {
		n := len(p.idle) - 1
		c := p.idle[n]
		p.idle[n] = nil
		p.idle = p.idle[:n]
		if t.fallback.IdleConnTimeout > 0 && time.Since(c.idleSince) >= t.fallback.IdleConnTimeout {
			c.Close()
			continue
		}
		p.mu.Unlock()
		return c, true, nil
	}
	p.mu.Unlock()
	dial := t.fallback.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	c, err := dial(ctx, "tcp", addr)
	if err != nil {
		return nil, false, err
	}
	return &syncConn{Conn: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}, false, nil
}

func (t *SyncTransport) put(c *syncConn, p *syncConnPool) {
	limit := t.fallback.MaxIdleConnsPerHost
	if limit == 0 {
		limit = http.DefaultMaxIdleConnsPerHost
	}
	if limit < 0 || t.fallback.DisableKeepAlives || t.closed.Load() {
		c.Close()
		return
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		c.Close()
		return
	}
	c.idleSince = time.Now()
	p.mu.Lock()
	if len(p.idle) < limit {
		p.idle = append(p.idle, c)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	c.Close()
}

func (t *SyncTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "http" || req.Method != http.MethodGet || req.Body != nil || t.fallback.Proxy != nil {
		return t.fallback.RoundTrip(req)
	}
	addr := req.URL.Host
	if req.URL.Port() == "" {
		addr = net.JoinHostPort(req.URL.Hostname(), "80")
	}
	value, _ := t.pools.LoadOrStore(addr, &syncConnPool{})
	p := value.(*syncConnPool)
	for attempt := 0; ; attempt++ {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		c, reused, err := t.take(req.Context(), addr, p)
		if err != nil {
			return nil, err
		}
		deadline, _ := req.Context().Deadline()
		if err := c.SetDeadline(deadline); err != nil {
			c.Close()
			return nil, err
		}
		// A cancelled request must never close a connection after it is reused.
		stop := context.AfterFunc(req.Context(), func() { c.Close() })
		err = req.Write(c.w)
		if err == nil {
			err = c.w.Flush()
		}
		var response *http.Response
		if err == nil {
			for {
				response, err = http.ReadResponse(c.r, req)
				if err != nil || response.StatusCode >= 200 || response.StatusCode == 101 {
					break
				}
				response.Body.Close()
			}
		}
		if err == nil {
			response.Body = &syncResponseBody{ReadCloser: response.Body, transport: t,
				conn: c, pool: p, stop: stop, reusable: !response.Close && !req.Close && response.StatusCode != 101}
			return response, nil
		}
		stop()
		c.Close()
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return nil, ctxErr
		}
		// An idle peer can close between checkout and the first read. A GET
		// without a body is replayable, matching the standard transport.
		if !reused || attempt != 0 {
			return nil, err
		}
	}
}

type syncResponseBody struct {
	io.ReadCloser
	transport *SyncTransport
	conn      *syncConn
	pool      *syncConnPool
	stop      func() bool
	reusable  bool
	eof       atomic.Bool
	failed    atomic.Bool
	closed    atomic.Bool
}

func (b *syncResponseBody) Read(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, net.ErrClosed
	}
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.eof.Store(true)
	}
	if err != nil && err != io.EOF {
		b.failed.Store(true)
	}
	return n, err
}

func (b *syncResponseBody) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Closing an unread response must not block draining it. It cannot be
	// reused, but closing the socket first also unblocks the HTTP body.Close.
	canReuse := b.reusable && b.eof.Load() && !b.failed.Load() && b.stop()
	if !canReuse {
		b.stop()
		b.conn.Close()
	}
	err := b.ReadCloser.Close()
	if canReuse && err == nil {
		b.transport.put(b.conn, b.pool)
	} else {
		b.conn.Close()
	}
	return err
}

func (t *SyncTransport) CloseIdleConnections() {
	t.pools.Range(func(_, value any) bool {
		p := value.(*syncConnPool)
		p.mu.Lock()
		idle := p.idle
		p.idle = nil
		p.mu.Unlock()
		for _, c := range idle {
			c.Close()
		}
		return true
	})
	t.fallback.CloseIdleConnections()
}

func (t *SyncTransport) Close() {
	if t.closed.CompareAndSwap(false, true) {
		close(t.done)
	}
	t.CloseIdleConnections()
}
