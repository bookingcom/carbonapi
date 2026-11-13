package net

import (
	"context"
	"net"
	"sync"
	"time"
)

// DialFunc dials a network connection, like net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// WarmDialer hands out backend connections that were dialed before the
// process started serving.
//
// A release reload (SIGUSR2, gracehttp re-exec) starts the new process with
// an empty connection pool and moves full traffic onto it at once: every
// render dials, thousands of dials to the stores start within a second, and
// with the 200 ms connect timeout a slow SYN round turns into render 500s.
// The old process keeps serving until the new one calls gracehttp.Serve, so
// dialing ahead of that costs nothing and the new process takes over with a
// warm pool.
type WarmDialer struct {
	dial DialFunc
	ttl  time.Duration

	mu   sync.Mutex
	warm map[string][]warmConn
}

type warmConn struct {
	net.Conn
	dialed time.Time
}

// NewWarmDialer returns a dialer that serves prewarmed connections first and
// falls back to dial. A prewarmed connection older than ttl is never handed
// out, so a backend's idle timeout cannot have closed it.
func NewWarmDialer(dial DialFunc, ttl time.Duration) *WarmDialer {
	return &WarmDialer{dial: dial, ttl: ttl, warm: make(map[string][]warmConn)}
}

// DialContext returns a prewarmed connection to addr if one is left, and
// dials otherwise.
func (d *WarmDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network == "tcp" {
		if c := d.take(addr); c != nil {
			return c, nil
		}
	}
	return d.dial(ctx, network, addr)
}

func (d *WarmDialer) take(addr string) net.Conn {
	d.mu.Lock()
	defer d.mu.Unlock()
	conns := d.warm[addr]
	for len(conns) > 0 {
		c := conns[len(conns)-1]
		conns[len(conns)-1] = warmConn{}
		conns = conns[:len(conns)-1]
		if time.Since(c.dialed) < d.ttl {
			d.warm[addr] = conns
			return c.Conn
		}
		_ = c.Close()
	}
	delete(d.warm, addr)
	return nil
}

// Idle reports how many prewarmed connections are waiting to be handed out.
func (d *WarmDialer) Idle() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, conns := range d.warm {
		n += len(conns)
	}
	return n
}

// PrewarmResult summarises one Prewarm call.
type PrewarmResult struct {
	Dialed, Failed int
	Elapsed        time.Duration
}

// Prewarm dials perAddr connections to every address with dial, at most
// parallel at a time in total, and keeps them for DialContext. Dials go round
// by round over the addresses, so no backend sees a burst of more than a few
// concurrent connects. It returns once every dial finished or ctx is done;
// connections still being dialed then are closed. Connections nobody took
// are closed after the dialer's ttl.
func (d *WarmDialer) Prewarm(ctx context.Context, dial DialFunc, addrs []string, perAddr, parallel int) PrewarmResult {
	start := time.Now()
	var res PrewarmResult
	if perAddr <= 0 || len(addrs) == 0 {
		return res
	}
	parallel = max(parallel, 1)

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, parallel)
	)
dialing:
	for round := 0; round < perAddr; round++ {
		for _, addr := range addrs {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break dialing
			}
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				defer func() { <-sem }()
				c, err := dial(ctx, "tcp", addr)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					res.Failed++
					return
				}
				if ctx.Err() != nil {
					_ = c.Close()
					res.Failed++
					return
				}
				res.Dialed++
				d.mu.Lock()
				d.warm[addr] = append(d.warm[addr], warmConn{Conn: c, dialed: time.Now()})
				d.mu.Unlock()
			}(addr)
		}
	}
	wg.Wait()
	time.AfterFunc(d.ttl, d.expire)
	res.Elapsed = time.Since(start)
	return res
}

// expire closes prewarmed connections that outlived the ttl.
func (d *WarmDialer) expire() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for addr, conns := range d.warm {
		keep := conns[:0]
		for _, c := range conns {
			if time.Since(c.dialed) < d.ttl {
				keep = append(keep, c)
				continue
			}
			_ = c.Close()
		}
		if len(keep) == 0 {
			delete(d.warm, addr)
			continue
		}
		d.warm[addr] = keep
	}
}
