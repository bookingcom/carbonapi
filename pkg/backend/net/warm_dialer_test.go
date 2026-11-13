package net

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// countingListener accepts and holds connections, counting them.
func countingListener(t *testing.T) (string, *atomic.Int64, chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	conns := make(chan net.Conn, 128)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			conns <- c
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		close(conns)
		for c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String(), &n, conns
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWarmDialerHandsOutPrewarmedConnsThenDials(t *testing.T) {
	addr, accepted, _ := countingListener(t)
	base := (&net.Dialer{Timeout: time.Second}).DialContext
	d := NewWarmDialer(base, time.Minute)

	res := d.Prewarm(context.Background(), base, []string{addr}, 3, 2)
	if res.Dialed != 3 || res.Failed != 0 {
		t.Fatalf("prewarm = %+v, want 3 dialed, 0 failed", res)
	}
	waitFor(t, "3 accepted", func() bool { return accepted.Load() == 3 })
	if d.Idle() != 3 {
		t.Fatalf("idle = %d, want 3", d.Idle())
	}

	for i := 0; i < 3; i++ {
		c, err := d.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	if got := accepted.Load(); got != 3 {
		t.Fatalf("prewarmed conns were not reused: %d accepts after 3 dials", got)
	}
	c, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitFor(t, "fallback dial", func() bool { return accepted.Load() == 4 })
	if d.Idle() != 0 {
		t.Fatalf("idle = %d, want 0", d.Idle())
	}
}

func TestWarmDialerExpiresUnusedConns(t *testing.T) {
	addr, accepted, conns := countingListener(t)
	base := (&net.Dialer{Timeout: time.Second}).DialContext
	d := NewWarmDialer(base, 50*time.Millisecond)

	if res := d.Prewarm(context.Background(), base, []string{addr}, 1, 1); res.Dialed != 1 {
		t.Fatalf("prewarm = %+v, want 1 dialed", res)
	}
	server := <-conns
	// The expiry closes the client side; the server then reads EOF.
	if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("server read = %v, want EOF after expiry", err)
	}
	if d.Idle() != 0 {
		t.Fatalf("idle = %d after expiry, want 0", d.Idle())
	}
	// An expired dialer dials afresh.
	c, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitFor(t, "fresh dial", func() bool { return accepted.Load() == 2 })
}

func TestWarmDialerSkipsStaleConn(t *testing.T) {
	addr, accepted, _ := countingListener(t)
	base := (&net.Dialer{Timeout: time.Second}).DialContext
	d := NewWarmDialer(base, time.Hour)
	if res := d.Prewarm(context.Background(), base, []string{addr}, 1, 1); res.Dialed != 1 {
		t.Fatalf("prewarm = %+v", res)
	}
	d.ttl = 0 // the prewarmed conn is now older than the ttl
	c, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitFor(t, "fresh dial instead of stale conn", func() bool { return accepted.Load() == 2 })
}

func TestWarmDialerPrewarmCountsFailuresAndHonoursContext(t *testing.T) {
	refused := func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}
	d := NewWarmDialer(refused, time.Minute)
	if res := d.Prewarm(context.Background(), refused, []string{"a:1", "b:1"}, 2, 4); res.Dialed != 0 || res.Failed != 4 {
		t.Fatalf("prewarm = %+v, want 0 dialed, 4 failed", res)
	}

	block := func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	res := d.Prewarm(ctx, block, []string{"a:1", "b:1", "c:1"}, 100, 2)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("prewarm ignored its context: took %v", elapsed)
	}
	if res.Dialed != 0 || d.Idle() != 0 {
		t.Fatalf("prewarm = %+v, idle %d; want nothing kept", res, d.Idle())
	}

	if res := d.Prewarm(context.Background(), refused, []string{"a:1"}, 0, 4); res != (PrewarmResult{}) {
		t.Fatalf("disabled prewarm = %+v, want zero", res)
	}
}
