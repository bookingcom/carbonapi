package net

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncTransportReuseAndFraming(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprint(chunked), func(t *testing.T) {
			var connections atomic.Int64
			payload := strings.Repeat("abcdefgh", 8192)
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Test") != "present" {
					t.Error("missing request header")
				}
				if !chunked {
					w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
				} else {
					w.(http.Flusher).Flush()
				}
				io.WriteString(w, payload)
			}))
			s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			s.Start()
			defer s.Close()
			tr := NewSyncTransport(&http.Transport{MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Second})
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr}
			for i := 0; i < 10; i++ {
				req, _ := http.NewRequest("GET", s.URL, nil)
				req.Header.Set("X-Test", "present")
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || string(body) != payload {
					t.Fatalf("body %d, %v", len(body), err)
				}
			}
			if n := connections.Load(); n != 1 {
				t.Fatalf("reused connections: got %d want 1", n)
			}
		})
	}
}

func TestSyncTransportCancellationAndReuse(t *testing.T) {
	entered := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocked" {
			close(entered)
			<-r.Context().Done()
			return
		}
		io.WriteString(w, "ok")
	}))
	defer s.Close()
	tr := NewSyncTransport(&http.Transport{MaxIdleConnsPerHost: 2})
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", s.URL+"/blocked", nil)
	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel did not fail request")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not unblock read")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(s.URL)
			if err != nil {
				t.Error(err)
				return
			}
			b, e := io.ReadAll(resp.Body)
			resp.Body.Close()
			if e != nil || string(b) != "ok" {
				t.Errorf("body %s, %v", b, e)
			}
		}()
	}
	wg.Wait()
}

func TestSyncTransportUnreadBodyAndStaleConnection(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat("x", 8192)) }))
	defer s.Close()
	tr := NewSyncTransport(&http.Transport{MaxIdleConnsPerHost: 2})
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: time.Second}
	resp, err := client.Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for i := 0; i < 4; i++ {
		resp, err = client.Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		s.CloseClientConnections()
	}
}

func TestSyncTransportCloseInterruptsBodyRead(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "200")
		io.WriteString(w, "x")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer s.Close()
	tr := NewSyncTransport(&http.Transport{})
	defer tr.Close()
	client := &http.Client{Transport: tr}
	resp, err := client.Get(s.URL) //nolint:bodyclose // Closed asynchronously below; a deferred close covers failures.
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 1)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, resp.Body); read <- err }()
	closed := make(chan error, 1)
	go func() { closed <- resp.Body.Close() }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("body close blocked")
	}
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("truncated response reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("body close did not unblock read")
	}
}

func BenchmarkSyncTransport(b *testing.B) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "small backend response") }))
	defer s.Close()
	for _, kind := range []string{"standard", "synchronous"} {
		b.Run(kind, func(b *testing.B) {
			base := &http.Transport{MaxIdleConnsPerHost: 64}
			var tr http.RoundTripper = base
			if kind == "synchronous" {
				tr = NewSyncTransport(base)
			}
			client := &http.Client{Transport: tr}
			defer client.CloseIdleConnections()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					resp, err := client.Get(s.URL)
					if err != nil {
						b.Error(err)
						return
					}
					_, err = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
