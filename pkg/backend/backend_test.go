package backend

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDoLimiterTimeout(t *testing.T) {
	b := New(Config{
		Address: "localhost",
		Limit:   1,
	})

	if err := b.enter(context.Background()); err != nil {
		t.Error("Expected to enter limiter")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	req, err := b.request(ctx, "render", nil)
	if err != nil {
		t.Error(err)
	}

	_, err = b.do(ctx, req)
	if err == nil {
		t.Error("Expected to time out")
	}
}

func TestDo(t *testing.T) {
	exp := []byte("OK")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(exp)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b := New(Config{
		Address: addr,
		Client:  server.Client(),
	})

	req, err := b.request(context.Background(), "render", nil)
	if err != nil {
		t.Error(err)
	}

	resp, err := b.do(context.Background(), req)
	if err != nil {
		t.Error(err)
	}

	got, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}
	resp.Body.Close()

	if !bytes.Equal(got, exp) {
		t.Errorf("Bad response body\nExp %v\nGot %v", exp, resp.Body)
	}
}

func TestDoHTTPTimeout(t *testing.T) {
	d := time.Nanosecond
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * d)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b := New(Config{
		Address: addr,
		Client:  server.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	req, err := b.request(ctx, "render", nil)
	if err != nil {
		t.Error(err)
	}

	resp, err := b.do(ctx, req)
	if err == nil {
		t.Errorf("Expected error, got status code %d", resp.StatusCode)
	}
}
func TestDoHTTPError(t *testing.T) {
	exp := 500

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad", exp)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b := New(Config{
		Address: addr,
		Client:  server.Client(),
	})

	req, err := b.request(context.Background(), "render", nil)
	if err != nil {
		t.Error(err)
	}

	resp, err := b.do(context.Background(), req)
	if err == nil {
		t.Errorf("Expected error, got status code %d", resp.StatusCode)
	}

	if got := resp.StatusCode; got != exp {
		t.Errorf("Expected status code %d, got %d", exp, got)
	}
}

func TestRequest(t *testing.T) {
	b := New(Config{Address: "localhost"})

	_, err := b.request(context.Background(), "render", nil)
	if err != nil {
		t.Error(err)
	}
}

func TestEnterNilLimiter(t *testing.T) {
	b := New(Config{})

	ctx, _ := context.WithTimeout(context.Background(), 0)
	if got := b.enter(ctx); got != nil {
		t.Error("Expected to enter limiter")
	}
}

func TestEnterLimiter(t *testing.T) {
	b := New(Config{Limit: 1})

	if got := b.enter(context.Background()); got != nil {
		t.Error("Expected to enter limiter")
	}
}

func TestEnterLimiterTimeout(t *testing.T) {
	b := New(Config{Limit: 1})

	if err := b.enter(context.Background()); err != nil {
		t.Error("Expected to enter limiter")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	if got := b.enter(ctx); got == nil {
		t.Error("Expected to time out")
	}
}

func TestExitNilLimiter(t *testing.T) {
	b := New(Config{})

	if err := b.leave(); err != nil {
		t.Error("Expected to leave limiter")
	}
}

func TestEnterExitLimiter(t *testing.T) {
	b := New(Config{Limit: 1})

	if err := b.enter(context.Background()); err != nil {
		t.Error("Expected to enter limiter")
	}

	if err := b.leave(); err != nil {
		t.Error("Expected to leave limiter")
	}
}

func TestEnterExitLimiterError(t *testing.T) {
	b := New(Config{Limit: 1})

	if err := b.leave(); err == nil {
		t.Error("Expected to get error")
	}
}

func TestURL(t *testing.T) {
	b := New(Config{Address: "localhost:8080"})

	type setup struct {
		endpoint string
		expected string
	}

	setups := []setup{
		setup{
			endpoint: "render",
			expected: "http://localhost:8080/render",
		},
		setup{
			endpoint: "/render",
			expected: "http://localhost:8080/render",
		},
		setup{
			endpoint: "render/",
			expected: "http://localhost:8080/render/",
		},
		setup{
			endpoint: "/render/",
			expected: "http://localhost:8080/render/",
		},
		setup{
			endpoint: "/render?target=foo",
			expected: "http://localhost:8080/render?target=foo",
		},
		setup{
			endpoint: "/render/?target=foo",
			expected: "http://localhost:8080/render/?target=foo",
		},
	}

	for i, s := range setups {
		t.Run(fmt.Sprintf("%d: %s", i, s.endpoint), func(t *testing.T) {
			if got := b.url(s.endpoint); got != s.expected {
				t.Errorf("Bad url\nGot %s\nExp %s", got, s.expected)
			}
		})
	}
}
