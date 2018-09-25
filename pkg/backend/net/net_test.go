package net

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestContains(t *testing.T) {
	b := New(Config{})
	b.tlds = map[string]struct{}{
		"foo": struct{}{},
	}

	if ok := b.Contains([]string{"foo"}); !ok {
		t.Error("Expected true")
	}

	if ok := b.Contains([]string{"foo.bar"}); !ok {
		t.Error("Expected true")
	}

	if ok := b.Contains([]string{"bar"}); ok {
		t.Error("Expected false")
	}

	if ok := b.Contains([]string{"bar", "foo"}); !ok {
		t.Error("Expected true")
	}
}

func TestCall(t *testing.T) {
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

	got, err := b.call(context.Background(), b.url("/render"), nil)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(got, exp) {
		t.Errorf("Bad response body\nExp %v\nGot %v", exp, got)
	}
}

func TestCallServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad", 500)
	}))

	addr := strings.TrimPrefix(server.URL, "http://")
	b := New(Config{
		Address: addr,
		Client:  server.Client(),
	})

	_, err := b.call(context.Background(), b.url("/render"), nil)
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCallTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	addr := strings.TrimPrefix(server.URL, "http://")
	b := New(Config{
		Address: addr,
		Client:  server.Client(),
		Timeout: time.Nanosecond,
	})

	_, err := b.call(context.Background(), b.url("/render"), nil)
	if err == nil {
		t.Error("Expected error")
	}
}

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

	req, err := b.request(ctx, b.url("/render"), nil)
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

	req, err := b.request(context.Background(), b.url("/render"), nil)
	if err != nil {
		t.Error(err)
	}

	got, err := b.do(context.Background(), req)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(got, exp) {
		t.Errorf("Bad response body\nExp %v\nGot %v", exp, got)
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

	req, err := b.request(ctx, b.url("/render"), nil)
	if err != nil {
		t.Error(err)
	}

	_, err = b.do(ctx, req)
	if err == nil {
		t.Errorf("Expected error")
	}
}
func TestDoHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad", 500)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b := New(Config{
		Address: addr,
		Client:  server.Client(),
	})

	req, err := b.request(context.Background(), b.url("/render"), nil)
	if err != nil {
		t.Error(err)
	}

	_, err = b.do(context.Background(), req)
	if err == nil {
		t.Errorf("Expected error")
	}
}

func TestRequest(t *testing.T) {
	b := New(Config{Address: "localhost"})

	_, err := b.request(context.Background(), b.url("/render"), nil)
	if err != nil {
		t.Error(err)
	}
}

func TestEnterNilLimiter(t *testing.T) {
	b := New(Config{})

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

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
		expected *url.URL
	}

	setups := []setup{
		setup{
			endpoint: "/render",
			expected: &url.URL{
				Scheme: "http",
				Host:   "localhost:8080",
				Path:   "/render",
			},
		},
	}

	for i, s := range setups {
		t.Run(fmt.Sprintf("%d: %s", i, s.endpoint), func(t *testing.T) {
			got := b.url(s.endpoint)

			if got.Scheme != s.expected.Scheme ||
				got.Host != s.expected.Host ||
				got.Path != s.expected.Path {
				t.Errorf("Bad url\nGot %s\nExp %s", got, s.expected)
			}
		})
	}
}

func TestCarbonapiv2RenderEncoder(t *testing.T) {
	u := &url.URL{}

	var from int32 = 100
	var until int32 = 200
	metrics := []string{"foo", "bar"}

	gotURL, gotReader := carbonapiV2RenderEncoder(u, from, until, metrics)
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["from"]; len(got) != 1 || got[0] != "100" {
		t.Errorf("Expected from=100, got %v", got)
	}

	if got := vals["until"]; len(got) != 1 || got[0] != "200" {
		t.Errorf("Expected until=200, got %v", got)
	}

	got := vals["target"]
	if len(got) != 2 || got[0] != "foo" || got[1] != "bar" {
		t.Errorf("Bad target: got %v, expected %v", got, metrics)
	}
}

func TestCarbonapiv2InfoEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL, gotReader := carbonapiV2InfoEncoder(u, "foo")
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["target"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}

func TestCarbonapiv2FindEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL, gotReader := carbonapiV2FindEncoder(u, "foo")
	if gotReader != nil {
		t.Error("Expected nil reader")
	}

	vals := gotURL.Query()

	if got := vals["query"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}
