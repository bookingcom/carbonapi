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

	"github.com/dgryski/go-expirecache"
)

func TestAddress(t *testing.T) {
	b, err := New(Config{
		Address: "localhost:8080",
	})
	if err != nil {
		t.Error(err)
		return
	}

	exp := "localhost:8080"

	if b.address != "localhost:8080" {
		t.Errorf("Expected %s, got '%s'", exp, b.address)
	}

	if b.scheme != "http" {
		t.Errorf("Expected http scheme, got '%s'", b.scheme)
	}

	b, err = New(Config{
		Address: "https://localhost:8080",
	})
	if err != nil {
		t.Error(err)
		return
	}

	if b.address != "localhost:8080" {
		t.Errorf("Expected %s, got '%s'", exp, b.address)
	}

	if b.scheme != "https" {
		t.Errorf("Expected http scheme, got '%s'", b.scheme)
	}
}

func TestContains(t *testing.T) {
	b, err := New(Config{})
	if err != nil {
		t.Error(err)
		return
	}

	b.cache.Set("foo", struct{}{}, 0, 30)

	if ok := b.Contains([]string{"foo"}); !ok {
		t.Error("Expected true")
	}

	if ok := b.Contains([]string{"foo.bar"}); ok {
		t.Error("Expected false")
	}

	if ok := b.Contains([]string{"bar"}); ok {
		t.Error("Expected false")
	}

	if ok := b.Contains([]string{"bar", "foo"}); !ok {
		t.Error("Expected true")
	}

	if ok := b.Contains([]string{"*"}); ok {
		t.Error("Expected false")
	}

	b.cache = expirecache.New(0)
	if ok := b.Contains([]string{"foo"}); ok {
		t.Error("Expected false")
	}
}

func TestCall(t *testing.T) {
	exp := []byte("OK")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(exp)
	}))
	defer server.Close()

	b, err := New(Config{
		Address: server.URL,
		Client:  server.Client(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, got, err := b.call(context.Background(), b.url("/render"), "render")
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
	b, err := New(Config{
		Address: addr,
		Client:  server.Client(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, _, err = b.call(context.Background(), b.url("/render"), "render")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestCallTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	b, err := New(Config{
		Address: server.URL,
		Client:  server.Client(),
		Timeout: time.Nanosecond,
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, _, err = b.call(context.Background(), b.url("/render"), "render")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestDo(t *testing.T) {
	exp := []byte("OK")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(exp)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b, err := New(Config{
		Address: addr,
		Client:  server.Client(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	req, err := b.request(context.Background(), b.url("/render"))
	if err != nil {
		t.Error(err)
	}

	_, got, err := b.do(req, "")
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
	b, err := New(Config{
		Address: addr,
		Client:  server.Client(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	req, err := b.request(ctx, b.url("/render"))
	if err != nil {
		t.Error(err)
	}

	_, _, err = b.do(req, "")
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
	b, err := New(Config{
		Address: addr,
		Client:  server.Client(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	req, err := b.request(context.Background(), b.url("/render"))
	if err != nil {
		t.Error(err)
	}

	_, _, err = b.do(req, "")
	if err == nil {
		t.Errorf("Expected error")
	}
}

func TestRequest(t *testing.T) {
	b, err := New(Config{Address: "localhost"})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = b.request(context.Background(), b.url("/render"))
	if err != nil {
		t.Error(err)
	}
}

func TestURL(t *testing.T) {
	b, err := New(Config{Address: "localhost:8080"})
	if err != nil {
		t.Error(err)
		return
	}

	type setup struct {
		endpoint string
		expected *url.URL
	}

	setups := []setup{
		{
			endpoint: "/render",
			expected: &url.URL{
				Scheme: "http",
				Host:   "localhost:8080",
				Path:   "/render",
			},
		},
	}

	for i, s := range setups {
		s := s
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

	gotURL := carbonapiV2RenderEncoder(u, from, until, metrics)

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

	gotURL := carbonapiV2InfoEncoder(u, "foo")

	vals := gotURL.Query()

	if got := vals["target"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}

func TestCarbonapiv2FindEncoder(t *testing.T) {
	u := &url.URL{}

	gotURL := carbonapiV2FindEncoder(u, "foo")

	vals := gotURL.Query()

	if got := vals["query"]; len(got) != 1 || got[0] != "foo" {
		t.Errorf("Bad target: got %v", got)
	}

}
