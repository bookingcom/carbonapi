package backend

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpBackend "github.com/go-graphite/carbonapi/pkg/backend/net"
)

func TestScatterGatherEmpty(t *testing.T) {
	resp, err := ScatterGather(context.Background(), nil, nil, nil)
	if err != nil {
		t.Error(err)
	}

	if len(resp) > 0 {
		t.Error("Expected an empty response")
	}
}

func TestScatterGather(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "yo")
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b := httpBackend.New(httpBackend.Config{
		Address: addr,
		Client:  server.Client(),
	})

	resps, err := ScatterGather(context.Background(), []Backend{b}, b.URL("/render"), nil)
	if err != nil {
		t.Error(err)
	}

	if len(resps) != 1 {
		t.Error("Didn't get all responses")
	}

	if !bytes.Equal(resps[0], []byte("yo")) {
		t.Errorf("Didn't get expected response\nGot %v\nExp %v", resps[0], []byte("yo"))
	}
}

func TestScatterGatherTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
	}))
	defer server.Close()

	addr := strings.TrimPrefix(server.URL, "http://")
	b := httpBackend.New(httpBackend.Config{
		Address: addr,
		Client:  server.Client(),
		Timeout: time.Nanosecond,
	})

	_, err := ScatterGather(context.Background(), []Backend{b}, b.URL("/render"), nil)
	if err == nil {
		t.Error("Expected an error")
	}
}

func TestScatterGatherHammer(t *testing.T) {
	N := 10

	backends := make([]Backend, N)
	for i := 0; i < N; i++ {
		j := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%d", j)
		}))
		defer s.Close()

		addr := strings.TrimPrefix(s.URL, "http://")
		b := httpBackend.New(httpBackend.Config{
			Address: addr,
			Client:  s.Client(),
		})
		backends[i] = b
	}

	u := backends[0].URL("/render")
	resps, err := ScatterGather(context.Background(), backends, u, nil)
	if err != nil {
		t.Error(err)
	}

	if len(resps) != N {
		t.Error("Didn't get all responses")
	}

	uniqueBodies := make(map[string]struct{})
	for i := 0; i < N; i++ {
		uniqueBodies[string(resps[i])] = struct{}{}
	}
	if len(uniqueBodies) != N {
		t.Errorf("Expected %d unique responses, got %d:\n%+v", N, len(uniqueBodies), uniqueBodies)
	}
}

func TestScatterGatherHammerOneTimeout(t *testing.T) {
	N := 10

	backends := make([]Backend, 0, N)
	for i := 0; i < N; i++ {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Millisecond)
		}))
		defer s.Close()

		addr := strings.TrimPrefix(s.URL, "http://")
		cfg := httpBackend.Config{
			Address: addr,
			Client:  s.Client(),
		}

		if i == 0 {
			cfg.Timeout = time.Nanosecond
		}

		b := httpBackend.New(cfg)
		backends = append(backends, b)
	}

	u := backends[0].URL("/render")
	resps, err := ScatterGather(context.Background(), backends, u, nil)
	if err != nil {
		t.Error(err)
	}

	if len(resps) != N-1 {
		t.Errorf("Expected %d responses, got %d", N-1, len(resps))
	}
}
