package backend

import (
	"context"
	"fmt"
	"testing"
)

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
