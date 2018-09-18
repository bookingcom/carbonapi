package backend

import (
	"fmt"
	"testing"
)

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
