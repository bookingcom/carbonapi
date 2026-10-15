package net

import (
	"fmt"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func set(c *pathCache, paths ...string) {
	c.setAll(len(paths), func(i int) string { return paths[i] })
}

func TestPathCacheExpiryIsExact(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := newPathCacheWithClock(120*time.Second, clk.now)

	set(c, "a.b.c")
	clk.advance(120 * time.Second)
	if !c.containsAny([]string{"a.b.c"}) {
		t.Fatal("entry must be valid up to and including its expiry instant")
	}
	clk.advance(time.Nanosecond)
	if c.containsAny([]string{"a.b.c"}) {
		t.Fatal("entry must be invalid right after its expiry instant")
	}
}

func TestPathCacheRefreshAcrossRotation(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := newPathCacheWithClock(100*time.Second, clk.now)

	set(c, "x")
	clk.advance(90 * time.Second)
	set(c, "x") // refreshed: valid until t=1190
	clk.advance(20 * time.Second)
	set(c, "other") // rotates; "x" moves to the previous generation
	clk.advance(75 * time.Second)
	if !c.containsAny([]string{"x"}) {
		t.Fatal("refreshed entry must stay valid for expiry after the refresh")
	}
	clk.advance(10 * time.Second)
	if c.containsAny([]string{"x"}) {
		t.Fatal("entry must expire expiry after its last set")
	}
}

func TestPathCacheIsBounded(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := newPathCacheWithClock(10*time.Second, clk.now)

	// One distinct path per second for ten windows: only about two windows
	// worth may be retained.
	for i := 0; i < 100; i++ {
		set(c, fmt.Sprintf("m.%d", i))
		clk.advance(time.Second)
	}
	if n := c.len(); n > 21 {
		t.Fatalf("cache holds %d entries, want at most two windows (21)", n)
	}
	for i := 90; i < 100; i++ {
		if !c.containsAny([]string{fmt.Sprintf("m.%d", i)}) {
			t.Fatalf("recent entry m.%d missing", i)
		}
	}
	for i := 0; i < 89; i++ {
		if c.containsAny([]string{fmt.Sprintf("m.%d", i)}) {
			t.Fatalf("expired entry m.%d reported present", i)
		}
	}
}

func TestPathCacheIdleGapDropsEverything(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := newPathCacheWithClock(10*time.Second, clk.now)

	set(c, "old.1", "old.2")
	clk.advance(time.Hour)
	set(c, "new")
	if n := c.len(); n != 1 {
		t.Fatalf("after an idle gap only the new entry may remain, have %d", n)
	}
}

func TestPathCacheSetLeaves(t *testing.T) {
	c := newPathCache(time.Minute)
	c.setLeaves([]types.Match{
		{Path: "a.b", IsLeaf: true},
		{Path: "a.c", IsLeaf: false},
	})
	if !c.containsAny([]string{"a.b"}) {
		t.Error("leaf must be cached")
	}
	if c.containsAny([]string{"a.c"}) {
		t.Error("non-leaf must not be cached")
	}
}

// The old cache kept validity as "now <= validUntil" too; check a mix of
// valid, expired and absent paths against a straightforward model.
func TestPathCacheMatchesModel(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	expiry := 7 * time.Second
	c := newPathCacheWithClock(expiry, clk.now)
	model := map[string]time.Time{}

	for step := 0; step < 400; step++ {
		p := fmt.Sprintf("p.%d", (step*7919)%37)
		if step%3 != 0 {
			set(c, p)
			model[p] = clk.t.Add(expiry)
		}
		for i := 0; i < 37; i++ {
			q := fmt.Sprintf("p.%d", i)
			until, ok := model[q]
			want := ok && !until.Before(clk.t)
			if got := c.containsAny([]string{q}); got != want {
				t.Fatalf("step %d path %s: got %v want %v", step, q, got, want)
			}
		}
		clk.advance(time.Duration(step%5) * 900 * time.Millisecond)
	}
}
