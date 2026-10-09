package net

import (
	"sync"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// pathCache remembers which metric paths a backend has recently returned, so
// requests can be routed only to the backends that hold the metric.
//
// An entry is valid for expiry after its last set, checked exactly on lookup.
// Entries live in two generations that rotate every expiry: anything still in
// the previous generation at rotation time was set at least expiry ago, so the
// whole generation can be dropped at once. This bounds memory to roughly two
// expiry windows of distinct paths, without scanning or per-entry eviction.
type pathCache struct {
	mu sync.RWMutex

	cur  map[string]int64 // path -> valid-until, unix nanoseconds
	prev map[string]int64

	expiry       time.Duration
	nextRotation int64

	now func() time.Time
}

func newPathCache(expiry time.Duration) *pathCache {
	return newPathCacheWithClock(expiry, time.Now)
}

func newPathCacheWithClock(expiry time.Duration, now func() time.Time) *pathCache {
	return &pathCache{
		cur:          make(map[string]int64),
		prev:         make(map[string]int64),
		expiry:       expiry,
		nextRotation: now().Add(expiry).UnixNano(),
		now:          now,
	}
}

// containsAny reports whether any of the paths was set less than expiry ago.
func (c *pathCache) containsAny(paths []string) bool {
	now := c.now().UnixNano()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, p := range paths {
		if until, ok := c.cur[p]; ok && until >= now {
			return true
		}
		if until, ok := c.prev[p]; ok && until >= now {
			return true
		}
	}

	return false
}

// setAll marks path(0) .. path(n-1) as valid for another expiry.
// It takes the lock once for the whole batch.
func (c *pathCache) setAll(n int, path func(i int) string) {
	if n == 0 {
		return
	}
	now := c.now().UnixNano()
	until := now + int64(c.expiry)

	c.mu.Lock()
	if now >= c.nextRotation {
		c.rotate(now)
	}
	for i := 0; i < n; i++ {
		c.cur[path(i)] = until
	}
	c.mu.Unlock()
}

// setLeaves marks the paths of the leaf matches as valid for another expiry.
func (c *pathCache) setLeaves(matches []types.Match) {
	leaves := 0
	for i := range matches {
		if matches[i].IsLeaf {
			leaves++
		}
	}
	if leaves == 0 {
		return
	}
	now := c.now().UnixNano()
	until := now + int64(c.expiry)

	c.mu.Lock()
	if now >= c.nextRotation {
		c.rotate(now)
	}
	for i := range matches {
		if matches[i].IsLeaf {
			c.cur[matches[i].Path] = until
		}
	}
	c.mu.Unlock()
}

func (c *pathCache) rotate(now int64) {
	// If more than one window passed without a set, the current generation
	// is entirely expired as well.
	if now >= c.nextRotation+int64(c.expiry) {
		c.prev = make(map[string]int64)
	} else {
		c.prev = c.cur
	}
	c.cur = make(map[string]int64, len(c.prev))
	c.nextRotation = now + int64(c.expiry)
}

func (c *pathCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cur) + len(c.prev)
}
