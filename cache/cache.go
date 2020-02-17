package cache

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/dgryski/go-expirecache"
)

var (
	ErrTimeout  = errors.New("cache: timeout")
	ErrNotFound = errors.New("cache: not found")
)

type BytesCache interface {
	Get(k string) ([]byte, error)
	Set(k string, v []byte, expire int32)
}

type NullCache struct{}

func (NullCache) Get(string) ([]byte, error) { return nil, ErrNotFound }
func (NullCache) Set(string, []byte, int32)  {}

func NewExpireCache(maxsize uint64) BytesCache {
	ec := expirecache.New(maxsize)
	go ec.ApproximateCleaner(10 * time.Second)
	return &ExpireCache{ec: ec}
}

type ExpireCache struct {
	ec *expirecache.Cache
}

func (ec ExpireCache) Get(k string) ([]byte, error) {
	v, ok := ec.ec.Get(k)

	if !ok {
		return nil, ErrNotFound
	}

	return v.([]byte), nil
}

func (ec ExpireCache) Set(k string, v []byte, expire int32) {
	ec.ec.Set(k, v, uint64(len(v)), expire)
}

func (ec ExpireCache) Items() int { return ec.ec.Items() }

func (ec ExpireCache) Size() uint64 { return ec.ec.Size() }

func NewMemcached(prefix string, timeoutMs uint64, servers ...string) BytesCache {
	return &MemcachedCache{
		prefix:         prefix,
		queryTimeoutMs: timeoutMs,
		client:         memcache.New(servers...),
	}
}

type MemcachedCache struct {
	prefix         string
	client         *memcache.Client
	timeouts       uint64
	queryTimeoutMs uint64
}

func (m *MemcachedCache) Get(k string) ([]byte, error) {
	key := sha1.Sum([]byte(k))
	hk := hex.EncodeToString(key[:])
	done := make(chan bool, 1)

	var err error
	var item *memcache.Item

	go func() {
		item, err = m.client.Get(m.prefix + hk)
		done <- true
	}()

	timeout := time.After(time.Duration(m.queryTimeoutMs) * time.Millisecond)

	select {
	case <-timeout:
		atomic.AddUint64(&m.timeouts, 1)
		return nil, ErrTimeout
	case <-done:
	}

	if err != nil {
		// translate to internal cache miss error
		if err == memcache.ErrCacheMiss {
			err = ErrNotFound
		}
		return nil, err
	}

	if item == nil {
		// in case if memcached client returns (nil, nil)
		return nil, nil
	}
	return item.Value, nil
}

func (m *MemcachedCache) Set(k string, v []byte, expire int32) {
	key := sha1.Sum([]byte(k))
	hk := hex.EncodeToString(key[:])
	go m.client.Set(&memcache.Item{Key: m.prefix + hk, Value: v, Expiration: expire})
}

func (m *MemcachedCache) Timeouts() uint64 {
	return atomic.LoadUint64(&m.timeouts)
}

type ReplicatedMemcached struct {
	prefix    string
	instances []*memcache.Client

	// was 50 ms
	timeoutMs uint64 // timeout for getting data
	// TODO (grzkv) Scrape this metric
	timeouts uint64 // timeouts counter
}

// NewReplicatedMemcached creates a set of identical memcached instances.
func NewReplicatedMemcached(prefix string, timeout uint64, servers ...string) BytesCache {
	m := ReplicatedMemcached{
		prefix:    prefix,
		timeoutMs: timeout,
	}

	for _, s := range servers {
		m.instances = append(m.instances, memcache.New(s))
	}

	return &m
}

// Get gets value for the key from the replicated memcached.
// It sends the request to all replicas and picks the first valid answer
// (event if it's a not-found) or times out.
func (m *ReplicatedMemcached) Get(k string) ([]byte, error) {
	resCh := make(chan cacheResponse)

	tout := time.After(time.Duration(m.timeoutMs) * time.Millisecond)
	cacheErrs := ""
	for i := 0; i < len(m.instances); i++ {
		select {
		case res := <-resCh:
			if res.err != nil {
				cacheErrs = cacheErrs + "; " + res.err.Error()
			} else if !res.found {
				return nil, ErrNotFound
			}

			return res.data, nil
		case <-tout:
			atomic.AddUint64(&m.timeouts, 1)
			return nil, ErrTimeout
		}
	}

	// if this point is reached, it means that all caches returned errors
	return nil, errors.New("all caches failed with errors: " + cacheErrs)
}

// Set sets the key-value pair for all cache instances.
func (rm *ReplicatedMemcached) Set(k string, val []byte, expire int32) {
	key := sha1.Sum([]byte(k))
	hk := hex.EncodeToString(key[:])
	for _, m := range rm.instances {
		go func(k_ string, val_ []byte, expire_ int32, m_ *memcache.Client) {
			m_.Set(&memcache.Item{
				Key:        rm.prefix + k_,
				Value:      val_,
				Expiration: expire,
			})
		}(hk, val, expire, m)
	}
}

type cacheResponse struct {
	found bool
	data  []byte
	err   error
}

func getFromReplica(m *memcache.Client, k string, prefix string, res chan<- cacheResponse, wg *sync.WaitGroup) {
	defer wg.Done()

	key := sha1.Sum([]byte(k))
	hk := hex.EncodeToString(key[:])

	item, err := m.Get(prefix + hk)

	if err != nil {
		if err == memcache.ErrCacheMiss {
			res <- cacheResponse{found: false, data: nil}
			return
		}
		return
	}

	res <- cacheResponse{found: true, data: item.Value}
}
