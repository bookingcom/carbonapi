package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dgryski/go-expirecache"
)

var (
	ErrTimeout  = errors.New("cache: timeout")
	ErrNotFound = errors.New("cache: not found")
)

type BytesCache interface {
	Get(k string) ([]byte, error)
	Set(k string, v []byte, expire int32) error
}

type NullCache struct{}

func (NullCache) Get(string) ([]byte, error)      { return nil, ErrNotFound }
func (NullCache) Set(string, []byte, int32) error { return nil }

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

func (ec ExpireCache) Set(k string, v []byte, expire int32) error {
	ec.ec.Set(k, v, uint64(len(v)), expire)
	return nil
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
	hk := getCacheHashKey(k)
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

func (m *MemcachedCache) Set(k string, v []byte, expire int32) error {
	hk := getCacheHashKey(k)
	return m.client.Set(&memcache.Item{Key: m.prefix + hk, Value: v, Expiration: expire})
}

func (m *MemcachedCache) Timeouts() uint64 {
	return atomic.LoadUint64(&m.timeouts)
}

// ReplicatedMemcached represents the caching setup when all the memcached instances
// are identical. Each read and write refers to all of them.
type ReplicatedMemcached struct {
	prefix    string
	instances []*memcache.Client

	timeoutMs uint64

	reqCount      *prometheus.CounterVec
	respReadCount *prometheus.CounterVec
	timeoutCount  prometheus.Counter
}

// Cache is a cache interface. Mainly for testing abilities.
type Cache interface {
	Get(string) (*memcache.Item, error)
	Set(*memcache.Item) error
}

// NewReplicatedMemcached creates a set of identical memcached instances.
func NewReplicatedMemcached(prefix string, timeout uint64, reqCount *prometheus.CounterVec,
	respCount *prometheus.CounterVec, timeoutCount prometheus.Counter, servers ...string) BytesCache {
	m := ReplicatedMemcached{
		prefix:        prefix,
		timeoutMs:     timeout,
		reqCount:      reqCount,
		respReadCount: respCount,
		timeoutCount:  timeoutCount,
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
	// chan size is selected so that timeouts do not block getFromReplica goroutines
	resCh := make(chan cacheResponse, len(m.instances))

	for _, replica := range m.instances {
		m.reqCount.With(prometheus.Labels{"operation": "get"}).Inc()
		go getFromReplica(replica, k, m.prefix, resCh)
	}

	tout := time.After(time.Duration(m.timeoutMs) * time.Millisecond)
	var cacheErrs strings.Builder
	notFounds := 0
	timedOut := false
	for range m.instances {
		select {
		case res := <-resCh:
			if res.err != nil {
				m.respReadCount.With(prometheus.Labels{"operation": "get", "status": "error"}).Inc()
				cacheErrs.WriteString("; " + res.err.Error())
				continue
			} else if !res.found {
				m.respReadCount.With(prometheus.Labels{"operation": "get", "status": "not_found"}).Inc()
				notFounds++
				continue
			}

			m.respReadCount.With(prometheus.Labels{"operation": "get", "status": "ok"}).Inc()
			return res.data, nil
		case <-tout:
			m.timeoutCount.Inc()
			cacheErrs.WriteString("; " + ErrTimeout.Error())
			timedOut = true
		}
		if timedOut {
			break
		}
	}
	if notFounds > len(m.instances)/2 {
		return nil, ErrNotFound
	}

	// if this point is reached, it means that majority of caches returned errors
	return nil, errors.New("majority of caches failed with errors: " + cacheErrs.String())
}

// Set sets the key-value pair for all cache instances.
func (rm *ReplicatedMemcached) Set(k string, val []byte, expire int32) error {
	hk := getCacheHashKey(k)
	errCh := make(chan error, len(rm.instances))
	for _, m := range rm.instances {
		rm.reqCount.With(prometheus.Labels{"operation": "set"}).Inc()
		go func(k_ string, val_ []byte, expire_ int32, m_ Cache) {
			err := m_.Set(&memcache.Item{
				Key:        rm.prefix + k_,
				Value:      val_,
				Expiration: expire_,
			})
			errCh <- err
		}(hk, val, expire, m)
	}
	var cacheErrs strings.Builder
	errorFound := false
	for i := 0; i < len(rm.instances); i++ {
		err := <-errCh
		if err != nil {
			rm.respReadCount.With(prometheus.Labels{"operation": "set", "status": "error"}).Inc()
			errorFound = true
			cacheErrs.WriteString("; " + err.Error())
		} else {
			rm.respReadCount.With(prometheus.Labels{"operation": "set", "status": "ok"}).Inc()
		}
	}
	if !errorFound {
		return nil
	}
	return errors.New("caches failed with errors: " + cacheErrs.String())
}

type cacheResponse struct {
	found bool
	data  []byte
	err   error
}

func getFromReplica(m Cache, k string, prefix string, res chan<- cacheResponse) {
	hk := getCacheHashKey(k)
	item, err := m.Get(prefix + hk)

	if err != nil {
		if err == memcache.ErrCacheMiss {
			res <- cacheResponse{found: false, data: nil}
			return
		}
		res <- cacheResponse{found: false, data: nil, err: err}
		return
	}

	res <- cacheResponse{found: true, data: item.Value}
}

func getCacheHashKey(k string) string {
	key := sha256.Sum256([]byte(k))
	hk := hex.EncodeToString(key[:])
	return hk
}
