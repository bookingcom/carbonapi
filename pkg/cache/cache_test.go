package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type TestMemcache struct {
	delayMs uint32
	data    map[string]([]byte)
	errKeys map[string]bool
}

func (m *TestMemcache) Get(k string) (res *memcache.Item, err error) {
	time.Sleep(time.Duration(m.delayMs) * time.Millisecond)
	if _, there := m.errKeys[k]; there {
		return nil, errors.New("cache test err")
	}
	if _, there := m.data[k]; there {
		return &memcache.Item{
			Key:   k,
			Value: m.data[k],
		}, nil
	} else {
		return nil, memcache.ErrCacheMiss
	}
}

func (m *TestMemcache) SetErr(key string) {
	m.errKeys[key] = true
}

func (m *TestMemcache) Set(i *memcache.Item) error {
	if m.data == nil {
		m.data = map[string]([]byte){}
	}
	m.data[i.Key] = i.Value
	return nil
}

func TestGetFromReplica(t *testing.T) {
	aData := []byte("aval")

	m := &TestMemcache{
		delayMs: 1,
		data: map[string][]byte{
			getCacheHashKey("a"): aData,
		},
		errKeys: map[string]bool{
			getCacheHashKey("c"): true,
		},
	}

	resCh := make(chan cacheResponse, 1)
	getFromReplica(m, "a", "", resCh)
	res := <-resCh
	if res.err != nil {
		t.Fatalf("unexpected error occured %v", res.err)
	}
	if !res.found {
		t.Fatal("unexpected not found returned")
	}
	if string(res.data) != string(aData) {
		t.Fatalf("unequal data returned got %v, expected %v", string(res.data), string(aData))
	}

	getFromReplica(m, "b", "", resCh)
	res = <-resCh
	if res.err != nil {
		t.Fatalf("unexpected error occured %v", res.err)
	}
	if res.found {
		t.Fatal("unexpected found returned")
	}

	getFromReplica(m, "c", "", resCh)
	res = <-resCh
	if res.err == nil {
		t.Fatal("unexpected nil error")
	}
}
