package cache

import (
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/google/go-cmp/cmp"
)

type TestMemcache struct {
	delayMs uint32
	data    map[string]([]byte)
}

func (m *TestMemcache) Get(k string) (res *memcache.Item, err error) {
	time.Sleep(time.Duration(m.delayMs) * time.Millisecond)
	if _, there := m.data[k]; there {
		return &memcache.Item{
			Key:   k,
			Value: ([]byte)(m.data[k]),
		}, nil
	} else {
		return nil, memcache.ErrCacheMiss
	}
}

func (m *TestMemcache) Set(i *memcache.Item) error {
	if m.data == nil {
		m.data = map[string]([]byte){}
	}
	m.data[i.Key] = i.Value
	return nil
}

func TestReplicatedMemcacheWithPartialTimeout(t *testing.T) {
	m := ReplicatedMemcached{
		prefix:    "test",
		timeoutMs: 20,
		instances: []Cache{
			&TestMemcache{
				delayMs: 1,
			},
			&TestMemcache{
				delayMs: 10,
			},
			&TestMemcache{
				// this one will timeout every time
				delayMs: 100,
			},
		},
	}

	aData := []byte("aval")
	bData := []byte("bval")
	m.Set("a", aData, 0)
	m.Set("b", bData, 0)

	aRes, err := m.Get("a")
	if !cmp.Equal(aRes, aData) {
		t.Fatalf("Expected %v value for key %s, got %v", aData, "a", aRes)
	}
	if err != nil {
		t.Fatalf("Error while getting value for key %s; %v", "a", err)
	}

	xRes, err := m.Get("x")
	if err != ErrNotFound {
		t.Fatalf("Expected cache miss, that did not happen. Got %v and err %v instead", xRes, err)
	}
}

func TestReplicatedMemcacheTimeout(t *testing.T) {
	m := ReplicatedMemcached{
		prefix:    "test",
		timeoutMs: 10,
		instances: []Cache{
			&TestMemcache{
				delayMs: 250,
			},
			&TestMemcache{
				delayMs: 230,
			},
			&TestMemcache{
				delayMs: 100,
			},
		},
	}

	aData := []byte("aval")
	bData := []byte("bval")
	m.Set("a", aData, 0)
	m.Set("b", bData, 0)

	t0 := time.Now()
	aRes, err := m.Get("a")
	t.Logf("Get took %d ms\n", (time.Now().Sub(t0)).Milliseconds())
	if err != ErrTimeout {
		t.Fatalf("Expected timeout, got val %v, err %v", aRes, err)
	}
}
