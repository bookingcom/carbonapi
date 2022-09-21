package zipper

import (
	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/backend/mock"
	"reflect"
	"testing"
)

func TestGetBackendsForPrefix(t *testing.T) {
	allBackends := []backend.Backend{
		mock.Backend{Address: "0"},
		mock.Backend{Address: "1"},
		mock.Backend{Address: "2"},
		mock.Backend{Address: "3"},
	}
	var tt = []struct {
		prefix      string
		backends    []backend.Backend
		tldCache    map[string][]*backend.Backend
		outBackends []*backend.Backend
	}{
		{
			prefix:      "",
			backends:    allBackends,
			tldCache:    map[string][]*backend.Backend{},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1], &allBackends[2], &allBackends[3]},
		},
		{
			prefix:   "a",
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"b": {&allBackends[0], &allBackends[1]},
				"c": {&allBackends[2], &allBackends[3]},
			},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1], &allBackends[2], &allBackends[3]},
		},
		{
			prefix:   "a",
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"a": {&allBackends[0], &allBackends[1]},
				"c": {&allBackends[2], &allBackends[3]},
			},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1]},
		},
		{
			prefix:   "a.b.c",
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"a": {&allBackends[0], &allBackends[1]},
				"c": {&allBackends[2], &allBackends[3]},
			},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1]},
		},
		{
			prefix:   "a.b.c",
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"a":   {&allBackends[0], &allBackends[1]},
				"c":   {&allBackends[2], &allBackends[3]},
				"a.b": {&allBackends[1]},
			},
			outBackends: []*backend.Backend{&allBackends[1]},
		},
	}

	for _, tst := range tt {
		bs := getBackendsForPrefix(tst.prefix, tst.backends, tst.tldCache)
		if len(bs) != len(tst.outBackends) {
			t.Fatalf("unexpected number of backends: expected %d, got %d", len(tst.outBackends), len(bs))
		}
		for i := range bs {
			if (*bs[i]).GetServerAddress() != (*tst.outBackends[i]).GetServerAddress() {
				t.Fatalf("unexpected backend at position %d: expected backend %s, actual %s",
					i, (*tst.outBackends[i]).GetServerAddress(), (*bs[i]).GetServerAddress())
			}
		}
	}
}

func TestSortedByNsCount(t *testing.T) {
	var tt = []struct {
		prefixes    []string
		outPrefixes []string
	}{
		{
			prefixes:    []string{"a.b", "", "a.b.c", "a"},
			outPrefixes: []string{"", "a", "a.b", "a.b.c"},
		},
		{
			prefixes:    []string{"a.b.c", "a", "a.a.a"},
			outPrefixes: []string{"a", "a.b.c", "a.a.a"},
		},
		{
			prefixes:    []string{""},
			outPrefixes: []string{""},
		},
	}

	for _, tst := range tt {
		res := sortedByNsCount(tst.prefixes)
		if !reflect.DeepEqual(res, tst.outPrefixes) {
			t.Fatalf("unexpected sort: expected %+v, got %+v", tst.outPrefixes, res)
		}
	}
}
