package zipper

import (
	"reflect"
	"testing"

	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/backend/mock"
)

func TestGetBackendsForPrefix(t *testing.T) {
	allBackends := []backend.Backend{
		backend.NewBackend(mock.Backend{Address: "0"}, 0, 0, nil, nil, nil),
		backend.NewBackend(mock.Backend{Address: "1"}, 0, 0, nil, nil, nil),
		backend.NewBackend(mock.Backend{Address: "2"}, 0, 0, nil, nil, nil),
		backend.NewBackend(mock.Backend{Address: "3"}, 0, 0, nil, nil, nil),
	}
	var tt = []struct {
		prefix      tldPrefix
		backends    []backend.Backend
		tldCache    map[string][]*backend.Backend
		outBackends []*backend.Backend
	}{
		{
			prefix:      tldPrefix{"", []string{}, 0},
			backends:    allBackends,
			tldCache:    map[string][]*backend.Backend{},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1], &allBackends[2], &allBackends[3]},
		},
		{
			prefix:   tldPrefix{"a", []string{"a"}, 1},
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"b": {&allBackends[0], &allBackends[1]},
				"c": {&allBackends[2], &allBackends[3]},
			},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1], &allBackends[2], &allBackends[3]},
		},
		{
			prefix:   tldPrefix{"a", []string{"a"}, 1},
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"a": {&allBackends[0], &allBackends[1]},
				"c": {&allBackends[2], &allBackends[3]},
			},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1]},
		},
		{
			prefix:   tldPrefix{"a.b.c", []string{"a", "b", "c"}, 3},
			backends: allBackends,
			tldCache: map[string][]*backend.Backend{
				"a": {&allBackends[0], &allBackends[1]},
				"c": {&allBackends[2], &allBackends[3]},
			},
			outBackends: []*backend.Backend{&allBackends[0], &allBackends[1]},
		},
		{
			prefix:   tldPrefix{"a.b.c", []string{"a", "b", "c"}, 3},
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

func TestGetTargetTopLevelDomain(t *testing.T) {
	var tt = []struct {
		target   string
		prefixes []string
		outTLD   string
	}{
		{
			target:   "a.b.f.g.h",
			prefixes: []string{"a.b", "a.b.c", "v", "b"},
			outTLD:   "a.b.f",
		},
		{
			target:   "a.b.c.g.h",
			prefixes: []string{"a.b", "a.b.c", "v", "b"},
			outTLD:   "a.b.c.g",
		},
		{
			target:   "a.h.f.g.h",
			prefixes: []string{"a.b", "a.b.c", "v", "b"},
			outTLD:   "a",
		},
		{
			target:   "b.h.f.g.h",
			prefixes: []string{"a.b", "a.b.c", "v", "b"},
			outTLD:   "b.h",
		},
	}

	for _, tst := range tt {
		tldPrefixes := InitTLDPrefixes(nil, tst.prefixes)
		tld := getTargetTopLevelDomain(tst.target, tldPrefixes)
		if tld != tst.outTLD {
			t.Fatalf("unexpected tld: expected %+v, got %+v", tst.outTLD, tld)
		}
	}
}

func TestInitTLDPrefixes(t *testing.T) {
	var tt = []struct {
		prefixes    []string
		outPrefixes []tldPrefix
	}{
		{
			prefixes: []string{"a.b", "", "a.b.c", "a"},
			outPrefixes: []tldPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
				{
					prefix:        "a",
					segments:      []string{"a"},
					segmentsCount: 1,
				},
				{
					prefix:        "a.b",
					segments:      []string{"a", "b"},
					segmentsCount: 2,
				},
				{
					prefix:        "a.b.c",
					segments:      []string{"a", "b", "c"},
					segmentsCount: 3,
				},
			},
		},
		{
			prefixes: []string{"a.b.c", "a", "a.a.a"},
			outPrefixes: []tldPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
				{
					prefix:        "a",
					segments:      []string{"a"},
					segmentsCount: 1,
				},
				{
					prefix:        "a.b.c",
					segments:      []string{"a", "b", "c"},
					segmentsCount: 3,
				},
				{
					prefix:        "a.a.a",
					segments:      []string{"a", "a", "a"},
					segmentsCount: 3,
				},
			},
		},
		{
			prefixes: []string{},
			outPrefixes: []tldPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
			},
		},
		{
			prefixes: []string{""},
			outPrefixes: []tldPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
			},
		},
		{
			prefixes: []string{"a", "a"},
			outPrefixes: []tldPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
				{
					prefix:        "a",
					segments:      []string{"a"},
					segmentsCount: 1,
				},
			},
		},
		{
			prefixes: []string{"a", "a..b", ".c.f"},
			outPrefixes: []tldPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
				{
					prefix:        "a",
					segments:      []string{"a"},
					segmentsCount: 1,
				},
			},
		},
	}

	lg := zap.NewNop()
	for _, tst := range tt {
		res := InitTLDPrefixes(lg, tst.prefixes)
		if !reflect.DeepEqual(res, tst.outPrefixes) {
			t.Fatalf("unexpected tld prefix init: expected %+v, got %+v", tst.outPrefixes, res)
		}
	}
}
