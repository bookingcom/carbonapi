package tldcache

import (
	"reflect"
	"testing"

	"go.uber.org/zap"
)

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
		outPrefixes []TopLevelDomainPrefix
	}{
		{
			prefixes: []string{"a.b", "", "a.b.c", "a"},
			outPrefixes: []TopLevelDomainPrefix{
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
			outPrefixes: []TopLevelDomainPrefix{
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
			outPrefixes: []TopLevelDomainPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
			},
		},
		{
			prefixes: []string{""},
			outPrefixes: []TopLevelDomainPrefix{
				{
					prefix:        "",
					segments:      nil,
					segmentsCount: 0,
				},
			},
		},
		{
			prefixes: []string{"a", "a"},
			outPrefixes: []TopLevelDomainPrefix{
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
			outPrefixes: []TopLevelDomainPrefix{
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
