package json

import (
	"reflect"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// TODO (grzkv): These are mostly based on common sense and legacy code. Need to check vs graphite-web
func TestFindMatchesJSONEncoding(t *testing.T) {
	var tests = []struct {
		name string
		in   types.Matches
		out  []jsonMatch
	}{
		{
			name: "simple",
			in: types.Matches{
				Name: "*.*",
				Matches: []types.Match{
					{
						Path:   "*.sin",
						IsLeaf: true,
					},
				},
			},
			out: []jsonMatch{
				{
					AllowChildren: 0,
					Leaf:          1,
					Expandable:    0,
					Context:       make(map[string]int),
					ID:            "*.sin",
					Text:          "sin",
				},
			},
		},
		{
			name: "test with only dups",
			in: types.Matches{
				Name: "a.*.1",
				Matches: []types.Match{
					{
						Path:   "a.*.1",
						IsLeaf: true,
					},
					{
						Path:   "a.*.1",
						IsLeaf: true,
					},
					{
						Path:   "a.*.1",
						IsLeaf: true,
					},
				},
			},
			out: []jsonMatch{
				{
					AllowChildren: 0,
					Leaf:          1,
					Expandable:    0,
					Context:       make(map[string]int),
					ID:            "a.*.1",
					Text:          "1",
				},
			},
		},
		{
			name: "test with various values",
			in: types.Matches{
				Name: "a.b.*",
				Matches: []types.Match{
					{
						Path:   "a.b.e",
						IsLeaf: true,
					},
					{
						Path:   "a.b.c",
						IsLeaf: true,
					},
					{
						Path:   "a.b.d",
						IsLeaf: true,
					},
					{
						Path:   "a.b.d",
						IsLeaf: true,
					},
				},
			},
			out: []jsonMatch{
				{
					AllowChildren: 0,
					Leaf:          1,
					Expandable:    0,
					Context:       make(map[string]int),
					ID:            "a.b.c",
					Text:          "c",
				},
				{
					AllowChildren: 0,
					Leaf:          1,
					Expandable:    0,
					Context:       make(map[string]int),
					ID:            "a.b.d",
					Text:          "d",
				},
				{
					AllowChildren: 0,
					Leaf:          1,
					Expandable:    0,
					Context:       make(map[string]int),
					ID:            "a.b.e",
					Text:          "e",
				},
			},
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			jms := matchesToJSONMatches(tst.in)

			if !reflect.DeepEqual(jms, tst.out) {
				t.Errorf("got %v value for matches, %v expected", jms, tst.out)
			}
		})
	}
}
