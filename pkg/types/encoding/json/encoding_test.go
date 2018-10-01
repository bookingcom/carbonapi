package json

import (
	"testing"

	"github.com/go-graphite/carbonapi/pkg/types"
)

func TestExpandable(t *testing.T) {
	ms := types.Matches{
		Name: "*.*",
		Matches: []types.Match{
			types.Match{
				Path:   "*.sin",
				IsLeaf: true,
			},
		},
	}

	jms := matchesToJSONMatches(ms)

	if len(jms) != 1 {
		t.Error("Expected 1 match")
	}

	jm := jms[0]

	if jm.AllowChildren == 1 {
		t.Error("Expected leaf")
	}

	if jm.Leaf == 0 {
		t.Error("Expected leaf")
	}

	if jm.Expandable == 0 {
		t.Error("Expected expandable")
	}
}
