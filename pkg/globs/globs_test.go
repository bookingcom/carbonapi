package globs

import (
	"github.com/bookingcom/carbonapi/pkg/types"
	"reflect"
	"testing"
)

func TestGetBrokenGlobs(t *testing.T) {
	var tests = []struct {
		name       string
		metric     string
		glob       types.Matches
		maxBatch   int
		newQueries []string
	}{
		{
			name:     "test1",
			metric:   "a1.*.*.*",
			maxBatch: 5,
			glob: types.Matches{
				Name: "a1.*.*.*",
				Matches: []types.Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.*", "a1.b1.c2.*"},
		},
		{
			name:     "test2",
			metric:   "a1.b*.c*.d*",
			maxBatch: 6,
			glob: types.Matches{
				Name: "a1.b*.c*.d*",
				Matches: []types.Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d*", "a1.b1.c2.d*"},
		},
		{
			name:     "test3",
			metric:   "a1.b*.c*.d*",
			maxBatch: 2,
			glob: types.Matches{
				Name: "a1.b*.c*.d*",
				Matches: []types.Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b1.c1.d2", IsLeaf: true}, {Path: "a1.b1.c1.d3", IsLeaf: true},
					{Path: "a1.b1.c2.d1", IsLeaf: true}, {Path: "a1.b1.c2.d2", IsLeaf: true}, {Path: "a1.b1.c2.d3", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d1", "a1.b1.c1.d2", "a1.b1.c1.d3", "a1.b1.c2.d1", "a1.b1.c2.d2", "a1.b1.c2.d3"},
		},
		{
			name:     "test4",
			metric:   "a1.*.*.*",
			maxBatch: 6,
			glob: types.Matches{
				Name: "a1.*.*.*",
				Matches: []types.Match{
					{Path: "a1.b1.c1.d1", IsLeaf: true}, {Path: "a1.b2.c2.d2", IsLeaf: true}, {Path: "a1.b3.c3.d3", IsLeaf: true},
					{Path: "a1.b4.c4.d4", IsLeaf: true}, {Path: "a1.b5.c5.d5", IsLeaf: true}, {Path: "a1.b6.c6.d6", IsLeaf: true},
				},
			},
			newQueries: []string{"a1.b1.c1.d1", "a1.b2.c2.d2", "a1.b3.c3.d3", "a1.b4.c4.d4", "a5.b5.c5.d5", "a1.b6.c6.d6"},
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			newQueries := GetBrokenGlobs(tst.metric, tst.glob, tst.maxBatch)
			if len(newQueries) != len(tst.newQueries) {
				t.Fatalf("newQueries is different from expected: %+v, %+v", newQueries, tst.newQueries)
			}
		})
	}
}

func TestFindBestStars(t *testing.T) {
	var tests = []struct {
		name         string
		starNSs      []starNS
		maxBatchSize int
		found        []int
		finalProd    int
	}{
		{
			name: "test1",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 1001,
			found:        []int{1, 2},
			finalProd:    1000,
		},
		{
			name: "test2",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 100,
			found:        []int{2},
			finalProd:    50,
		},
		{
			name: "test3",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 5,
			found:        []int{},
			finalProd:    1,
		},
		{
			name: "test4",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 30000,
			found:        []int{0, 2, 3},
			finalProd:    15000,
		},
		{
			name: "test5",
			starNSs: []starNS{
				{uniqueMembers: 10},
				{uniqueMembers: 20},
				{uniqueMembers: 50},
				{uniqueMembers: 30},
			},
			maxBatchSize: 30001,
			found:        []int{1, 2, 3},
			finalProd:    30000,
		},
	}

	for _, tst := range tests {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			var found []int
			finalProd := findBestStars(tst.starNSs, 0, 1, tst.maxBatchSize, &found)
			if !reflect.DeepEqual(found, tst.found) {
				t.Fatalf("found is different from expected: %+v, %+v", found, tst.found)
			}
			if finalProd != tst.finalProd {
				t.Fatalf("finalProd is different from expected: %d, %d", finalProd, tst.finalProd)
			}
		})
	}
}
