package globs

import (
	"github.com/bookingcom/carbonapi/pkg/types"
	"sort"
	"strings"
)

type metricNS struct {
	name  string
	level int
	star  bool
}

type starNS struct {
	metricNS
	uniqueMembers int
}

func explodeMetric(metric string) []metricNS {
	nsNames := strings.Split(metric, ".")
	nss := make([]metricNS, len(nsNames))
	for i, nsName := range nsNames {
		nss[i] = metricNS{
			name:  nsName,
			level: i,
			star:  strings.Contains(nsName, "*"),
		}
	}
	return nss
}

func implodeMetric(nss []metricNS) string {
	var metric string
	for i, ns := range nss {
		metric += ns.name
		if i != len(nss)-1 {
			metric += "."
		}
	}
	return metric
}

func buildBrokenGlobResultsOneStar(starNSs []starNS, maxMatchesPerGlob int, queryMatches [][]metricNS, starIndex int) (finalGlobs []string, limitExceeded bool) {
	newGlobs := make(map[string]int)
	keptNS := make([]metricNS, len(queryMatches))
	for i := range queryMatches {
		keptNS[i] = queryMatches[i][starNSs[starIndex].level]
		queryMatches[i][starNSs[starIndex].level] = starNSs[starIndex].metricNS
		newGlob := implodeMetric(queryMatches[i])
		newGlobs[newGlob]++
	}
	newUniqueGlobs := make([]string, 0, len(newGlobs))
	for newGlob, resultCount := range newGlobs {
		if resultCount >= maxMatchesPerGlob {
			for i := range queryMatches {
				queryMatches[i][starNSs[starIndex].level] = keptNS[i]
			}
			return nil, true
		}
		newUniqueGlobs = append(newUniqueGlobs, newGlob)
	}
	return newUniqueGlobs, false
}

func findBestGlobsGreedy(starNSs []starNS, maxBatchSize int, queryMatches [][]metricNS) []string {
	var finalGlobs []string
	for i := range starNSs {
		gs, limited := buildBrokenGlobResultsOneStar(starNSs, maxBatchSize, queryMatches, i)
		if limited {
			continue
		}
		finalGlobs = gs
	}
	return finalGlobs
}

func GetGreedyBrokenGlobs(query string, glob types.Matches, maxMatchesPerGlob int) ([]string, bool) {
	queryMatches := make([][]metricNS, 0, len(glob.Matches))
	for _, m := range glob.Matches {
		if m.IsLeaf {
			queryMatches = append(queryMatches, explodeMetric(m.Path))
		}
	}
	matchesCount := len(queryMatches)
	if matchesCount < maxMatchesPerGlob {
		return []string{query}, false
	}
	originalExplodedQuery := explodeMetric(query)
	var starNSs []starNS
	for i, originalNS := range originalExplodedQuery {
		if originalNS.star {
			starns := starNS{
				metricNS: originalNS,
			}
			members := make(map[string]int)
			for _, m := range queryMatches {
				matchNS := m[i]
				members[matchNS.name]++
			}
			starns.uniqueMembers = len(members)
			starNSs = append(starNSs, starns)
		}
	}
	sort.Slice(starNSs, func(i, j int) bool {
		return starNSs[i].uniqueMembers > starNSs[j].uniqueMembers
	})
	newUniqueGlobs := findBestGlobsGreedy(starNSs, maxMatchesPerGlob, queryMatches)
	if len(newUniqueGlobs) == 0 || len(newUniqueGlobs) == len(queryMatches) {
		oldMatches := make([]string, 0, len(queryMatches))
		for _, m := range glob.Matches {
			if m.IsLeaf {
				oldMatches = append(oldMatches, m.Path)
			}
		}
		return oldMatches, false
	}
	return newUniqueGlobs, true
}
