package globs

import (
	"github.com/bookingcom/carbonapi/pkg/types"
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

func findBestStars(starNSs []starNS, i, prod, maxBatchSize int, found *[]int) int {
	if prod >= maxBatchSize {
		return 0
	}
	if i == len(starNSs) {
		return prod
	}
	withFound := make([]int, len(*found))
	withoutFound := make([]int, len(*found))
	copy(withFound, *found)
	copy(withoutFound, *found)
	withFound = append(withFound, i) //nolint:makezero
	bestWith := findBestStars(starNSs, i+1, prod*starNSs[i].uniqueMembers, maxBatchSize, &withFound)
	bestWithout := findBestStars(starNSs, i+1, prod, maxBatchSize, &withoutFound)
	if bestWithout > bestWith {
		*found = withoutFound
		return bestWithout
	} else {
		*found = withFound
		return bestWith
	}
}

func GetBrokenGlobs(query string, glob types.Matches, maxMatchesPerGlob int) []string {
	queryMatches := make([][]metricNS, 0, len(glob.Matches))
	for _, m := range glob.Matches {
		if m.IsLeaf {
			queryMatches = append(queryMatches, explodeMetric(m.Path))
		}
	}
	matchesCount := len(queryMatches)
	if matchesCount < maxMatchesPerGlob {
		return []string{query}
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
	var newUniqueGlobs []string

errorLoop:
	for errorRate := 0.0; errorRate <= 0.5; errorRate += 0.1 {
		var foundStars []int
		findBestStars(starNSs, 0, 1, int(float64(maxMatchesPerGlob)*(1.0-errorRate)), &foundStars)
		var chosenNSs []starNS
		for _, index := range foundStars {
			chosenNSs = append(chosenNSs, starNSs[index])
		}
		newGlobs := make(map[string]int)
		for i := range queryMatches {
			newMatch := make([]metricNS, len(queryMatches[i]))
			copy(newMatch, queryMatches[i])
			for _, ns := range chosenNSs {
				newMatch[ns.level] = ns.metricNS
			}
			newGlob := implodeMetric(newMatch)
			newGlobs[newGlob]++
		}
		newUniqueGlobs = make([]string, 0, len(newGlobs))
		for newGlob, resultCount := range newGlobs {
			if resultCount >= maxMatchesPerGlob {
				continue errorLoop
			}
			newUniqueGlobs = append(newUniqueGlobs, newGlob)
		}
		break
	}
	if len(newUniqueGlobs) == len(queryMatches) {
		oldMatches := make([]string, 0, len(queryMatches))
		for _, m := range glob.Matches {
			if m.IsLeaf {
				oldMatches = append(oldMatches, m.Path)
			}
		}
		return oldMatches
	}
	return newUniqueGlobs
}
