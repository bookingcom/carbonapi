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
		if ns.name == "" {
			continue
		}
		metric += ns.name
		if i != len(nss)-1 {
			metric += "."
		}
	}
	return metric
}

func getOmittedStarsMetric(originalNSs []metricNS, queryMatch []metricNS, omittingStars []int) string {
	if len(omittingStars) == 0 {
		return implodeMetric(queryMatch)
	}
	newMatch := make([]metricNS, len(queryMatch))
	j := 0
	for i, ns := range queryMatch {
		if j < len(omittingStars) && i == omittingStars[j] {
			newMatch[i] = metricNS{
				name:  ns.name,
				level: ns.level,
				star:  ns.star,
			}
			j++
		} else {
			newMatch[i] = metricNS{
				name:  originalNSs[i].name,
				level: originalNSs[i].level,
				star:  originalNSs[i].star,
			}
		}
	}
	return implodeMetric(newMatch)
}

func buildBrokenGlobResultsOmittingOneStar(originalNSs []metricNS, maxMatchesPerGlob int, queryMatches [][]metricNS, omittingStars []int) (finalGlobs []string, limitExceeded bool) {
	newGlobs := make(map[string]int)
	for i := range queryMatches {
		newGlob := getOmittedStarsMetric(originalNSs, queryMatches[i], omittingStars)
		newGlobs[newGlob]++
	}
	newUniqueGlobs := make([]string, 0, len(newGlobs))
	limited := false
	for newGlob, resultCount := range newGlobs {
		if resultCount >= maxMatchesPerGlob {
			limited = true
		}
		newUniqueGlobs = append(newUniqueGlobs, newGlob)
	}
	return newUniqueGlobs, limited
}

func findBestGlobsGreedy(originalNSs []metricNS, maxBatchSize int, queryMatches [][]metricNS) []string {
	var finalGlobs []string
	var omittedStars []int
	for i := range originalNSs {
		if !originalNSs[i].star {
			continue
		}
		newOmittedStars := make([]int, len(omittedStars)+1)
		copy(newOmittedStars, omittedStars)
		newOmittedStars[len(omittedStars)] = i
		gs, limited := buildBrokenGlobResultsOmittingOneStar(originalNSs, maxBatchSize, queryMatches, newOmittedStars)
		omittedStars = newOmittedStars
		finalGlobs = gs
		if !limited {
			break
		}
	}
	return finalGlobs
}

func normalizeQueryNamespaces(query string) string {
	return implodeMetric(explodeMetric(query))
}

func GetGreedyBrokenGlobs(query string, glob types.Matches, maxMatchesPerGlob int, maxGlobBrokenQueries int) ([]string, bool) {
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
	query = normalizeQueryNamespaces(query)
	originalExplodedQuery := explodeMetric(query)
	newUniqueGlobs := findBestGlobsGreedy(originalExplodedQuery, maxMatchesPerGlob, queryMatches)
	if len(newUniqueGlobs) == 0 ||
		len(newUniqueGlobs) == len(queryMatches) ||
		// We should have an upper limit for the number of glob queries generated.
		// The reason is that expanding a huge amount of globs is expensive for go-carbon.
		len(newUniqueGlobs) > maxGlobBrokenQueries {
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
