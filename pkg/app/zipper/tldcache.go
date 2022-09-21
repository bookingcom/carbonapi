package zipper

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/pkg/errors"
)

func (app *App) probeTopLevelDomains(ms *PrometheusMetrics) {
	probeTicker := time.NewTicker(time.Duration(app.Config.InternalRoutingCache) * time.Second) // TODO: The ticker resources are never freed
	for {
		topLevelDomainCache := make(map[string][]*backend.Backend)
		// We should always have empty prefix to get default TLDs
		allPrefixes := []string{""}
		allPrefixes = append(allPrefixes, app.Config.TLDCacheExtraPrefixes...)
		// Sorting to avoid involving unnecessary backends
		sortedPrefixes := sortedByNsCount(allPrefixes)
		for _, prefix := range sortedPrefixes {
			prefix = trimPrefix(prefix)
			bs := getBackendsForPrefix(prefix, app.Backends, topLevelDomainCache)
			for i := 0; i < len(bs); i++ {
				topLevelDomains, err := getTopLevelDomains(*bs[i], prefix)
				ms.TLDCacheProbeReqTotal.Inc()
				if err != nil {
					// this could add a lot of noise to logs
					// lg.Error("failed to probe TLD cache for a backend", zap.Error(err), zap.String("backend", app.Backends[i].GetServerAddress()))
					ms.TLDCacheProbeErrors.Inc()
				}
				for _, topLevelDomain := range topLevelDomains {
					topLevelDomainCache[topLevelDomain] = append(topLevelDomainCache[topLevelDomain], bs[i])
				}
			}
		}
		for tld, num := range topLevelDomainCache {
			ms.TLDCacheHostsPerDomain.WithLabelValues(tld).Set(float64(len(num)))
		}
		app.TopLevelDomainCache.Set("tlds", topLevelDomainCache, 0, 2*app.Config.InternalRoutingCache)

		<-probeTicker.C
	}
}

type tldPrefix struct {
	prefix  string
	nsCount int
}

func sortedByNsCount(prefixes []string) []string {
	countedPrefixes := make([]tldPrefix, len(prefixes))
	for i, prefix := range prefixes {
		if prefix == "" {
			countedPrefixes[i] = tldPrefix{
				prefix:  prefix,
				nsCount: 0,
			}
		}
		countedPrefixes[i] = tldPrefix{
			prefix:  prefix,
			nsCount: strings.Count(prefix, ".") + 1,
		}
	}
	sort.Slice(countedPrefixes, func(i, j int) bool {
		return countedPrefixes[i].nsCount < countedPrefixes[j].nsCount
	})
	sortedPrefixes := make([]string, len(prefixes))
	for i := range countedPrefixes {
		sortedPrefixes[i] = countedPrefixes[i].prefix
	}
	return sortedPrefixes
}

func getBackendsForPrefix(prefix string, backends []backend.Backend, tldCache map[string][]*backend.Backend) []*backend.Backend {
	var bs []*backend.Backend
	for {
		if prefixBackends, ok := tldCache[prefix]; ok {
			bs = prefixBackends
			break
		}
		lastDotIndex := strings.LastIndex(prefix, ".")
		if lastDotIndex == -1 {
			for i := range backends {
				bs = append(bs, &backends[i])
			}
			break
		}
		prefix = trimPrefix(prefix[:lastDotIndex])
	}
	return bs
}

func trimPrefix(prefix string) string {
	return strings.Trim(prefix, ".*")
}

// Returns the backend's top-level domains.
func getTopLevelDomains(backend backend.Backend, prefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := "*"
	if prefix != "" {
		query = prefix + ".*"
	}
	request := types.NewFindRequest(query)
	matches, err := backend.Find(ctx, request)
	if err != nil {
		return nil, errors.Wrap(err, "find request failed")
	}
	var paths []string
	for _, m := range matches.Matches {
		paths = append(paths, m.Path)
	}
	return paths, nil
}
