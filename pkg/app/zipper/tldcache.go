package zipper

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/pkg/errors"
)

func (app *App) probeTopLevelDomains(ms *PrometheusMetrics) {
	probeTicker := time.NewTicker(time.Duration(app.Config.InternalRoutingCache) * time.Second) // TODO: The ticker resources are never freed
	for {
		topLevelDomainCache := make(map[string][]*backend.Backend)
		ms.TLDCacheProbeReqTotal.Add(float64(len(app.Backends)))
		for i := 0; i < len(app.Backends); i++ {
			topLevelDomains, err := getTopLevelDomains(app.Backends[i])
			if err != nil {
				// this could add a lot of noise to logs
				// lg.Error("failed to probe TLD cache for a backend", zap.Error(err), zap.String("backend", app.Backends[i].GetServerAddress()))
				ms.TLDCacheProbeErrors.Inc()
			}
			for _, topLevelDomain := range topLevelDomains {
				topLevelDomainCache[topLevelDomain] = append(topLevelDomainCache[topLevelDomain], &app.Backends[i])
			}
		}
		for tld, num := range topLevelDomainCache {
			ms.TLDCacheHostsPerDomain.WithLabelValues(tld).Set(float64(len(num)))
		}
		app.TopLevelDomainCache.Set("tlds", topLevelDomainCache, 0, 2*app.Config.InternalRoutingCache)

		<-probeTicker.C
	}
}

// Returns the backend's top-level domains.
func getTopLevelDomains(backend backend.Backend) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	request := types.NewFindRequest("*")
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
