package zipper

import (
	"context"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
)

func (app *App) doProbe() {
	topLevelDomainCache := make(map[string][]*backend.Backend)
	for i := 0; i < len(app.Backends); i++ {
		topLevelDomains := getTopLevelDomains(app.Backends[i])
		for _, topLevelDomain := range topLevelDomains {
			topLevelDomainCache[topLevelDomain] = append(topLevelDomainCache[topLevelDomain], &app.Backends[i])
		}
	}
	app.TopLevelDomainCache.Set("tlds", topLevelDomainCache, 0, 2*app.Config.InternalRoutingCache)
}

// Returns the backend's top-level domains.
func getTopLevelDomains(backend backend.Backend) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	request := types.NewFindRequest("*")
	matches, err := backend.Find(ctx, request)
	if err != nil {
		return nil
	}
	var paths []string
	for _, m := range matches.Matches {
		paths = append(paths, m.Path)
	}
	return paths
}

func (app *App) probeTopLevelDomains() {
	app.doProbe()
	probeTicker := time.NewTicker(time.Duration(app.Config.InternalRoutingCache) * time.Second)
	for range probeTicker.C {
		app.doProbe()
	}
}
