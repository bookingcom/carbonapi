package tldcache

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
)

type addrBackend struct{ addr string }

func (b addrBackend) Find(context.Context, types.FindRequest) (types.Matches, error) {
	return types.Matches{}, nil
}
func (b addrBackend) Info(context.Context, types.InfoRequest) ([]types.Info, error) { return nil, nil }
func (b addrBackend) Render(context.Context, types.RenderRequest) ([]types.Metric, error) {
	return nil, nil
}
func (b addrBackend) Contains([]string) bool                { return false }
func (b addrBackend) Logger() *zap.Logger                   { return zap.NewNop() }
func (b addrBackend) GetServerAddress() string              { return b.addr }
func (b addrBackend) BackendInfo() (string, string, string) { return b.addr, "", "" }

// legacyFilterByTopLevelDomain is the implementation before the allocation
// cleanup, kept to pin the new one to identical output.
func legacyFilterByTopLevelDomain(cache *expirecache.Cache, backends []backend.Backend, targetTLDs []string) []backend.Backend {
	bs := make([]backend.Backend, 0)
	allTLDBackends := make([]*backend.Backend, 0)

	topLevelDomainCache, _ := cache.Get("tlds")
	var tldCache map[string][]*backend.Backend
	if x, ok := topLevelDomainCache.(map[string][]*backend.Backend); ok {
		tldCache = x
	}

	if tldCache == nil {
		return backends
	}
	alreadyAddedBackends := make(map[string]bool)
	for _, target := range targetTLDs {
		tldBackends := tldCache[target]
		for _, backend := range tldBackends {
			a := *backend
			if !alreadyAddedBackends[a.GetServerAddress()] {
				alreadyAddedBackends[a.GetServerAddress()] = true
				allTLDBackends = append(allTLDBackends, backend)
			}
		}
	}
	for _, tldBackend := range allTLDBackends {
		bs = append(bs, *tldBackend)
	}

	return bs
}

func addrsOf(bs []backend.Backend) []string {
	out := make([]string, len(bs))
	for i := range bs {
		out[i] = bs[i].GetServerAddress()
	}
	return out
}

func TestFilterByTopLevelDomainMatchesLegacy(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for iter := 0; iter < 500; iter++ {
		nBackends := 1 + rnd.Intn(80)
		backends := make([]backend.Backend, nBackends)
		for i := range backends {
			// Some backends share an address on purpose.
			backends[i] = backend.Backend{BackendImpl: addrBackend{addr: fmt.Sprintf("10.0.0.%d:8080", rnd.Intn(nBackends))}}
		}
		tlds := []string{"general", "aggregations", "carbon", "general.tuning", "kafka"}
		tldMap := map[string][]*backend.Backend{}
		for _, tld := range tlds {
			for j := rnd.Intn(nBackends + 5); j > 0; j-- {
				// Duplicate pointers are possible too (leaf and branch of the same name).
				tldMap[tld] = append(tldMap[tld], &backends[rnd.Intn(nBackends)])
			}
		}
		cache := expirecache.New(0)
		if rnd.Intn(10) != 0 {
			cache.Set("tlds", tldMap, 0, 600)
		}

		targets := make([]string, rnd.Intn(4))
		for i := range targets {
			targets[i] = append(tlds, "missing")[rnd.Intn(len(tlds)+1)]
		}

		want := addrsOf(legacyFilterByTopLevelDomain(cache, backends, targets))
		got := addrsOf(filterByTopLevelDomain(cache, backends, targets))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("iter %d targets %v:\n got %v\nwant %v", iter, targets, got, want)
		}
	}
}
