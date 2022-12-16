package carbonapi

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/tldcache"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

// Find executes find request by checking cache and sending it to the backends.
func Find(cache *expirecache.Cache, TLDPrefixes []tldcache.TopLevelDomainPrefix, backends []backend.Backend, ctx context.Context,
	originalQuery string, ms *ZipperPrometheusMetrics, lg *zap.Logger) (types.Matches, error) {
	request := types.NewFindRequest(originalQuery)
	bs := tldcache.FilterBackendByTopLevelDomain(cache, TLDPrefixes, backends, []string{originalQuery})
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, []string{originalQuery})
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	metrics, errs := backend.Finds(ctx, bs, request, ms.FindOutDuration)
	err := errorsFanIn(errs, len(bs))

	if err != nil {
		var notFound types.ErrNotFound
		if !errors.As(err, &notFound) {
			return metrics, err
		}
	}

	sort.Slice(metrics.Matches, func(i, j int) bool {
		if metrics.Matches[i].Path < metrics.Matches[j].Path {
			return true
		}
		if metrics.Matches[i].Path > metrics.Matches[j].Path {
			return false
		}
		return metrics.Matches[i].Path < metrics.Matches[j].Path
	})

	return metrics, nil
}

// Render executes the render request by checking cache and sending it to the backends.
func Render(cache *expirecache.Cache, TLDPrefixes []tldcache.TopLevelDomainPrefix, backends []backend.Backend, mismatchConfig cfg.RenderReplicaMismatchConfig, ctx context.Context, target string, from int64, until int64,
	ms *ZipperPrometheusMetrics, lg *zap.Logger) ([]types.Metric, error) {

	request := types.NewRenderRequest([]string{target}, int32(from), int32(until))
	bs := tldcache.FilterBackendByTopLevelDomain(cache, TLDPrefixes, backends, request.Targets)
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, request.Targets)
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	metrics, stats, errs := backend.Renders(ctx, bs, request, mismatchConfig, lg)
	ms.Renders.Add(float64(stats.DataPointCount))
	ms.RenderMismatches.Add(float64(stats.MismatchCount))
	ms.RenderFixedMismatches.Add(float64(stats.FixedMismatchCount))
	err := errorsFanIn(errs, len(bs))
	if err != nil {
		return metrics, err
	}

	if stats.MismatchCount > stats.FixedMismatchCount {
		ms.RenderMismatchedResponses.Inc()
	}

	return metrics, err
}

// Info executes the info request by checking cache and sending it to the backends.
func Info(cache *expirecache.Cache, TLDPrefixes []tldcache.TopLevelDomainPrefix, backends []backend.Backend, ctx context.Context, target string, ms *ZipperPrometheusMetrics, lg *zap.Logger) ([]types.Info, error) {
	request := types.NewInfoRequest(target)

	bs := tldcache.FilterBackendByTopLevelDomain(cache, TLDPrefixes, backends, []string{target})
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, []string{target})
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}

	infos, errs := backend.Infos(ctx, bs, request)
	err := errorsFanIn(errs, len(bs))

	return infos, err
}

func errorsFanIn(errs []error, nBackends int) error {
	nErrs := len(errs)
	var counts = make(map[string]int)
	switch {
	case (nErrs == 0):
		return nil
	case (nErrs < nBackends):
		return nil
	case (nErrs > nBackends):
		return errors.New("got more errors than there are backends. Probably something is broken")
	default:
		// everything failed, nErrs == nBackends
		nNotNotFounds := 0
		for _, e := range errs {
			counts[e.Error()] += 1
			if _, ok := e.(types.ErrNotFound); !ok {
				nNotNotFounds += 1
			}
		}

		nMajority := (nBackends + 1) / 2

		if nNotNotFounds < nMajority {
			return types.ErrNotFound(fmt.Sprintf(
				"majority of backends returned not found. %d total errors, %d not found",
				nErrs, nErrs-nNotNotFounds))
		}
		message := fmt.Sprintf("all backends failed with mixed errors: %+v", counts)
		if len(message) > 300 {
			message = message[:300]
		}
		return errors.New(message)
	}
}
