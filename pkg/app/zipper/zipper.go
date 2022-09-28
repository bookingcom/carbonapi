package zipper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

// Setup sets up the zipper for future lanuch.
// lgOverride optionally overrides logger if non-nil. TODO: Remove after merge is done.
// metricsNS adds namespace to Prom metrics if non-empty. TODO: Remove after merge is done.
func Setup(configFile string, BuildVersion string, metricsNS string, lgOverride *zap.Logger) (*App, *zap.Logger) {
	if configFile == "" {
		log.Fatal("missing config file option")
	}

	fh, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("unable to read config file: %s", err)
	}

	config, err := cfg.ParseZipperConfig(fh)
	if err != nil {
		log.Fatalf("failed to parse config at %s: %s", configFile, err)
	}
	fh.Close()

	if config.MaxProcs != 0 {
		runtime.GOMAXPROCS(config.MaxProcs)
	}

	if len(config.GetBackends()) == 0 {
		log.Fatal("no Backends loaded -- exiting")
	}

	var logger *zap.Logger
	if lgOverride != nil {
		logger = lgOverride.With(zap.Bool("zipper", true))
	} else {
		logger, err = config.LoggerConfig.Build()
		if err != nil {
			log.Fatalf("failed to initiate logger: %s", err)
		}
		logger = logger.Named("carbonzipper")
	}

	logger.Info("starting carbonzipper",
		zap.String("build_version", BuildVersion),
		zap.String("zipperConfig", fmt.Sprintf("%+v", config)),
	)

	ms := NewPrometheusMetrics(config, metricsNS)
	bs, err := InitBackends(config, ms, logger)
	if err != nil {
		logger.Fatal("failed to init backends", zap.Error(err))
	}

	app := &App{
		Config:              config,
		Metrics:             ms,
		Backends:            bs,
		TopLevelDomainCache: expirecache.New(0),
		TLDPrefixes:         InitTLDPrefixes(logger, config.TLDCacheExtraPrefixes),
		Lg: logger,
	}

	return app, logger
}

// Find executes find request by checking cache and sending it to the backends.
func Find(app *App, ctx context.Context, originalQuery string, ms *PrometheusMetrics, lg *zap.Logger) (types.Matches, error) {
	request := types.NewFindRequest(originalQuery)
	bs := app.filterBackendByTopLevelDomain([]string{originalQuery})
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, []string{originalQuery})
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	metrics, errs := backend.Finds(ctx, bs, request, ms.FindOutDuration)
	err := errorsFanIn(errs, len(bs))

	if ctx.Err() != nil {
		// context was cancelled even if some of the requests succeeded
		ms.RequestCancel.WithLabelValues("find", ctx.Err().Error()).Inc()
	}

	if err != nil {
		var notFound types.ErrNotFound
		if errors.As(err, &notFound) {
			// graphite-web 0.9.12 needs to get a 200 OK response with an empty
			// body to be happy with its life, so we can't 404 a /metrics/find
			// request that finds nothing. We are however interested in knowing
			// that we found nothing on the monitoring side, so we claim we
			// returned a 404 code to Prometheus.

			ms.FindNotFound.Inc()
			lg.Info("not found", zap.Error(err))
		} else {
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
func Render(app *App, ctx context.Context, target string, from int64, until int64,
	ms *PrometheusMetrics, lg *zap.Logger) ([]types.Metric, error) {

	request := types.NewRenderRequest([]string{target}, int32(from), int32(until))
	request.Trace.OutDuration = ms.RenderOutDurationExp
	bs := app.filterBackendByTopLevelDomain(request.Targets)
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, request.Targets)
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	metrics, stats, errs := backend.Renders(ctx, bs, request, app.Config.RenderReplicaMismatchConfig, lg)
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
func Info(app *App, ctx context.Context, target string, ms *PrometheusMetrics, lg *zap.Logger) ([]types.Info, error) {
	request := types.NewInfoRequest(target)

	bs := app.filterBackendByTopLevelDomain([]string{target})
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
