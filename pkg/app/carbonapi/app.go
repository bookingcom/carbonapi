package carbonapi

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.uber.org/zap/zapcore"

	"github.com/bookingcom/carbonapi/pkg/backend"
	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/blocker"
	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/expr/functions"
	"github.com/bookingcom/carbonapi/pkg/expr/functions/cairo/png"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/tldcache"
	"github.com/dgryski/go-expirecache"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/facebookgo/pidfile"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// BuildVersion is provided to be overridden at build time. Eg. go build -ldflags -X 'main.BuildVersion=...'
var BuildVersion string

// App is the main carbonapi runnable
type App struct {
	config       cfg.API
	ZipperConfig cfg.Zipper

	queryCache             cache.BytesCache
	findCache              cache.BytesCache
	TopLevelDomainCache    *expirecache.Cache
	TopLevelDomainPrefixes []tldcache.TopLevelDomainPrefix

	requestBlocker *blocker.RequestBlocker

	defaultTimeZone *time.Location

	// During processing we use two independent queues that share a semaphore to prevent stampeding.
	// fastQ includes regular requests
	fastQ chan *RenderReq
	// slowQ contains large requests that could fill the queue and create a stampede.
	slowQ chan *RenderReq

	Backends []backend.Backend

	ms            PrometheusMetrics
	ZipperMetrics *ZipperPrometheusMetrics
}

// New creates a new app
func New(config cfg.API, lg *zap.Logger, buildVersion string) (*App, error) {
	if len(config.Backends) == 0 {
		lg.Fatal("no backends specified for upstreams!")
	}

	BuildVersion = buildVersion
	app := &App{
		config:          config,
		queryCache:      cache.NullCache{},
		findCache:       cache.NullCache{},
		defaultTimeZone: time.Local,
		ms:              newPrometheusMetrics(config),
		requestBlocker:  blocker.NewRequestBlocker(config.BlockHeaderFile, config.BlockHeaderUpdatePeriod, lg),
		fastQ:           make(chan *RenderReq, config.QueueSize),
		slowQ:           make(chan *RenderReq, config.QueueSize),
	}
	app.requestBlocker.ReloadRules()

	setUpConfig(app, lg)

	app.ZipperConfig, app.Backends, app.TopLevelDomainCache, app.TopLevelDomainPrefixes, app.ZipperMetrics = SetupZipper(config.ZipperConfig, BuildVersion, lg)
	go tldcache.ProbeTopLevelDomains(app.TopLevelDomainCache, app.TopLevelDomainPrefixes, app.Backends, app.ZipperConfig.InternalRoutingCache,
		app.ZipperMetrics.TLDCacheProbeReqTotal, app.ZipperMetrics.TLDCacheProbeErrors)

	return app, nil
}

// Start starts the app: inits handlers, logger, starts HTTP server
func (app *App) Start(lg *zap.Logger) {
	registerPrometheusMetrics(&app.ms, app.ZipperMetrics)
	app.ms.Version.WithLabelValues(BuildVersion).Set(1)

	handler := initHandlers(app, lg)
	internalHandler := initHandlersInternal(app, lg)

	app.requestBlocker.ScheduleRuleReload()

	gracehttp.SetLogger(zap.NewStdLog(lg))
	err := gracehttp.Serve(
		&http.Server{
			Addr:         app.config.Listen,
			Handler:      handler,
			ReadTimeout:  time.Second,
			WriteTimeout: app.config.Timeouts.Global * 2, // It has to be greater than Timeout.Global because we use that value as per-request context timeout
		},
		&http.Server{
			Addr:         app.config.ListenInternal,
			Handler:      internalHandler,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Minute, // This long timeout is necessary for profiling.
		},
	)
	if err != nil {
		lg.Fatal("gracehttp failed", zap.Error(err))
	}
}

func setUpConfig(app *App, logger *zap.Logger) {
	for name, color := range app.config.DefaultColors {
		if err := png.SetColor(name, color); err != nil {
			logger.Warn("invalid color specified and will be ignored",
				zap.String("reason", "color must be valid hex rgb or rbga value, e.x. '#c80032', 'c80032', 'c80032ff', etc."),
				zap.Error(err),
			)
		}
	}

	functions.New(app.config.FunctionsConfigs, logger)

	switch app.config.Cache.Type {
	case "memcache":
		if len(app.config.Cache.MemcachedServers) == 0 {
			logger.Fatal("memcache cache requested but no memcache servers provided")
		}

		logger.Info("memcached configured",
			zap.Strings("servers", app.config.Cache.MemcachedServers),
		)

		app.queryCache = cache.NewMemcached(app.config.Cache.Prefix, app.config.Cache.QueryTimeoutMs, app.config.Cache.MemcachedServers...)
		app.findCache = cache.NewMemcached(app.config.Cache.Prefix, app.config.Cache.QueryTimeoutMs, app.config.Cache.MemcachedServers...)

	case "memcacheReplicated":
		if len(app.config.Cache.MemcachedServers) == 0 {
			logger.Fatal("replicated memcache cache requested but no memcache servers provided")
		}
		logger.Info("replicated memcached configured",
			zap.Strings("servers", app.config.Cache.MemcachedServers))

		respReadRender, err := app.ms.CacheRespRead.CurryWith(prometheus.Labels{"request": "render"})
		if err != nil {
			logger.Fatal("could not form respRead metric for the render cache", zap.Error(err))
		}
		reqsRender, err := app.ms.CacheRequests.CurryWith(prometheus.Labels{"request": "render"})
		if err != nil {
			logger.Fatal("could not form reqests counter metric for the render cache", zap.Error(err))
		}
		app.queryCache = cache.NewReplicatedMemcached(app.config.Cache.Prefix,
			app.config.Cache.QueryTimeoutMs,
			app.config.Cache.MemcachedTimeoutMs,
			app.config.Cache.MemcachedMaxIdleConns,
			reqsRender,
			respReadRender,
			app.ms.CacheTimeouts.WithLabelValues("render"),
			app.config.Cache.MemcachedServers...)

		respReadFind, err := app.ms.CacheRespRead.CurryWith(prometheus.Labels{"request": "find"})
		if err != nil {
			logger.Fatal("could not form respRead metrics for the find cache", zap.Error(err))
		}
		reqsFind, err := app.ms.CacheRequests.CurryWith(prometheus.Labels{"request": "find"})
		if err != nil {
			logger.Fatal("could not form reqests counter metric for the find cache", zap.Error(err))
		}
		app.findCache = cache.NewReplicatedMemcached(app.config.Cache.Prefix,
			app.config.Cache.QueryTimeoutMs,
			app.config.Cache.MemcachedTimeoutMs,
			app.config.Cache.MemcachedMaxIdleConns,
			reqsFind,
			respReadFind,
			app.ms.CacheTimeouts.WithLabelValues("find"),
			app.config.Cache.MemcachedServers...)

	case "mem":
		app.queryCache = cache.NewExpireCache(uint64(app.config.Cache.Size * 1024 * 1024))
		app.findCache = cache.NewExpireCache(uint64(app.config.Cache.Size * 1024 * 1024))

	case "null":
		// defaults
		app.queryCache = cache.NullCache{}
		app.findCache = cache.NullCache{}
	default:
		logger.Error("unknown cache type",
			zap.String("cache_type", app.config.Cache.Type),
			zap.Strings("known_cache_types", []string{"null", "mem", "memcache"}),
		)
	}

	if app.config.TimezoneString != "" {
		fields := strings.Split(app.config.TimezoneString, ",")

		if len(fields) == 2 {
			// For input using utc offset format: "UTC+1,3600"
			offs, err := strconv.Atoi(fields[1])
			if err != nil {
				logger.Fatal("unable to parse seconds",
					zap.String("field[1]", fields[1]),
					zap.Error(err),
				)
			}

			app.defaultTimeZone = time.FixedZone(fields[0], offs)
			logger.Info("using fixed timezone",
				zap.String("timezone", app.defaultTimeZone.String()),
				zap.Int("offset", offs),
			)
		} else {
			// For input using names from IANA Time Zone database, such as "America/New_York"
			loc, err := time.LoadLocation(app.config.TimezoneString)
			if err != nil {
				logger.Fatal("failed to parse tz string",
					zap.String("timezone_string", app.config.TimezoneString),
					zap.Int("fields_got", len(fields)),
					zap.Error(err),
					zap.Int("fields_expected", 2),
				)
			}

			app.defaultTimeZone = loc
		}
	}

	if len(app.config.UnicodeRangeTables) != 0 {
		for _, stringRange := range app.config.UnicodeRangeTables {
			parser.RangeTables = append(parser.RangeTables, unicode.Scripts[stringRange])
		}
	} else {
		parser.RangeTables = append(parser.RangeTables, unicode.Latin)
	}

	if app.config.PidFile != "" {
		pidfile.SetPidfilePath(app.config.PidFile)
	}
	err := pidfile.Write()
	if err != nil && !pidfile.IsNotConfigured(err) {
		logger.Fatal("error during pidfile.Write()",
			zap.Error(err),
		)
	}

}

func (app *App) deferredAccessLogging(accessLogger *zap.Logger, r *http.Request, accessLogDetails *carbonapipb.AccessLogDetails, t time.Time, level zapcore.Level) {
	accessLogDetails.Runtime = time.Since(t).Seconds()
	accessLogDetails.RequestMethod = r.Method

	fields, err := accessLogDetails.GetLogFields()
	if err != nil {
		accessLogger.Error("could not marshal access log details", zap.Error(err))
	}
	var logMsg string
	if accessLogDetails.HttpCode/100 < 4 {
		logMsg = "request served"
	} else if accessLogDetails.HttpCode/100 == 4 {
		logMsg = "request failed with client error"
	} else {
		logMsg = "request failed with server error"
	}
	if ce := accessLogger.Check(level, logMsg); ce != nil {
		ce.Write(fields...)
	}

	if app != nil {
		app.ms.Responses.WithLabelValues(
			fmt.Sprintf("%d", accessLogDetails.HttpCode),
			accessLogDetails.Handler,
			fmt.Sprintf("%t", accessLogDetails.FromCache)).Inc()
	}
}

func (app *App) bucketRequestTimes(req *http.Request, t time.Duration) {
	app.ms.DurationExp.Observe(t.Seconds())
	app.ms.DurationLin.Observe(t.Seconds())

	if req.URL.Path == "/render" || req.URL.Path == "/render/" {
		app.ms.RenderDurationExp.Observe(t.Seconds())
	}
	if req.URL.Path == "/metrics/find" || req.URL.Path == "/metrics/find/" {
		app.ms.FindDurationExp.Observe(t.Seconds())
		app.ms.FindDurationLin.Observe(t.Seconds())
	}
}

func InitBackends(config cfg.Zipper, ms *ZipperPrometheusMetrics, logger *zap.Logger) ([]backend.Backend, error) {
	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		IdleConnTimeout:     3 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   config.Timeouts.Connect,
			KeepAlive: config.KeepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	configBackendList := config.GetBackends()
	backends := make([]backend.Backend, 0, len(configBackendList))
	for _, host := range configBackendList {
		if host.Http == "" {
			return nil, fmt.Errorf("backend without http address was provided: %+v", host)
		}
		dc, cluster, _ := config.InfoOfBackend(host.Http)
		var b backend.Backend
		var err error

		bConf := bnet.Config{
			Address:            host.Http,
			DC:                 dc,
			Cluster:            cluster,
			Client:             client,
			Timeout:            config.Timeouts.AfterStarted,
			PathCacheExpirySec: uint32(config.ExpireDelaySec),
			QHist:              ms.TimeInQueueSeconds,
			Responses:          ms.BackendResponses,
			Logger:             logger,
		}
		var be backend.BackendImpl
		if host.Grpc != "" {
			be, err = bnet.NewGrpc(bnet.GrpcConfig{
				Config:      bConf,
				GrpcAddress: host.Grpc,
			})
		} else {
			be, err = bnet.New(bConf)
		}
		b = backend.NewBackend(be,
			config.BackendQueueSize,
			config.ConcurrencyLimitPerServer,
			ms.BackendRequestsInQueue,
			ms.BackendSemaphoreSaturation,
			ms.BackendTimeInQSec,
			ms.BackendEnqueuedRequests)

		if err != nil {
			return backends, fmt.Errorf("Couldn't create backend for '%s'", host)
		}

		backends = append(backends, b)
	}

	return backends, nil
}

// Setup sets up the zipper for future lanuch.
func SetupZipper(configFile string, BuildVersion string, lg *zap.Logger) (cfg.Zipper, []backend.Backend, *expirecache.Cache,
	[]tldcache.TopLevelDomainPrefix, *ZipperPrometheusMetrics) {
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
		log.Fatal("no backends loaded; exiting")
	}

	lg.Info("starting carbonzipper",
		zap.String("build_version", BuildVersion),
		zap.String("zipperConfig", fmt.Sprintf("%+v", config)),
	)

	ms := NewZipperPrometheusMetrics(config)
	bs, err := InitBackends(config, ms, lg)
	if err != nil {
		lg.Fatal("failed to init backends", zap.Error(err))
	}

	return config, bs, expirecache.New(0), tldcache.InitTLDPrefixes(lg, config.TLDCacheExtraPrefixes), ms
}
