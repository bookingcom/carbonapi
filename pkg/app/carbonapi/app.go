package carbonapi

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.uber.org/zap/zapcore"

	"github.com/bookingcom/carbonapi/expr/functions"
	"github.com/bookingcom/carbonapi/expr/functions/cairo/png"
	"github.com/bookingcom/carbonapi/pkg/app/zipper"
	"github.com/bookingcom/carbonapi/pkg/backend"
	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/blocker"
	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/prioritylimiter"
	"github.com/bookingcom/carbonapi/pkg/trace"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/facebookgo/pidfile"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// BuildVersion is provided to be overridden at build time. Eg. go build -ldflags -X 'main.BuildVersion=...'
var BuildVersion string

// App is the main carbonapi runnable
type App struct {
	config         cfg.API
	queryCache     cache.BytesCache
	findCache      cache.BytesCache
	requestBlocker *blocker.RequestBlocker

	defaultTimeZone *time.Location

	backend backend.Backend

	// During processing we use two independent queues that share a semaphore to prevent stampeding.
	// fastQ includes regular requests
	fastQ chan *renderReq
	// slowQ contains large requests that could fill the queue and create a stampede.
	slowQ chan *renderReq

	ms PrometheusMetrics
	Lg *zap.Logger

	Zipper        *zipper.App
	ZipperLimiter *prioritylimiter.Limiter
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
		fastQ:           make(chan *renderReq, config.QueueSize),
		slowQ:           make(chan *renderReq, config.QueueSize),
	}
	app.requestBlocker.ReloadRules()

	backend, err := initBackend(app.config, lg,
		app.ms.ActiveUpstreamRequests, app.ms.WaitingUpstreamRequests,
		app.ms.UpstreamLimiterEnters, app.ms.UpstreamLimiterExits)
	if err != nil {
		lg.Fatal("couldn't initialize backends", zap.Error(err))
	}

	app.backend = backend
	setUpConfig(app, lg)

	if config.EmbedZipper {
		lg.Info("starting embedded zipper")
		var zlg *zap.Logger
		app.Zipper, zlg = zipper.Setup(config.ZipperConfig, BuildVersion, "zipper", lg)
		app.ZipperLimiter = prioritylimiter.New(config.ConcurrencyLimitPerServer,
			prioritylimiter.WithMetrics(app.ms.ActiveUpstreamRequests, app.ms.WaitingUpstreamRequests,
				app.ms.UpstreamLimiterEnters, app.ms.UpstreamLimiterExits))
		go app.Zipper.Start(false, zlg)
	}

	return app, nil
}

// Start starts the app: inits handlers, logger, starts HTTP server
func (app *App) Start(logger *zap.Logger) func() {
	flush := trace.InitTracer(BuildVersion, "carbonapi", logger, app.config.Traces)
	app.registerPrometheusMetrics()

	handler := initHandlers(app, logger)
	internalHandler := initHandlersInternal(app, logger)

	app.requestBlocker.ScheduleRuleReload()

	gracehttp.SetLogger(zap.NewStdLog(logger))
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
		logger.Fatal("gracehttp failed", zap.Error(err))
	}

	return flush
}

func (app *App) registerPrometheusMetrics() {
	prometheus.MustRegister(app.ms.Requests)
	prometheus.MustRegister(app.ms.Responses)
	prometheus.MustRegister(app.ms.FindNotFound)
	prometheus.MustRegister(app.ms.RenderPartialFail)
	prometheus.MustRegister(app.ms.RequestCancel)
	prometheus.MustRegister(app.ms.DurationExp)
	prometheus.MustRegister(app.ms.DurationLin)
	prometheus.MustRegister(app.ms.UpstreamRequests)
	prometheus.MustRegister(app.ms.RenderDurationExp)
	prometheus.MustRegister(app.ms.RenderDurationExpSimple)
	prometheus.MustRegister(app.ms.RenderDurationExpComplex)
	prometheus.MustRegister(app.ms.RenderDurationLinSimple)
	prometheus.MustRegister(app.ms.RenderDurationPerPointExp)
	prometheus.MustRegister(app.ms.FindDurationExp)
	prometheus.MustRegister(app.ms.FindDurationLin)
	prometheus.MustRegister(app.ms.FindDurationLinSimple)
	prometheus.MustRegister(app.ms.FindDurationLinComplex)

	prometheus.MustRegister(app.ms.UpstreamRequestsInQueue)
	prometheus.MustRegister(app.ms.UpstreamSemaphoreSaturation)
	prometheus.MustRegister(app.ms.UpstreamEnqueuedRequests)
	prometheus.MustRegister(app.ms.UpstreamSubRenderNum)
	prometheus.MustRegister(app.ms.UpstreamTimeInQSec)
	prometheus.MustRegister(app.ms.UpstreamTimeouts)

	prometheus.MustRegister(app.ms.TimeInQueueExp)
	prometheus.MustRegister(app.ms.TimeInQueueLin)
	prometheus.MustRegister(app.ms.ActiveUpstreamRequests)
	prometheus.MustRegister(app.ms.WaitingUpstreamRequests)
	prometheus.MustRegister(app.ms.UpstreamLimiterEnters)
	prometheus.MustRegister(app.ms.UpstreamLimiterExits)

	prometheus.MustRegister(app.ms.CacheRequests)
	prometheus.MustRegister(app.ms.CacheRespRead)
	prometheus.MustRegister(app.ms.CacheTimeouts)
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

func initBackend(config cfg.API, logger *zap.Logger, activeUpstreamRequests, waitingUpstreamRequests prometheus.Gauge,
	limiterEnters prometheus.Counter, limiterExits *prometheus.CounterVec) (backend.Backend, error) {
	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		DialContext: (&net.Dialer{
			Timeout:   config.Timeouts.Connect,
			KeepAlive: config.KeepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	if len(config.Backends) == 0 {
		return backend.NewBackend(nil, 0, 0, nil, nil, nil, nil), errors.New("got empty list of backends from config")
	}
	host := config.Backends[0]

	b, err := bnet.New(bnet.Config{
		Address:            host,
		Client:             client,
		Timeout:            config.Timeouts.AfterStarted,
		Limit:              0, // the old limiter is DISABLED now. TODO: Cleanup.
		PathCacheExpirySec: uint32(config.ExpireDelaySec),
		Logger:             logger,
		ActiveRequests:     activeUpstreamRequests,
		WaitingRequests:    waitingUpstreamRequests,
		LimiterEnters:      limiterEnters,
		LimiterExits:       limiterExits,
	})

	if err != nil {
		return backend.NewBackend(b, 0, 0, nil, nil, nil, nil), fmt.Errorf("Couldn't create backend for '%s'", host)
	}

	return backend.NewBackend(b, 0, 0, nil, nil, nil, nil), nil
}
