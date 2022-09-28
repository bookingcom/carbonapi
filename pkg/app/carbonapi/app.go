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

	prometheusMetrics PrometheusMetrics
	Lg *zap.Logger

	Zipper *zipper.App
}

// New creates a new app
func New(config cfg.API, logger *zap.Logger, buildVersion string) (*App, error) {
	if len(config.Backends) == 0 {
		logger.Fatal("no backends specified for upstreams!")
	}

	BuildVersion = buildVersion
	app := &App{
		config:            config,
		queryCache:        cache.NullCache{},
		findCache:         cache.NullCache{},
		defaultTimeZone:   time.Local,
		prometheusMetrics: newPrometheusMetrics(config),
		requestBlocker:    blocker.NewRequestBlocker(config.BlockHeaderFile, config.BlockHeaderUpdatePeriod, logger),
	}
	app.requestBlocker.ReloadRules()

	// TODO(gmagnusson): Setup backends
	backend, err := initBackend(app.config, logger,
		app.prometheusMetrics.ActiveUpstreamRequests,
		app.prometheusMetrics.WaitingUpstreamRequests)
	if err != nil {
		logger.Fatal("couldn't initialize backends", zap.Error(err))
	}

	app.backend = backend
	setUpConfig(app, logger)

	return app, nil
}

// Start starts the app: inits handlers, logger, starts HTTP server
func (app *App) Start(logger *zap.Logger) func() {
	flush := trace.InitTracer(BuildVersion, "carbonapi", logger, app.config.Traces)

	handler := initHandlers(app, logger)
	internalHandler := initHandlersInternal(app, logger)
	prometheusServer := app.registerPrometheusMetrics(internalHandler)

	app.requestBlocker.ScheduleRuleReload()

	gracehttp.SetLogger(zap.NewStdLog(logger))
	err := gracehttp.Serve(&http.Server{
		Addr:         app.config.Listen,
		Handler:      handler,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: app.config.Timeouts.Global * 2, // It has to be greater than Timeout.Global because we use that value as per-request context timeout
	}, prometheusServer)
	if err != nil {
		logger.Fatal("gracehttp failed",
			zap.Error(err),
		)
	}
	return flush
}

func (app *App) registerPrometheusMetrics(internalHandler http.Handler) *http.Server {
	prometheus.MustRegister(app.prometheusMetrics.Requests)
	prometheus.MustRegister(app.prometheusMetrics.Responses)
	prometheus.MustRegister(app.prometheusMetrics.FindNotFound)
	prometheus.MustRegister(app.prometheusMetrics.RenderPartialFail)
	prometheus.MustRegister(app.prometheusMetrics.RequestCancel)
	prometheus.MustRegister(app.prometheusMetrics.DurationExp)
	prometheus.MustRegister(app.prometheusMetrics.DurationLin)
	prometheus.MustRegister(app.prometheusMetrics.RenderDurationExp)
	prometheus.MustRegister(app.prometheusMetrics.RenderDurationExpSimple)
	prometheus.MustRegister(app.prometheusMetrics.RenderDurationExpComplex)
	prometheus.MustRegister(app.prometheusMetrics.RenderDurationLinSimple)
	prometheus.MustRegister(app.prometheusMetrics.RenderDurationPerPointExp)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationExp)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationLin)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationLinSimple)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationLinComplex)
	prometheus.MustRegister(app.prometheusMetrics.TimeInQueueExp)
	prometheus.MustRegister(app.prometheusMetrics.TimeInQueueLin)
	prometheus.MustRegister(app.prometheusMetrics.ActiveUpstreamRequests)
	prometheus.MustRegister(app.prometheusMetrics.WaitingUpstreamRequests)

	writeTimeout := app.config.Timeouts.Global
	if writeTimeout < 30*time.Second {
		writeTimeout = time.Minute
	}

	s := &http.Server{
		Addr:         app.config.ListenInternal,
		Handler:      internalHandler,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: writeTimeout,
	}

	return s
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

		app.queryCache = cache.NewReplicatedMemcached(app.config.Cache.Prefix, app.config.Cache.QueryTimeoutMs, app.config.Cache.MemcachedServers...)
		app.findCache = cache.NewReplicatedMemcached(app.config.Cache.Prefix, app.config.Cache.QueryTimeoutMs, app.config.Cache.MemcachedServers...)

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
		app.prometheusMetrics.Responses.WithLabelValues(
			fmt.Sprintf("%d", accessLogDetails.HttpCode),
			accessLogDetails.Handler,
			fmt.Sprintf("%t", accessLogDetails.FromCache)).Inc()
	}
}

func (app *App) bucketRequestTimes(req *http.Request, t time.Duration) {
	app.prometheusMetrics.DurationExp.Observe(t.Seconds())
	app.prometheusMetrics.DurationLin.Observe(t.Seconds())

	if req.URL.Path == "/render" || req.URL.Path == "/render/" {
		app.prometheusMetrics.RenderDurationExp.Observe(t.Seconds())
	}
	if req.URL.Path == "/metrics/find" || req.URL.Path == "/metrics/find/" {
		app.prometheusMetrics.FindDurationExp.Observe(t.Seconds())
		app.prometheusMetrics.FindDurationLin.Observe(t.Seconds())
	}
}

func initBackend(config cfg.API, logger *zap.Logger, activeUpstreamRequests, waitingUpstreamRequests prometheus.Gauge) (backend.Backend, error) {
	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		DialContext: (&net.Dialer{
			Timeout:   config.Timeouts.Connect,
			KeepAlive: config.KeepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	// TODO (grzkv): Stop using a list, move to a single value in config
	if len(config.Backends) == 0 {
		return nil, errors.New("got empty list of backends from config")
	}
	host := config.Backends[0]

	b, err := bnet.New(bnet.Config{
		Address:            host,
		Client:             client,
		Timeout:            config.Timeouts.AfterStarted,
		Limit:              config.ConcurrencyLimitPerServer,
		PathCacheExpirySec: uint32(config.ExpireDelaySec),
		Logger:             logger,
		ActiveRequests:     activeUpstreamRequests,
		WaitingRequests:    waitingUpstreamRequests,
	})

	if err != nil {
		return b, fmt.Errorf("Couldn't create backend for '%s'", host)
	}

	return b, nil
}
