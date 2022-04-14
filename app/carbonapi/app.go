package carbonapi

import (
	"errors"
	"expvar"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/bookingcom/carbonapi/blocker"
	"github.com/bookingcom/carbonapi/cache"
	"github.com/bookingcom/carbonapi/carbonapipb"
	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/expr/functions"
	"github.com/bookingcom/carbonapi/expr/functions/cairo/png"
	"github.com/bookingcom/carbonapi/mstats"
	"github.com/bookingcom/carbonapi/pathcache"
	"github.com/bookingcom/carbonapi/pkg/backend"
	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/trace"
	"github.com/bookingcom/carbonapi/util"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/facebookgo/pidfile"
	"github.com/lomik/zapwriter"
	"github.com/peterbourgon/g2g"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// BuildVersion is provided to be overridden at build time. Eg. go build -ldflags -X 'main.BuildVersion=...'
var BuildVersion string

func init() {
	expvar.NewString("GoVersion").Set(runtime.Version())
	apiMetrics.Goroutines = expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	})
	expvar.Publish("goroutines", apiMetrics.Goroutines)

	startMinute := time.Now().Unix() / 60
	apiMetrics.Uptime = expvar.Func(func() interface{} {
		return time.Now().Unix()/60 - startMinute
	})
	expvar.Publish("uptime", apiMetrics.Uptime)
}

// App is the main carbonapi runnable
type App struct {
	config         cfg.API
	queryCache     cache.BytesCache
	findCache      cache.BytesCache
	requestBlocker *blocker.RequestBlocker

	defaultTimeZone *time.Location

	backend backend.Backend

	prometheusMetrics PrometheusMetrics
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
func (app *App) Start() func() {
	logger := zapwriter.Logger("carbonapi")
	accessLogger := zapwriter.Logger("access")

	flush := trace.InitTracer(BuildVersion, "carbonapi", logger, app.config.Traces)

	handler := initHandlers(app, accessLogger, logger)
	internalHandler := initHandlersInternal(app, accessLogger, logger)
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

	// TODO (grzkv): Move expvars to init since they are global to the package
	expvar.Publish("config", expvar.Func(func() interface{} { return app.config }))

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

		mcache := app.queryCache.(*cache.MemcachedCache)

		// TODO (grzkv) Move to conventional Prom metrics.
		apiMetrics.MemcacheTimeouts = expvar.Func(func() interface{} {
			return mcache.Timeouts()
		})
		expvar.Publish("memcache_timeouts", apiMetrics.MemcacheTimeouts)
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

		qcache := app.queryCache.(*cache.ExpireCache)

		apiMetrics.CacheSize = expvar.Func(func() interface{} {
			return qcache.Size()
		})
		expvar.Publish("cache_size", apiMetrics.CacheSize)

		apiMetrics.CacheItems = expvar.Func(func() interface{} {
			return qcache.Items()
		})
		expvar.Publish("cache_items", apiMetrics.CacheItems)

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

	var host string
	if envhost := os.Getenv("GRAPHITEHOST") + ":" + os.Getenv("GRAPHITEPORT"); envhost != ":" || app.config.Graphite.Host != "" {
		switch {
		case envhost != ":" && app.config.Graphite.Host != "":
			host = app.config.Graphite.Host
		case envhost != ":":
			host = envhost
		case app.config.Graphite.Host != "":
			host = app.config.Graphite.Host
		}
	}

	// +1 to track every over the number of buckets we track
	timeBuckets = make([]int64, app.config.Buckets+1)
	expTimeBuckets = make([]int64, app.config.Buckets+1)
	expvar.Publish("requestBuckets", expvar.Func(renderTimeBuckets))
	expvar.Publish("expRequestBuckets", expvar.Func(renderExpTimeBuckets))

	// Setup in-memory path cache for carbonzipper requests
	app.config.PathCache = pathcache.NewPathCache(app.config.ExpireDelaySec)

	zipperMetrics.CacheSize = expvar.Func(func() interface{} { return app.config.PathCache.ECSize() })
	expvar.Publish("cacheSize", zipperMetrics.CacheSize)

	zipperMetrics.CacheItems = expvar.Func(func() interface{} { return app.config.PathCache.ECItems() })
	expvar.Publish("cacheItems", zipperMetrics.CacheItems)

	if host != "" {
		// register our metrics with graphite
		graphite := g2g.NewGraphite(host, app.config.Graphite.Interval, 10*time.Second)

		hostname, _ := os.Hostname()
		hostname = strings.Replace(hostname, ".", "_", -1)

		prefix := app.config.Graphite.Prefix

		pattern := app.config.Graphite.Pattern
		pattern = strings.Replace(pattern, "{prefix}", prefix, -1)
		pattern = strings.Replace(pattern, "{fqdn}", hostname, -1)

		graphite.Register(fmt.Sprintf("%s.requests", pattern), apiMetrics.Requests)
		graphite.Register(fmt.Sprintf("%s.responses", pattern), apiMetrics.Responses)
		graphite.Register(fmt.Sprintf("%s.errors", pattern), apiMetrics.Errors)

		for i := 0; i <= app.config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("%s.requests_in_%dms_to_%dms", pattern, i*100, (i+1)*100), bucketEntry(i))
			lower, upper := util.Bounds(i)
			graphite.Register(fmt.Sprintf("%s.exp.requests_in_%05dms_to_%05dms", pattern, lower, upper), bucketEntry(i))
		}

		graphite.Register(fmt.Sprintf("%s.request_cache_hits", pattern), apiMetrics.RequestCacheHits)
		graphite.Register(fmt.Sprintf("%s.request_cache_misses", pattern), apiMetrics.RequestCacheMisses)
		graphite.Register(fmt.Sprintf("%s.request_cache_overhead_ns", pattern), apiMetrics.RenderCacheOverheadNS)

		graphite.Register(fmt.Sprintf("%s.find_requests", pattern), apiMetrics.FindRequests)
		graphite.Register(fmt.Sprintf("%s.find_cache_hits", pattern), apiMetrics.FindCacheHits)
		graphite.Register(fmt.Sprintf("%s.find_cache_misses", pattern), apiMetrics.FindCacheMisses)
		graphite.Register(fmt.Sprintf("%s.find_cache_overhead_ns", pattern), apiMetrics.FindCacheOverheadNS)

		graphite.Register(fmt.Sprintf("%s.render_requests", pattern), apiMetrics.RenderRequests)

		if apiMetrics.MemcacheTimeouts != nil {
			graphite.Register(fmt.Sprintf("%s.memcache_timeouts", pattern), apiMetrics.MemcacheTimeouts)
		}

		if apiMetrics.CacheSize != nil {
			graphite.Register(fmt.Sprintf("%s.cache_size", pattern), apiMetrics.CacheSize)
			graphite.Register(fmt.Sprintf("%s.cache_items", pattern), apiMetrics.CacheItems)
		}

		graphite.Register(fmt.Sprintf("%s.zipper.find_requests", pattern), zipperMetrics.FindRequests)
		graphite.Register(fmt.Sprintf("%s.zipper.find_errors", pattern), zipperMetrics.FindErrors)

		graphite.Register(fmt.Sprintf("%s.zipper.render_requests", pattern), zipperMetrics.RenderRequests)
		graphite.Register(fmt.Sprintf("%s.zipper.render_errors", pattern), zipperMetrics.RenderErrors)

		graphite.Register(fmt.Sprintf("%s.zipper.info_requests", pattern), zipperMetrics.InfoRequests)
		graphite.Register(fmt.Sprintf("%s.zipper.info_errors", pattern), zipperMetrics.InfoErrors)

		graphite.Register(fmt.Sprintf("%s.zipper.timeouts", pattern), zipperMetrics.Timeouts)

		graphite.Register(fmt.Sprintf("%s.zipper.cache_size", pattern), zipperMetrics.CacheSize)
		graphite.Register(fmt.Sprintf("%s.zipper.cache_items", pattern), zipperMetrics.CacheItems)

		graphite.Register(fmt.Sprintf("%s.zipper.cache_hits", pattern), zipperMetrics.CacheHits)
		graphite.Register(fmt.Sprintf("%s.zipper.cache_misses", pattern), zipperMetrics.CacheMisses)

		go mstats.Start(app.config.Graphite.Interval)

		graphite.Register(fmt.Sprintf("%s.goroutines", pattern), apiMetrics.Goroutines)
		graphite.Register(fmt.Sprintf("%s.uptime", pattern), apiMetrics.Uptime)
		graphite.Register(fmt.Sprintf("%s.alloc", pattern), &mstats.Alloc)
		graphite.Register(fmt.Sprintf("%s.total_alloc", pattern), &mstats.TotalAlloc)
		graphite.Register(fmt.Sprintf("%s.num_gc", pattern), &mstats.NumGC)
		graphite.Register(fmt.Sprintf("%s.pause_ns", pattern), &mstats.PauseNS)

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

func (app *App) deferredAccessLogging(accessLogger *zap.Logger, r *http.Request, accessLogDetails *carbonapipb.AccessLogDetails, t time.Time, logAsError bool) {
	accessLogDetails.Runtime = time.Since(t).Seconds()
	accessLogDetails.RequestMethod = r.Method

	fields, err := accessLogDetails.GetLogFields()
	if err != nil {
		accessLogger.Error("could not marshal access log details", zap.Error(err))
	}

	// TODO (grzkv) This logic is not obvious for the user
	if logAsError {
		accessLogger.Error("request failed", fields...)
		apiMetrics.Errors.Add(1)
	} else {
		// TODO (grzkv) The code can differ from the real one. Clean up
		// accessLogDetails.HttpCode = http.StatusOK
		accessLogger.Info("request served", fields...)
		apiMetrics.Responses.Add(1)
	}

	if app != nil {
		app.prometheusMetrics.Responses.WithLabelValues(
			fmt.Sprintf("%d", accessLogDetails.HttpCode),
			accessLogDetails.Handler,
			fmt.Sprintf("%t", accessLogDetails.FromCache)).Inc()
	}
}

var timeBuckets []int64
var expTimeBuckets []int64

type bucketEntry int

func (b bucketEntry) String() string {
	return strconv.Itoa(int(atomic.LoadInt64(&timeBuckets[b])))
}

func renderTimeBuckets() interface{} {
	return timeBuckets
}

func renderExpTimeBuckets() interface{} {
	return timeBuckets
}

func findBucketIndex(buckets []int64, bucket int) int {
	var i int
	if bucket < 0 {
		i = 0
	} else if bucket < len(buckets)-1 {
		i = bucket
	} else {
		i = len(buckets) - 1
	}

	return i
}

func (app *App) bucketRequestTimes(req *http.Request, t time.Duration) {
	ms := t.Nanoseconds() / int64(time.Millisecond)

	bucket := int(ms / 100)
	bucketIdx := findBucketIndex(timeBuckets, bucket)
	atomic.AddInt64(&timeBuckets[bucketIdx], 1)

	expBucket := util.Bucket(ms, app.config.Buckets)
	expBucketIdx := findBucketIndex(expTimeBuckets, expBucket)
	atomic.AddInt64(&expTimeBuckets[expBucketIdx], 1)

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
