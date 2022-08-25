package zipper

import (
	"context"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-expirecache"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/mstats"
	"github.com/bookingcom/carbonapi/pkg/backend"
	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/trace"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/util"

	"github.com/dgryski/httputil"
	"github.com/facebookgo/grace/gracehttp"
	"github.com/peterbourgon/g2g"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// BuildVersion is replaced by ldflags
var BuildVersion string

// App represents the main zipper runnable
type App struct {
	config              cfg.Zipper
	prometheusMetrics   *PrometheusMetrics
	backends            []backend.Backend
	topLevelDomainCache *expirecache.Cache
}

// New inits backends and makes a new copy of the app. Does not run the app
func New(config cfg.Zipper, logger *zap.Logger, buildVersion string) (*App, error) {
	BuildVersion = buildVersion
	bs, err := initBackends(config, logger)
	if err != nil {
		logger.Fatal("Failed to initialize backends",
			zap.Error(err),
		)
		return nil, err
	}

	app := App{
		config:              config,
		prometheusMetrics:   NewPrometheusMetrics(config),
		backends:            bs,
		topLevelDomainCache: expirecache.New(0),
	}
	return &app, nil
}

// Start start launches the goroutines starts the app execution
func (app *App) Start(logger *zap.Logger) func() {
	flush := trace.InitTracer(BuildVersion, "carbonzipper", logger, app.config.Traces)

	// Should print nicer stack traces in case of unexpected panic.
	defer func() {
		if r := recover(); r != nil {
			logger.Fatal("Recovered from unhandled panic",
				zap.Stack("stacktrace"),
			)
		}
	}()

	// +1 to track every over the number of buckets we track
	timeBuckets = make([]int64, app.config.Buckets+1)
	expTimeBuckets = make([]int64, app.config.Buckets+1)

	httputil.PublishTrackedConnections("httptrack")
	publishExpvarz(app)

	handler := initHandlers(app, logger)

	// nothing in the app.config? check the environment
	if app.config.Graphite.Host == "" {
		if host := os.Getenv("GRAPHITEHOST") + ":" + os.Getenv("GRAPHITEPORT"); host != ":" {
			app.config.Graphite.Host = host
		}
	}

	if app.config.Graphite.Pattern == "" {
		app.config.Graphite.Pattern = "{prefix}.{fqdn}"
	}

	if app.config.Graphite.Prefix == "" {
		app.config.Graphite.Prefix = "carbon.zipper"
	}

	// only register g2g if we have a graphite host
	if app.config.Graphite.Host != "" {
		initGraphite(app)
	}

	go app.probeTopLevelDomains()
	metricsServer := metricsServer(app)

	gracehttp.SetLogger(zap.NewStdLog(logger))
	err := gracehttp.Serve(&http.Server{
		Addr:         app.config.Listen,
		Handler:      handler,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: app.config.Timeouts.Global * 2, // It has to be greater than Timeout.Global because we use that value as per-request context timeout
	}, metricsServer)

	if err != nil {
		log.Fatal("error during gracehttp.Serve()",
			zap.Error(err),
		)
	}
	return flush
}

func (app *App) doProbe() {
	topLevelDomainCache := make(map[string][]*backend.Backend)
	for i := 0; i < len(app.backends); i++ {
		topLevelDomains := getTopLevelDomains(app.backends[i])
		for _, topLevelDomain := range topLevelDomains {
			topLevelDomainCache[topLevelDomain] = append(topLevelDomainCache[topLevelDomain], &app.backends[i])
		}
	}
	app.topLevelDomainCache.Set("tlds", topLevelDomainCache, 0, 2*app.config.InternalRoutingCache)
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
	probeTicker := time.NewTicker(time.Duration(app.config.InternalRoutingCache) * time.Second)
	for range probeTicker.C {
		app.doProbe()
	}
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

func initBackends(config cfg.Zipper, logger *zap.Logger) ([]backend.Backend, error) {
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
			Limit:              config.ConcurrencyLimitPerServer,
			PathCacheExpirySec: uint32(config.ExpireDelaySec),
			Logger:             logger,
		}
		if host.Grpc != "" {
			b, err = bnet.NewGrpc(bnet.GrpcConfig{
				Config:      bConf,
				GrpcAddress: host.Grpc,
			})
		} else {
			b, err = bnet.New(bConf)
		}

		if err != nil {
			return backends, fmt.Errorf("Couldn't create backend for '%s'", host)
		}

		backends = append(backends, b)
	}

	return backends, nil
}

func initGraphite(app *App) {
	// register our metrics with graphite
	graphite := g2g.NewGraphite(app.config.Graphite.Host, app.config.Graphite.Interval, 10*time.Second)

	/* #nosec */
	hostname, _ := os.Hostname()
	hostname = strings.Replace(hostname, ".", "_", -1)

	prefix := app.config.Graphite.Prefix

	pattern := app.config.Graphite.Pattern
	pattern = strings.Replace(pattern, "{prefix}", prefix, -1)
	pattern = strings.Replace(pattern, "{fqdn}", hostname, -1)

	graphite.Register(fmt.Sprintf("%s.requests", pattern), Metrics.Requests)
	graphite.Register(fmt.Sprintf("%s.responses", pattern), Metrics.Responses)
	graphite.Register(fmt.Sprintf("%s.errors", pattern), Metrics.Errors)

	graphite.Register(fmt.Sprintf("%s.find_requests", pattern), Metrics.FindRequests)
	graphite.Register(fmt.Sprintf("%s.find_errors", pattern), Metrics.FindErrors)

	graphite.Register(fmt.Sprintf("%s.render_requests", pattern), Metrics.RenderRequests)
	graphite.Register(fmt.Sprintf("%s.render_errors", pattern), Metrics.RenderErrors)

	graphite.Register(fmt.Sprintf("%s.info_requests", pattern), Metrics.InfoRequests)
	graphite.Register(fmt.Sprintf("%s.info_errors", pattern), Metrics.InfoErrors)

	graphite.Register(fmt.Sprintf("%s.timeouts", pattern), Metrics.Timeouts)

	for i := 0; i <= app.config.Buckets; i++ {
		graphite.Register(fmt.Sprintf("%s.requests_in_%dms_to_%dms", pattern, i*100, (i+1)*100), bucketEntry(i))
		lower, upper := util.Bounds(i)
		graphite.Register(fmt.Sprintf("%s.exp.requests_in_%05dms_to_%05dms", pattern, lower, upper), expBucketEntry(i))
	}

	graphite.Register(fmt.Sprintf("%s.cache_size", pattern), Metrics.CacheSize)
	graphite.Register(fmt.Sprintf("%s.cache_items", pattern), Metrics.CacheItems)

	graphite.Register(fmt.Sprintf("%s.cache_hits", pattern), Metrics.CacheHits)
	graphite.Register(fmt.Sprintf("%s.cache_misses", pattern), Metrics.CacheMisses)

	go mstats.Start(app.config.Graphite.Interval)

	graphite.Register(fmt.Sprintf("%s.goroutines", pattern), Metrics.Goroutines)
	graphite.Register(fmt.Sprintf("%s.uptime", pattern), Metrics.Uptime)
	graphite.Register(fmt.Sprintf("%s.alloc", pattern), &mstats.Alloc)
	graphite.Register(fmt.Sprintf("%s.total_alloc", pattern), &mstats.TotalAlloc)
	graphite.Register(fmt.Sprintf("%s.num_gc", pattern), &mstats.NumGC)
	graphite.Register(fmt.Sprintf("%s.pause_ns", pattern), &mstats.PauseNS)
}

func metricsServer(app *App) *http.Server {
	prometheus.MustRegister(app.prometheusMetrics.Requests)
	prometheus.MustRegister(app.prometheusMetrics.Responses)
	prometheus.MustRegister(app.prometheusMetrics.Renders)
	prometheus.MustRegister(app.prometheusMetrics.RenderMismatches)
	prometheus.MustRegister(app.prometheusMetrics.RenderFixedMismatches)
	prometheus.MustRegister(app.prometheusMetrics.RenderMismatchedResponses)
	prometheus.MustRegister(app.prometheusMetrics.FindNotFound)
	prometheus.MustRegister(app.prometheusMetrics.RequestCancel)
	prometheus.MustRegister(app.prometheusMetrics.DurationExp)
	prometheus.MustRegister(app.prometheusMetrics.DurationLin)
	prometheus.MustRegister(app.prometheusMetrics.RenderDurationExp)
	prometheus.MustRegister(app.prometheusMetrics.RenderOutDurationExp)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationExp)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationLin)
	prometheus.MustRegister(app.prometheusMetrics.FindDurationByBackend)
	prometheus.MustRegister(app.prometheusMetrics.TimeInQueueExp)
	prometheus.MustRegister(app.prometheusMetrics.TimeInQueueLin)

	writeTimeout := app.config.Timeouts.Global
	if writeTimeout < 30*time.Second {
		writeTimeout = time.Minute
	}

	r := initMetricHandlers()

	s := &http.Server{
		Addr:         app.config.ListenInternal,
		Handler:      r,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: writeTimeout,
	}

	return s
}

func publishExpvarz(app *App) {
	expvar.Publish("requestBuckets", expvar.Func(renderTimeBuckets))
	expvar.Publish("expRequestBuckets", expvar.Func(renderExpTimeBuckets))

	Metrics.Goroutines = expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	})
	expvar.Publish("goroutines", Metrics.Goroutines)

	startMinute := time.Now().Unix() / 60
	Metrics.Uptime = expvar.Func(func() interface{} {
		return time.Now().Unix()/60 - startMinute
	})
	expvar.Publish("uptime", Metrics.Uptime)

	// export config via expvars
	expvar.Publish("config", expvar.Func(func() interface{} { return app.config }))

	/* Configure zipper */
	// set up caches

	Metrics.CacheSize = expvar.Func(func() interface{} { return app.config.PathCache.ECSize() })
	expvar.Publish("cacheSize", Metrics.CacheSize)

	Metrics.CacheItems = expvar.Func(func() interface{} { return app.config.PathCache.ECItems() })
	expvar.Publish("cacheItems", Metrics.CacheItems)
}
