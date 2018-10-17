package zipper

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/mstats"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"runtime"
	"expvar"
	"net/http"
	"github.com/dgryski/httputil"
	"github.com/bookingcom/carbonapi/util"
	"github.com/bookingcom/carbonapi/pkg/backend"
	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/pkg/errors"
	"os"
	"github.com/peterbourgon/g2g"
	"strings"
	"fmt"
	"log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http/pprof"
	"github.com/facebookgo/grace/gracehttp"
	"sync/atomic"
	"github.com/bookingcom/carbonapi/cfg"
	"net"
	"strconv"
)

var BuildVersion string
type EnvConfig struct {
	config   cfg.Zipper
	backends []backend.Backend
}

func StartCarbonZipper(config cfg.Zipper,logger *zap.Logger, buildVersion string)(cfg.Zipper, []backend.Backend){
	BuildVersion = buildVersion

	bs, err := initBackends(config, logger)
	envConfig := EnvConfig{config:config, backends:bs}
	if err != nil {
		logger.Fatal("Failed to initialize backends",
			zap.Error(err),
		)
	}

	backends := bs

	go func() {
		probeTicker := time.NewTicker(5 * time.Minute)
		for {
			for _, b := range backends {
				go b.Probe()
			}
			<-probeTicker.C
		}
	}()

	types.SetCorruptionWatcher(config.CorruptionThreshold, logger)

	// Should print nicer stack traces in case of unexpected panic.
	defer func() {
		if r := recover(); r != nil {
			logger.Fatal("Recovered from unhandled panic",
				zap.Stack("stacktrace"),
			)
		}
	}()

	logger = zapwriter.Logger("main")

	runtime.GOMAXPROCS(config.MaxProcs)

	// +1 to track every over the number of buckets we track
	timeBuckets = make([]int64, config.Buckets+1)
	expTimeBuckets = make([]int64, config.Buckets+1)

	httputil.PublishTrackedConnections("httptrack")
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
	expvar.Publish("config", expvar.Func(func() interface{} { return config }))

	/* Configure zipper */
	// set up caches

	Metrics.CacheSize = expvar.Func(func() interface{} { return config.PathCache.ECSize() })
	expvar.Publish("cacheSize", Metrics.CacheSize)

	Metrics.CacheItems = expvar.Func(func() interface{} { return config.PathCache.ECItems() })
	expvar.Publish("cacheItems", Metrics.CacheItems)

	r := http.NewServeMux()

	r.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(envConfig.findHandler, envConfig.bucketRequestTimes)))
	r.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(envConfig.renderHandler, envConfig.bucketRequestTimes)))
	r.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(envConfig.infoHandler, envConfig.bucketRequestTimes)))
	r.HandleFunc("/lb_check", envConfig.lbCheckHandler)

	handler := util.UUIDHandler(r)

	// nothing in the config? check the environment
	if config.Graphite.Host == "" {
		if host := os.Getenv("GRAPHITEHOST") + ":" + os.Getenv("GRAPHITEPORT"); host != ":" {
			config.Graphite.Host = host
		}
	}

	if config.Graphite.Pattern == "" {
		config.Graphite.Pattern = "{prefix}.{fqdn}"
	}

	if config.Graphite.Prefix == "" {
		config.Graphite.Prefix = "carbon.zipper"
	}

	// only register g2g if we have a graphite host
	if config.Graphite.Host != "" {
		// register our metrics with graphite
		graphite := g2g.NewGraphite(config.Graphite.Host, config.Graphite.Interval, 10*time.Second)

		/* #nosec */
		hostname, _ := os.Hostname()
		hostname = strings.Replace(hostname, ".", "_", -1)

		prefix := config.Graphite.Prefix

		pattern := config.Graphite.Pattern
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

		for i := 0; i <= config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("%s.requests_in_%dms_to_%dms", pattern, i*100, (i+1)*100), bucketEntry(i))
			lower, upper := util.Bounds(i)
			graphite.Register(fmt.Sprintf("%s.exp.requests_in_%05dms_to_%05dms", pattern, lower, upper), expBucketEntry(i))
		}

		graphite.Register(fmt.Sprintf("%s.cache_size", pattern), Metrics.CacheSize)
		graphite.Register(fmt.Sprintf("%s.cache_items", pattern), Metrics.CacheItems)

		graphite.Register(fmt.Sprintf("%s.cache_hits", pattern), Metrics.CacheHits)
		graphite.Register(fmt.Sprintf("%s.cache_misses", pattern), Metrics.CacheMisses)

		go mstats.Start(config.Graphite.Interval)

		graphite.Register(fmt.Sprintf("%s.goroutines", pattern), Metrics.Goroutines)
		graphite.Register(fmt.Sprintf("%s.uptime", pattern), Metrics.Uptime)
		graphite.Register(fmt.Sprintf("%s.alloc", pattern), &mstats.Alloc)
		graphite.Register(fmt.Sprintf("%s.total_alloc", pattern), &mstats.TotalAlloc)
		graphite.Register(fmt.Sprintf("%s.num_gc", pattern), &mstats.NumGC)
		graphite.Register(fmt.Sprintf("%s.pause_ns", pattern), &mstats.PauseNS)
	}

	go func() {
		prometheus.MustRegister(prometheusMetrics.Requests)
		prometheus.MustRegister(prometheusMetrics.Responses)
		prometheus.MustRegister(prometheusMetrics.DurationsExp)
		prometheus.MustRegister(prometheusMetrics.DurationsLin)

		writeTimeout := config.Timeouts.Global
		if writeTimeout < 30*time.Second {
			writeTimeout = time.Minute
		}

		r := http.NewServeMux()
		r.Handle("/metrics", promhttp.Handler())

		r.Handle("/debug/vars", expvar.Handler())
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)

		s := &http.Server{
			Addr:         config.ListenInternal,
			Handler:      r,
			ReadTimeout:  1 * time.Second,
			WriteTimeout: writeTimeout,
		}

		if err := s.ListenAndServe(); err != nil {
			logger.Fatal("Internal handle server failed",
				zap.Error(err),
			)
		}
	}()

	err = gracehttp.Serve(&http.Server{
		Addr:         config.Listen,
		Handler:      handler,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: config.Timeouts.Global,
	})

	if err != nil {
		log.Fatal("error during gracehttp.Serve()",
			zap.Error(err),
		)
	}
	return config, backends
}

var timeBuckets []int64
var expTimeBuckets []int64

type bucketEntry int
type expBucketEntry int

func (b bucketEntry) String() string {
	return strconv.Itoa(int(atomic.LoadInt64(&timeBuckets[b])))
}

func (b expBucketEntry) String() string {
	return strconv.Itoa(int(atomic.LoadInt64(&expTimeBuckets[b])))
}

func renderTimeBuckets() interface{} {
	return timeBuckets
}

func renderExpTimeBuckets() interface{} {
	return expTimeBuckets
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

func (envConfig *EnvConfig) bucketRequestTimes(req *http.Request, t time.Duration) {
	ms := t.Nanoseconds() / int64(time.Millisecond)

	bucket := int(ms / 100)
	bucketIdx := findBucketIndex(timeBuckets, bucket)
	atomic.AddInt64(&timeBuckets[bucketIdx], 1)

	expBucket := util.Bucket(ms, envConfig.config.Buckets)
	expBucketIdx := findBucketIndex(expTimeBuckets, expBucket)
	atomic.AddInt64(&expTimeBuckets[expBucketIdx], 1)

	prometheusMetrics.DurationsExp.Observe(t.Seconds())
	prometheusMetrics.DurationsLin.Observe(t.Seconds())
}

func initBackends(config cfg.Zipper, logger *zap.Logger) ([]backend.Backend, error) {
	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		DialContext: (&net.Dialer{
			Timeout:   config.Timeouts.Connect,
			KeepAlive: config.KeepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	backends := make([]backend.Backend, 0, len(config.Backends))
	for _, host := range config.Backends {
		b, err := bnet.New(bnet.Config{
			Address:            host,
			Client:             client,
			Timeout:            config.Timeouts.AfterStarted,
			Limit:              config.ConcurrencyLimitPerServer,
			PathCacheExpirySec: uint32(config.ExpireDelaySec),
			Logger:             logger,
		})

		if err != nil {
			return backends, errors.Errorf("Couldn't create backend for '%s'", host)
		}

		backends = append(backends, b)
	}

	return backends, nil
}
