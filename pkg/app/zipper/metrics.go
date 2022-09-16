package zipper

import (
	"expvar"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/mstats"
	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/peterbourgon/g2g"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains grouped expvars for /debug/vars and graphite
var Metrics = struct {
	Requests  *expvar.Int
	Responses *expvar.Int
	Errors    *expvar.Int

	Goroutines expvar.Func
	Uptime     expvar.Func

	FindRequests *expvar.Int
	FindErrors   *expvar.Int

	RenderRequests *expvar.Int
	RenderErrors   *expvar.Int

	InfoRequests *expvar.Int
	InfoErrors   *expvar.Int

	Timeouts *expvar.Int

	CacheSize   expvar.Func
	CacheItems  expvar.Func
	CacheMisses *expvar.Int
	CacheHits   *expvar.Int
}{
	Requests:  expvar.NewInt("requests"),
	Responses: expvar.NewInt("responses"),
	Errors:    expvar.NewInt("errors"),

	FindRequests: expvar.NewInt("find_requests"),
	FindErrors:   expvar.NewInt("find_errors"),

	RenderRequests: expvar.NewInt("render_requests"),
	RenderErrors:   expvar.NewInt("render_errors"),

	InfoRequests: expvar.NewInt("info_requests"),
	InfoErrors:   expvar.NewInt("info_errors"),

	Timeouts: expvar.NewInt("timeouts"),

	CacheHits:   expvar.NewInt("cache_hits"),
	CacheMisses: expvar.NewInt("cache_misses"),
}

// PrometheusMetrics keeps all the metrics exposed on /metrics endpoint
type PrometheusMetrics struct {
	Requests                  prometheus.Counter
	Responses                 *prometheus.CounterVec
	RenderMismatches          prometheus.Counter
	RenderFixedMismatches     prometheus.Counter
	RenderMismatchedResponses prometheus.Counter
	Renders                   prometheus.Counter
	FindNotFound              prometheus.Counter
	RequestCancel             *prometheus.CounterVec
	DurationExp               prometheus.Histogram
	DurationLin               prometheus.Histogram
	RenderDurationExp         prometheus.Histogram
	RenderOutDurationExp      *prometheus.HistogramVec
	FindDurationExp           prometheus.Histogram
	FindDurationLin           prometheus.Histogram
	FindOutDuration           *prometheus.HistogramVec
	TimeInQueueSeconds        *prometheus.HistogramVec

	TLDCacheProbeReqTotal  prometheus.Counter
	TLDCacheProbeErrors    prometheus.Counter
	TLDCacheHostsPerDomain prometheus.GaugeVec

	PathCacheFilteredRequests prometheus.Counter
	BackendResponses          *prometheus.CounterVec
}

// NewPrometheusMetrics creates a set of default Prom metrics
func NewPrometheusMetrics(config cfg.Zipper) *PrometheusMetrics {
	return &PrometheusMetrics{
		Requests: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "http_request_total",
				Help: "Count of HTTP requests",
			},
		),
		Responses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_responses_total",
				Help: "Count of HTTP responses, partitioned by return code and handler",
			},
			[]string{"code", "handler"},
		),
		RenderMismatchedResponses: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "render_mismatched_responses_total",
				Help: "Count of mismatched (unfixed) render responses",
			},
		),
		RenderFixedMismatches: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "render_fixed_mismatches_total",
				Help: "Count of fixed mismatched rendered data points",
			},
		),
		RenderMismatches: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "render_mismatches_total",
				Help: "Count of mismatched rendered data points",
			},
		),
		Renders: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "render_total",
				Help: "Count of rendered data points",
			},
		),
		FindNotFound: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "find_not_found",
				Help: "Count of not-found /find responses",
			},
		),
		RequestCancel: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "request_cancel",
				Help: "Context cancellations or incoming requests due to manual cancels or timeouts",
			},
			[]string{"handler", "cause"},
		),
		DurationExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_exp",
				Help: "The duration of HTTP requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.RequestDurationExp.Start,
					config.Monitoring.RequestDurationExp.BucketSize,
					config.Monitoring.RequestDurationExp.BucketsNum),
			},
		),
		DurationLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_lin",
				Help: "The duration of HTTP requests (linear)",
				Buckets: prometheus.LinearBuckets(
					config.Monitoring.RequestDurationLin.Start,
					config.Monitoring.RequestDurationLin.BucketSize,
					config.Monitoring.RequestDurationLin.BucketsNum),
			},
		),
		RenderDurationExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "render_request_duration_seconds_exp",
				Help: "The duration of render requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.RenderDurationExp.Start,
					config.Monitoring.RenderDurationExp.BucketSize,
					config.Monitoring.RenderDurationExp.BucketsNum),
			},
		),
		RenderOutDurationExp: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "render_outbound_request_duration_seconds_exp",
				Help: "The durations of render requests sent to storages (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					// TODO (grzkv) Do we need a separate config?
					// The buckets should be of comparable size.
					config.Monitoring.RenderDurationExp.Start,
					config.Monitoring.RenderDurationExp.BucketSize,
					config.Monitoring.RenderDurationExp.BucketsNum),
			},
			[]string{"dc", "cluster"},
		),
		FindDurationExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "find_request_duration_seconds_exp",
				Help: "The duration of find requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.FindDurationExp.Start,
					config.Monitoring.FindDurationExp.BucketSize,
					config.Monitoring.FindDurationExp.BucketsNum),
			},
		),
		FindDurationLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "find_request_duration_seconds_lin",
				Help: "The duration of find requests (linear), in ms",
				Buckets: prometheus.LinearBuckets(
					config.Monitoring.FindDurationLin.Start,
					config.Monitoring.FindDurationLin.BucketSize,
					config.Monitoring.FindDurationLin.BucketsNum),
			},
		),
		FindOutDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "find_out_duration_seconds",
				Help: "Duration of outgoing find requests per backend cluster.",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.FindOutDuration.Start,
					config.Monitoring.FindOutDuration.BucketSize,
					config.Monitoring.FindOutDuration.BucketsNum),
			},
			[]string{"cluster"},
		),
		TimeInQueueSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "time_in_queue",
				Help: "Time a request spends in queue in seconds.",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.TimeInQueueExpHistogram.Start/1000, // converstion ms -> s
					config.Monitoring.TimeInQueueExpHistogram.BucketSize,
					config.Monitoring.TimeInQueueExpHistogram.BucketsNum),
			},
			[]string{"request"},
		),
		TLDCacheProbeReqTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "tldcache_probe_req_total",
				Help: "The total number of find requests sent by TLD cache as probes.",
			},
		),
		TLDCacheProbeErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "tldcache_probe_errors_total",
				Help: "The total number of failed find requests sent by TLD cache as probes.",
			},
		),
		TLDCacheHostsPerDomain: *prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "tldcache_num_hosts_per_domain",
				Help: "The number of hosts per top-level domain.",
			},
			[]string{"domain"},
		),
		PathCacheFilteredRequests: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "path_cache_filtered_requests_total",
				Help: "The total number of requests with successful backend filter by path caches",
			},
		),
		BackendResponses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "backend_responses_total",
				Help: "Count of backend responses, partitioned by return code and handler",
			},
			[]string{"code", "handler"},
		),
	}
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

func initGraphite(app *App) {
	// register our metrics with graphite
	graphite := g2g.NewGraphite(app.Config.Graphite.Host, app.Config.Graphite.Interval, 10*time.Second)

	/* #nosec */
	hostname, _ := os.Hostname()
	hostname = strings.Replace(hostname, ".", "_", -1)

	prefix := app.Config.Graphite.Prefix

	pattern := app.Config.Graphite.Pattern
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

	for i := 0; i <= app.Config.Buckets; i++ {
		graphite.Register(fmt.Sprintf("%s.requests_in_%dms_to_%dms", pattern, i*100, (i+1)*100), bucketEntry(i))
		lower, upper := util.Bounds(i)
		graphite.Register(fmt.Sprintf("%s.exp.requests_in_%05dms_to_%05dms", pattern, lower, upper), expBucketEntry(i))
	}

	graphite.Register(fmt.Sprintf("%s.cache_size", pattern), Metrics.CacheSize)
	graphite.Register(fmt.Sprintf("%s.cache_items", pattern), Metrics.CacheItems)

	graphite.Register(fmt.Sprintf("%s.cache_hits", pattern), Metrics.CacheHits)
	graphite.Register(fmt.Sprintf("%s.cache_misses", pattern), Metrics.CacheMisses)

	go mstats.Start(app.Config.Graphite.Interval)

	graphite.Register(fmt.Sprintf("%s.goroutines", pattern), Metrics.Goroutines)
	graphite.Register(fmt.Sprintf("%s.uptime", pattern), Metrics.Uptime)
	graphite.Register(fmt.Sprintf("%s.alloc", pattern), &mstats.Alloc)
	graphite.Register(fmt.Sprintf("%s.total_alloc", pattern), &mstats.TotalAlloc)
	graphite.Register(fmt.Sprintf("%s.num_gc", pattern), &mstats.NumGC)
	graphite.Register(fmt.Sprintf("%s.pause_ns", pattern), &mstats.PauseNS)
}

func metricsServer(app *App) *http.Server {
	prometheus.MustRegister(app.Metrics.Requests)
	prometheus.MustRegister(app.Metrics.Responses)
	prometheus.MustRegister(app.Metrics.Renders)
	prometheus.MustRegister(app.Metrics.RenderMismatches)
	prometheus.MustRegister(app.Metrics.RenderFixedMismatches)
	prometheus.MustRegister(app.Metrics.RenderMismatchedResponses)
	prometheus.MustRegister(app.Metrics.FindNotFound)
	prometheus.MustRegister(app.Metrics.RequestCancel)
	prometheus.MustRegister(app.Metrics.DurationExp)
	prometheus.MustRegister(app.Metrics.DurationLin)
	prometheus.MustRegister(app.Metrics.RenderDurationExp)
	prometheus.MustRegister(app.Metrics.RenderOutDurationExp)
	prometheus.MustRegister(app.Metrics.FindDurationExp)
	prometheus.MustRegister(app.Metrics.FindDurationLin)
	prometheus.MustRegister(app.Metrics.FindOutDuration)
	prometheus.MustRegister(app.Metrics.TimeInQueueSeconds)

	prometheus.MustRegister(app.Metrics.TLDCacheHostsPerDomain)
	prometheus.MustRegister(app.Metrics.TLDCacheProbeErrors)
	prometheus.MustRegister(app.Metrics.TLDCacheProbeReqTotal)

	prometheus.MustRegister(app.Metrics.PathCacheFilteredRequests)
	prometheus.MustRegister(app.Metrics.BackendResponses)

	writeTimeout := app.Config.Timeouts.Global
	if writeTimeout < 30*time.Second {
		writeTimeout = time.Minute
	}

	r := initMetricHandlers()

	s := &http.Server{
		Addr:         app.Config.ListenInternal,
		Handler:      r,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: writeTimeout,
	}

	return s
}

func (app *App) bucketRequestTimes(req *http.Request, t time.Duration) {
	ms := t.Nanoseconds() / int64(time.Millisecond)

	bucket := int(ms / 100)
	bucketIdx := findBucketIndex(timeBuckets, bucket)
	atomic.AddInt64(&timeBuckets[bucketIdx], 1)

	expBucket := util.Bucket(ms, app.Config.Buckets)
	expBucketIdx := findBucketIndex(expTimeBuckets, expBucket)
	atomic.AddInt64(&expTimeBuckets[expBucketIdx], 1)

	app.Metrics.DurationExp.Observe(t.Seconds())
	app.Metrics.DurationLin.Observe(t.Seconds())

	if req.URL.Path == "/render" || req.URL.Path == "/render/" {
		app.Metrics.RenderDurationExp.Observe(t.Seconds())
	}
	if req.URL.Path == "/metrics/find" || req.URL.Path == "/metrics/find/" {
		app.Metrics.FindDurationExp.Observe(t.Seconds())
		app.Metrics.FindDurationLin.Observe(t.Seconds())
	}
}
