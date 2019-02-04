package carbonapi

import (
	"expvar"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetrics are metrix exported via /metrics endpoint for Prom scraping
type PrometheusMetrics struct {
	Requests       prometheus.Counter
	Responses      *prometheus.CounterVec
	DurationsExp   prometheus.Histogram
	DurationsLin   prometheus.Histogram
	TimeInQueueExp prometheus.Histogram
	TimeInQueueLin prometheus.Histogram
}

func newPrometheusMetrics(config cfg.API) PrometheusMetrics {
	return PrometheusMetrics{
		Requests: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "http_requests_total",
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
		DurationsExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_exp",
				Help: "The duration of HTTP requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.RequestDurationExp.Start,
					config.Zipper.Common.Monitoring.RequestDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.RequestDurationExp.BucketsNum),
			},
		),
		DurationsLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_lin",
				Help: "The duration of HTTP requests (linear)",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.RequestDurationLin.Start,
					config.Zipper.Common.Monitoring.RequestDurationLin.BucketSize,
					config.Zipper.Common.Monitoring.RequestDurationLin.BucketsNum),
			},
		),
		TimeInQueueExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "time_in_queue_ms_exp",
				Help: "Time a request to backend spends in queue (exponential), in ms",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.TimeInQueueExpHistogram.Start,
					config.Zipper.Common.Monitoring.TimeInQueueExpHistogram.BucketSize,
					config.Zipper.Common.Monitoring.TimeInQueueExpHistogram.BucketsNum),
			},
		),
		TimeInQueueLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "time_in_queue_ms_lin",
				Help: "Time a request to backend spends in queue (linear), in ms",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.TimeInQueueLinHistogram.Start,
					config.Zipper.Common.Monitoring.TimeInQueueLinHistogram.BucketSize,
					config.Zipper.Common.Monitoring.TimeInQueueLinHistogram.BucketsNum),
			},
		),
	}
}

// TODO (grzkv): Remove from global scope
// TODO (grzkv): Consider substituting this with Prometheus
var apiMetrics = struct {
	// Total counts across all request types
	Requests  *expvar.Int
	Responses *expvar.Int
	Errors    *expvar.Int

	Goroutines    expvar.Func
	Uptime        expvar.Func
	LimiterUse    expvar.Func
	LimiterUseMax expvar.Func

	// Despite the names, these only count /render requests
	RenderRequests        *expvar.Int
	RequestCacheHits      *expvar.Int
	RequestCacheMisses    *expvar.Int
	RenderCacheOverheadNS *expvar.Int

	FindRequests        *expvar.Int
	FindCacheHits       *expvar.Int
	FindCacheMisses     *expvar.Int
	FindCacheOverheadNS *expvar.Int

	MemcacheTimeouts expvar.Func

	CacheSize  expvar.Func
	CacheItems expvar.Func
}{
	Requests:  expvar.NewInt("requests"),
	Responses: expvar.NewInt("responses"),
	Errors:    expvar.NewInt("errors"),

	// TODO: request_cache -> render_cache
	RenderRequests:        expvar.NewInt("render_requests"),
	RequestCacheHits:      expvar.NewInt("request_cache_hits"),
	RequestCacheMisses:    expvar.NewInt("request_cache_misses"),
	RenderCacheOverheadNS: expvar.NewInt("render_cache_overhead_ns"),

	FindRequests: expvar.NewInt("find_requests"),

	FindCacheHits:       expvar.NewInt("find_cache_hits"),
	FindCacheMisses:     expvar.NewInt("find_cache_misses"),
	FindCacheOverheadNS: expvar.NewInt("find_cache_overhead_ns"),
}

// TODO (grzkv): Remove from global scope
var zipperMetrics = struct {
	FindRequests *expvar.Int
	FindErrors   *expvar.Int

	RenderRequests *expvar.Int
	RenderErrors   *expvar.Int

	InfoRequests *expvar.Int
	InfoErrors   *expvar.Int

	Timeouts *expvar.Int

	CacheSize  expvar.Func
	CacheItems expvar.Func

	CacheMisses *expvar.Int
	CacheHits   *expvar.Int
}{
	FindRequests: expvar.NewInt("zipper_find_requests"),
	FindErrors:   expvar.NewInt("zipper_find_errors"),

	RenderRequests: expvar.NewInt("zipper_render_requests"),
	RenderErrors:   expvar.NewInt("zipper_render_errors"),

	InfoRequests: expvar.NewInt("zipper_info_requests"),
	InfoErrors:   expvar.NewInt("zipper_info_errors"),

	Timeouts: expvar.NewInt("zipper_timeouts"),

	CacheHits:   expvar.NewInt("zipper_cache_hits"),
	CacheMisses: expvar.NewInt("zipper_cache_misses"),
}
