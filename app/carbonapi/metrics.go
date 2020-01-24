package carbonapi

import (
	"expvar"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetrics are metrix exported via /metrics endpoint for Prom scraping
type PrometheusMetrics struct {
	Requests                  prometheus.Counter
	Responses                 *prometheus.CounterVec
	FindNotFound              prometheus.Counter
	RenderPartialFail         prometheus.Counter
	RequestCancel             *prometheus.CounterVec
	DurationExp               prometheus.Histogram
	DurationLin               prometheus.Histogram
	RenderDurationExp         prometheus.Histogram
	RenderDurationLinSimple   prometheus.Histogram
	RenderDurationExpSimple   prometheus.Histogram
	RenderDurationExpComplex  prometheus.Histogram
	RenderDurationPerPointExp prometheus.Histogram
	FindDurationExp           prometheus.Histogram
	FindDurationLin           prometheus.Histogram
	FindDurationLinSimple     prometheus.Histogram
	FindDurationLinComplex    prometheus.Histogram
	TimeInQueueExp            prometheus.Histogram
	TimeInQueueLin            prometheus.Histogram
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
			[]string{"code", "handler", "from_cache"},
		),
		FindNotFound: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "find_not_found",
				Help: "Count of not-found /find responses",
			},
		),
		RenderPartialFail: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "render_part_fail",
				Help: "Count of /render requests that partially failed",
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
					config.Zipper.Common.Monitoring.RequestDurationExp.Start,
					config.Zipper.Common.Monitoring.RequestDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.RequestDurationExp.BucketsNum),
			},
		),
		DurationLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_lin",
				Help: "The duration of HTTP requests (linear)",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.RequestDurationLin.Start,
					config.Zipper.Common.Monitoring.RequestDurationLin.BucketSize,
					config.Zipper.Common.Monitoring.RequestDurationLin.BucketsNum),
			},
		),
		RenderDurationExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "render_request_duration_seconds_exp",
				Help: "The duration of render requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.RenderDurationExp.Start,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketsNum),
			},
		),
		RenderDurationLinSimple: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "render_request_duration_seconds_lin_simple",
				Help: "The duration of render requests (linear)",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.RenderDurationLinSimple.Start,
					config.Zipper.Common.Monitoring.RenderDurationLinSimple.BucketSize,
					config.Zipper.Common.Monitoring.RenderDurationLinSimple.BucketsNum),
			},
		),
		RenderDurationExpSimple: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "render_request_duration_seconds_exp_simple",
				Help: "The duration of simple render requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.RenderDurationExp.Start,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketsNum),
			},
		),
		RenderDurationExpComplex: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "render_request_duration_seconds_exp_complex",
				Help: "The duration of complex render requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.RenderDurationExp.Start,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketsNum),
			},
		),
		RenderDurationPerPointExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "render_request_duration_perpoint_milliseconds_exp",
				Help: "The duration of render requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.RenderDurationExp.Start/10,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.RenderDurationExp.BucketsNum),
			},
		),
		FindDurationExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "find_request_duration_seconds_exp",
				Help: "The duration of find requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Zipper.Common.Monitoring.FindDurationExp.Start,
					config.Zipper.Common.Monitoring.FindDurationExp.BucketSize,
					config.Zipper.Common.Monitoring.FindDurationExp.BucketsNum),
			},
		),
		FindDurationLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "find_request_duration_seconds_lin",
				Help: "The duration of find requests (linear), in ms",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.FindDurationLin.Start,
					config.Zipper.Common.Monitoring.FindDurationLin.BucketSize,
					config.Zipper.Common.Monitoring.FindDurationLin.BucketsNum),
			},
		),
		FindDurationLinSimple: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "find_request_duration_seconds_lin_simple",
				Help: "The duration of simple find requests (linear), in ms",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.FindDurationLinSimple.Start,
					config.Zipper.Common.Monitoring.FindDurationLinSimple.BucketSize,
					config.Zipper.Common.Monitoring.FindDurationLinSimple.BucketsNum),
			},
		),
		FindDurationLinComplex: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "find_request_duration_seconds_lin_complex",
				Help: "The duration of complex find requests (linear), in ms",
				Buckets: prometheus.LinearBuckets(
					config.Zipper.Common.Monitoring.FindDurationLinComplex.Start,
					config.Zipper.Common.Monitoring.FindDurationLinComplex.BucketSize,
					config.Zipper.Common.Monitoring.FindDurationLinComplex.BucketsNum),
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

var apiMetrics = struct {
	// Total counts across all request types
	// TODO duplicate
	Requests *expvar.Int
	// TODO duplicate
	Responses *expvar.Int
	Errors    *expvar.Int

	Goroutines expvar.Func
	Uptime     expvar.Func

	// TODO (grzkv) Move to Prom
	// Despite the names, these only count /render requests
	// TODO duplicate
	RenderRequests        *expvar.Int
	RequestCacheHits      *expvar.Int
	RequestCacheMisses    *expvar.Int
	RenderCacheOverheadNS *expvar.Int

	// TODO (grzkv) Move to Prom
	// TODO duplicate
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

	FindRequests:        expvar.NewInt("find_requests"),
	FindCacheHits:       expvar.NewInt("find_cache_hits"),
	FindCacheMisses:     expvar.NewInt("find_cache_misses"),
	FindCacheOverheadNS: expvar.NewInt("find_cache_overhead_ns"),
}

// TODO (grzkv): Move to Prometheus, as these are not runtime metrics.
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
