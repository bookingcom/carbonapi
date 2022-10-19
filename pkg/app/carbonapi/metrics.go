package carbonapi

import (
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetrics are metrix exported via /metrics endpoint for Prom scraping
type PrometheusMetrics struct {
	Requests          prometheus.Counter
	Responses         *prometheus.CounterVec
	FindNotFound      prometheus.Counter
	RenderPartialFail prometheus.Counter
	RequestCancel     *prometheus.CounterVec
	DurationExp       prometheus.Histogram
	DurationLin       prometheus.Histogram

	RenderDurationExp         prometheus.Histogram
	RenderDurationLinSimple   prometheus.Histogram
	RenderDurationExpSimple   prometheus.Histogram
	RenderDurationExpComplex  prometheus.Histogram
	RenderDurationPerPointExp prometheus.Histogram

	FindDurationExp        prometheus.Histogram
	FindDurationLin        prometheus.Histogram
	FindDurationLinSimple  prometheus.Histogram
	FindDurationLinComplex prometheus.Histogram

	UpstreamRequestsInQueue     *prometheus.GaugeVec
	UpstreamSemaphoreSaturation prometheus.Gauge
	UpstreamEnqueuedRequests    *prometheus.CounterVec
	UpstreamSubRenderNum        prometheus.Histogram
	UpstreamTimeInQSec          *prometheus.HistogramVec

	TimeInQueueExp prometheus.Histogram
	TimeInQueueLin prometheus.Histogram

	UpstreamRequests        *prometheus.CounterVec
	ActiveUpstreamRequests  prometheus.Gauge
	WaitingUpstreamRequests prometheus.Gauge
	UpstreamLimiterEnters   prometheus.Counter
	UpstreamLimiterExits    *prometheus.CounterVec

	CacheRequests *prometheus.CounterVec
	CacheRespRead *prometheus.CounterVec
	CacheTimeouts *prometheus.CounterVec
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
		UpstreamRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "upstream_requests_total",
				Help: "The number of requests that are propagated to be queried upstream and forwarded to backends",
			}, []string{"request"},
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

		UpstreamRequestsInQueue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "upstream_requests_in_queue",
			Help: "The number of upstream requests in the main processing queue.",
		}, []string{"queue"}),
		UpstreamSemaphoreSaturation: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "upstream_semaphore_saturation",
			Help: "The number of requests put in the main queue semaphore. Needs to be compared to the semaphore size.",
		}),
		UpstreamEnqueuedRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upstream_enqueued_requests",
			Help: "The count of requests put into the queue.",
		}, []string{"queue"}),
		UpstreamSubRenderNum: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "upstream_render_subrequests_num",
			Help: "The number of sub-requests for a render request after breaking the globs.",
			Buckets: prometheus.ExponentialBuckets(
				config.UpstreamSubRenderNumHistParams.Start,
				config.UpstreamSubRenderNumHistParams.BucketSize,
				config.UpstreamSubRenderNumHistParams.BucketsNum,
			),
		}),
		UpstreamTimeInQSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "upstream_time_in_q_sec",
			Help: "Duration between entering and exiting the queue.",
			Buckets: prometheus.ExponentialBuckets(
				config.UpstreamTimeInQSecHistParams.Start,
				config.UpstreamTimeInQSecHistParams.BucketSize,
				config.UpstreamTimeInQSecHistParams.BucketsNum,
			),
		}, []string{"queue"}),

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
		ActiveUpstreamRequests: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "active_upstream_requests",
				Help: "Number of in-flight upstream requests",
			},
		),
		WaitingUpstreamRequests: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "waiting_upstream_requests",
				Help: "Number of upstream requests waiting on the limiter",
			},
		),
		UpstreamLimiterEnters: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "upstream_limiter_enters",
			Help: "The counter of requests that entered the upstream limiter",
		}),
		UpstreamLimiterExits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upstream_limiter_exits",
			Help: "The counter of requests that exit the limiter by status",
		}, []string{"status"}),
		CacheRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cache_requests",
				Help: "Counter of requests to the top-level cache",
			},
			[]string{"request", "operation"},
		),
		CacheRespRead: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cache_resp_read",
				Help: "Counter of responses from the top-level cache that we have acutally read",
			},
			[]string{"request", "operation", "status"},
		),
		CacheTimeouts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cache_timeouts",
				Help: "Counter of top-level cache timed-out requests",
			},
			[]string{"request"},
		),
	}
}
