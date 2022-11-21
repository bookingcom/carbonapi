package zipper

import (
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/prometheus/client_golang/prometheus"
)

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

	BackendResponses        *prometheus.CounterVec
	ActiveUpstreamRequests  *prometheus.GaugeVec
	WaitingUpstreamRequests *prometheus.GaugeVec
	BackendLimiterEnters    *prometheus.CounterVec
	BackendLimiterExits     *prometheus.CounterVec

	RenderDurationExp    prometheus.Histogram
	RenderOutDurationExp *prometheus.HistogramVec
	FindDurationExp      prometheus.Histogram
	FindDurationLin      prometheus.Histogram
	FindOutDuration      *prometheus.HistogramVec

	BackendEnqueuedRequests    *prometheus.CounterVec
	BackendRequestsInQueue     *prometheus.GaugeVec
	BackendSemaphoreSaturation prometheus.Gauge
	BackendTimeInQSec          *prometheus.HistogramVec

	TimeInQueueSeconds *prometheus.HistogramVec

	TLDCacheProbeReqTotal prometheus.Counter
	TLDCacheProbeErrors   prometheus.Counter

	PathCacheFilteredRequests prometheus.Counter
}

// NewPrometheusMetrics creates a set of default Prom metrics
func NewPrometheusMetrics(config cfg.Zipper, ns string) *PrometheusMetrics {
	return &PrometheusMetrics{
		Requests: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "http_request_total",
				Help:      "Count of HTTP requests",
			},
		),
		Responses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "http_responses_total",
				Help:      "Count of HTTP responses, partitioned by return code and handler",
			},
			[]string{"code", "handler"},
		),
		RenderMismatchedResponses: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "render_mismatched_responses_total",
				Help:      "Count of mismatched (unfixed) render responses",
			},
		),
		RenderFixedMismatches: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "render_fixed_mismatches_total",
				Help:      "Count of fixed mismatched rendered data points",
			},
		),
		RenderMismatches: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "render_mismatches_total",
				Help:      "Count of mismatched rendered data points",
			},
		),
		Renders: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "render_total",
				Help:      "Count of rendered data points",
			},
		),
		FindNotFound: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "find_not_found",
				Help:      "Count of not-found /find responses",
			},
		),
		RequestCancel: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "request_cancel",
				Help:      "Context cancellations or incoming requests due to manual cancels or timeouts",
			},
			[]string{"handler", "cause"},
		),
		ActiveUpstreamRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "active_upstream_requests",
			Help:      "Number of in-flight requests to a backend",
		}, []string{"backend"}),
		WaitingUpstreamRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "waiting_upstream_requests",
			Help:      "Number of requests to a backend currently blocked in the limiter",
		}, []string{"backend"}),
		BackendLimiterEnters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "backend_limiter_enters",
			Help:      "Counter of requests that entered a backend limiter by backend",
		}, []string{"backend"}),
		BackendLimiterExits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "backend_limiter_exits",
			Help:      "Counter of backend limiter exits by backend and by status",
		}, []string{"backend", "status"}),
		RenderDurationExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "render_request_duration_seconds_exp",
				Help:      "The duration of render requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.RenderDurationExp.Start,
					config.Monitoring.RenderDurationExp.BucketSize,
					config.Monitoring.RenderDurationExp.BucketsNum),
			},
		),
		RenderOutDurationExp: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "render_outbound_request_duration_seconds_exp",
				Help:      "The durations of render requests sent to storages (exponential)",
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
				Namespace: ns,
				Name:      "find_request_duration_seconds_exp",
				Help:      "The duration of find requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.FindDurationExp.Start,
					config.Monitoring.FindDurationExp.BucketSize,
					config.Monitoring.FindDurationExp.BucketsNum),
			},
		),
		FindDurationLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "find_request_duration_seconds_lin",
				Help:      "The duration of find requests (linear), in ms",
				Buckets: prometheus.LinearBuckets(
					config.Monitoring.FindDurationLin.Start,
					config.Monitoring.FindDurationLin.BucketSize,
					config.Monitoring.FindDurationLin.BucketsNum),
			},
		),
		FindOutDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "find_out_duration_seconds",
				Help:      "Duration of outgoing find requests per backend cluster.",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.FindOutDuration.Start,
					config.Monitoring.FindOutDuration.BucketSize,
					config.Monitoring.FindOutDuration.BucketsNum),
			},
			[]string{"cluster"},
		),

		BackendEnqueuedRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "backend_enqueued_requests",
				Help:      "The number of requests to backends put in their respective queues.",
			},
			[]string{"request"},
		),
		BackendRequestsInQueue: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "backend_requests_in_queue",
				Help:      "The number of requests currently in the queue by request type.",
			},
			[]string{"request"},
		),
		BackendSemaphoreSaturation: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "backend_semaphore_saturation",
				Help:      "The number of requests currently in flight that saturate the semaphore.",
			},
		),
		BackendTimeInQSec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "backend_time_in_queue",
				Help:      "Time a request to backend spends waiting in queue by request type",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.BackendTimeInQSecHistParams.Start,
					config.Monitoring.BackendTimeInQSecHistParams.BucketSize,
					config.Monitoring.BackendTimeInQSecHistParams.BucketsNum,
				),
			},
			[]string{"request"},
		),

		TimeInQueueSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "time_in_queue",
				Help:      "Time a request spends in queue in seconds.",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.TimeInQueueExpHistogram.Start/1000, // converstion ms -> s
					config.Monitoring.TimeInQueueExpHistogram.BucketSize,
					config.Monitoring.TimeInQueueExpHistogram.BucketsNum),
			},
			[]string{"request"},
		),
		TLDCacheProbeReqTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "tldcache_probe_req_total",
				Help:      "The total number of find requests sent by TLD cache as probes.",
			},
		),
		TLDCacheProbeErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "tldcache_probe_errors_total",
				Help:      "The total number of failed find requests sent by TLD cache as probes.",
			},
		),
		PathCacheFilteredRequests: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "path_cache_filtered_requests_total",
				Help:      "The total number of requests with successful backend filter by path caches",
			},
		),
		BackendResponses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: ns,
				Name:      "backend_responses_total",
				Help:      "Count of backend responses, partitioned by status and request type",
			},
			[]string{"status", "request"},
		),
	}
}

func metricsServer(app *App) {
	prometheus.MustRegister(app.Metrics.Requests)
	prometheus.MustRegister(app.Metrics.Responses)
	prometheus.MustRegister(app.Metrics.Renders)
	prometheus.MustRegister(app.Metrics.RenderMismatches)
	prometheus.MustRegister(app.Metrics.RenderFixedMismatches)
	prometheus.MustRegister(app.Metrics.RenderMismatchedResponses)
	prometheus.MustRegister(app.Metrics.FindNotFound)
	prometheus.MustRegister(app.Metrics.RequestCancel)

	prometheus.MustRegister(app.Metrics.BackendResponses)
	prometheus.MustRegister(app.Metrics.ActiveUpstreamRequests)
	prometheus.MustRegister(app.Metrics.WaitingUpstreamRequests)
	prometheus.MustRegister(app.Metrics.BackendLimiterEnters)
	prometheus.MustRegister(app.Metrics.BackendLimiterExits)

	prometheus.MustRegister(app.Metrics.RenderDurationExp)
	prometheus.MustRegister(app.Metrics.RenderOutDurationExp)
	prometheus.MustRegister(app.Metrics.FindDurationExp)
	prometheus.MustRegister(app.Metrics.FindDurationLin)
	prometheus.MustRegister(app.Metrics.FindOutDuration)
	prometheus.MustRegister(app.Metrics.TimeInQueueSeconds)

	prometheus.MustRegister(app.Metrics.BackendEnqueuedRequests)
	prometheus.MustRegister(app.Metrics.BackendRequestsInQueue)
	prometheus.MustRegister(app.Metrics.BackendSemaphoreSaturation)
	prometheus.MustRegister(app.Metrics.BackendTimeInQSec)

	prometheus.MustRegister(app.Metrics.TLDCacheProbeErrors)
	prometheus.MustRegister(app.Metrics.TLDCacheProbeReqTotal)

	prometheus.MustRegister(app.Metrics.PathCacheFilteredRequests)
}
