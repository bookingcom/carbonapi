package zipper

import (
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetrics keeps all the metrics exposed on /metrics endpoint
type PrometheusMetrics struct {
	RenderMismatches          prometheus.Counter
	RenderFixedMismatches     prometheus.Counter
	RenderMismatchedResponses prometheus.Counter
	Renders                   prometheus.Counter

	BackendResponses *prometheus.CounterVec

	RenderOutDurationExp *prometheus.HistogramVec
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
func NewPrometheusMetrics(config cfg.Zipper) *PrometheusMetrics {
	return &PrometheusMetrics{
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

		BackendEnqueuedRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "backend_enqueued_requests",
				Help: "The number of requests to backends put in their respective queues.",
			},
			[]string{"request"},
		),
		BackendRequestsInQueue: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "backend_requests_in_queue",
				Help: "The number of requests currently in the queue by request type.",
			},
			[]string{"request"},
		),
		BackendSemaphoreSaturation: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "backend_semaphore_saturation",
				Help: "The number of requests currently in flight that saturate the semaphore.",
			},
		),
		BackendTimeInQSec: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "backend_time_in_queue",
				Help: "Time a request to backend spends waiting in queue by request type",
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
		PathCacheFilteredRequests: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "path_cache_filtered_requests_total",
				Help: "The total number of requests with successful backend filter by path caches",
			},
		),
		BackendResponses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "backend_responses_total",
				Help: "Count of backend responses, partitioned by status and request type",
			},
			[]string{"status", "request"},
		),
	}
}
