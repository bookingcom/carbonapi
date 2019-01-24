package zipper

import (
	"expvar"
	"strconv"
	"sync/atomic"

	"github.com/bookingcom/carbonapi/cfg"
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
	Requests     prometheus.Counter
	Responses    *prometheus.CounterVec
	DurationsExp prometheus.Histogram
	DurationsLin prometheus.Histogram
	TimeInQueue  prometheus.Histogram
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
				Name: "http_response_total",
				Help: "Count of HTTP responses, partitioned by return code and handler",
			},
			[]string{"code", "handler"},
		),
		DurationsExp: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_exp",
				Help: "The duration of HTTP requests (exponential)",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.RequestDurationExp.Start,
					config.Monitoring.RequestDurationExp.BucketSize,
					config.Monitoring.RequestDurationExp.BucketsNum),
			},
		),
		DurationsLin: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "http_request_duration_seconds_lin",
				Help: "The duration of HTTP requests (linear)",
				Buckets: prometheus.LinearBuckets(
					config.Monitoring.RequestDurationLin.Start,
					config.Monitoring.RequestDurationLin.BucketSize,
					config.Monitoring.RequestDurationLin.BucketsNum),
			},
		),
		TimeInQueue: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "time_in_queue_ms_exp",
				Help: "Time a request spends in queue (exponential), ms",
				Buckets: prometheus.ExponentialBuckets(
					config.Monitoring.TimeInQueueExpHistogram.Start,
					config.Monitoring.TimeInQueueExpHistogram.BucketSize,
					config.Monitoring.TimeInQueueExpHistogram.BucketsNum),
			},
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
