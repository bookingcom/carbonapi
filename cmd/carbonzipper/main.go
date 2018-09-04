package main

import (
	"context"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-graphite/carbonapi/cfg"
	"github.com/go-graphite/carbonapi/intervalset"
	"github.com/go-graphite/carbonapi/mstats"
	"github.com/go-graphite/carbonapi/util"
	"github.com/go-graphite/carbonapi/zipper"

	"sort"

	"github.com/dgryski/httputil"
	"github.com/facebookgo/grace/gracehttp"
	"github.com/facebookgo/pidfile"
	pb3 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	pickle "github.com/lomik/og-rek"
	"github.com/lomik/zapwriter"
	"github.com/peterbourgon/g2g"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var prometheusMetrics = struct {
	Requests  prometheus.Counter
	Responses *prometheus.CounterVec
	Durations prometheus.Histogram
}{
	Requests: prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "zipper_http_request_total",
			Help: "Count of HTTP requests",
		},
	),
	Responses: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zipper_http_response_total",
			Help: "Count of HTTP responses, partitioned by return code and handler",
		},
		[]string{"code", "handler"},
	),
	Durations: prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "zipper_http_request_duration_seconds",
			Help:    "The duration of HTTP requests",
			Buckets: prometheus.ExponentialBuckets(50.0, 2.0, 10),
		},
	),
}

// config contains necessary information for global
var config = struct {
	cfg.Zipper
	zipper *zipper.Zipper
}{
	cfg.DefaultZipperConfig,
	nil,
}

// Metrics contains grouped expvars for /debug/vars and graphite
var Metrics = struct {
	Requests  *expvar.Int
	Responses *expvar.Int
	Errors    *expvar.Int

	Goroutines    expvar.Func
	Uptime        expvar.Func
	LimiterUse    expvar.Func
	LimiterUseMax expvar.Func

	FindRequests *expvar.Int
	FindErrors   *expvar.Int

	SearchRequests *expvar.Int

	RenderRequests *expvar.Int
	RenderErrors   *expvar.Int

	InfoRequests *expvar.Int
	InfoErrors   *expvar.Int

	Timeouts *expvar.Int

	CacheSize         expvar.Func
	CacheItems        expvar.Func
	CacheMisses       *expvar.Int
	CacheHits         *expvar.Int
	SearchCacheSize   expvar.Func
	SearchCacheItems  expvar.Func
	SearchCacheMisses *expvar.Int
	SearchCacheHits   *expvar.Int

	Corruption *expvar.Float
}{
	Requests:  expvar.NewInt("requests"),
	Responses: expvar.NewInt("responses"),
	Errors:    expvar.NewInt("errors"),

	FindRequests: expvar.NewInt("find_requests"),
	FindErrors:   expvar.NewInt("find_errors"),

	SearchRequests: expvar.NewInt("search_requests"),

	RenderRequests: expvar.NewInt("render_requests"),
	RenderErrors:   expvar.NewInt("render_errors"),

	InfoRequests: expvar.NewInt("info_requests"),
	InfoErrors:   expvar.NewInt("info_errors"),

	Timeouts: expvar.NewInt("timeouts"),

	CacheHits:         expvar.NewInt("cache_hits"),
	CacheMisses:       expvar.NewInt("cache_misses"),
	SearchCacheHits:   expvar.NewInt("search_cache_hits"),
	SearchCacheMisses: expvar.NewInt("search_cache_misses"),

	Corruption: expvar.NewFloat("corruption"),
}

// BuildVersion is defined at build and reported at startup and as expvar
var BuildVersion = "(development version)"

// set during startup, read-only after that
var searchConfigured = false

const (
	contentTypeJSON     = "application/json"
	contentTypeProtobuf = "application/x-protobuf"
	contentTypePickle   = "application/pickle"
)

const (
	formatTypeEmpty         = ""
	formatTypePickle        = "pickle"
	formatTypeJSON          = "json"
	formatTypeProtobuf      = "protobuf"
	formatTypeProtobuf3     = "protobuf3"
	formatTypeV2            = "v2"
	formatTypeCarbonAPIV2PB = "carbonapi_v2_pb"
)

func findHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(req.Context(), config.Timeouts.Global)
	defer cancel()

	logger := zapwriter.Logger("find").With(
		zap.String("handler", "find"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	if ce := logger.Check(zap.DebugLevel, "got find request"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	originalQuery := req.FormValue("query")
	format := req.FormValue("format")

	Metrics.Requests.Add(1)
	prometheusMetrics.Requests.Inc()
	Metrics.FindRequests.Add(1)

	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "find"),
		zap.String("format", format),
		zap.String("target", originalQuery),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	metrics, stats, err := config.zipper.Find(ctx, logger, originalQuery)
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].Path < metrics[j].Path {
			return true
		}
		if metrics[i].Path > metrics[j].Path {
			return false
		}
		return metrics[i].Path < metrics[j].Path
	})
	sendStats(stats)
	if err != nil {
		accessLogger.Error("find failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", err.Error()),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "error fetching the data", http.StatusInternalServerError)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "find").Inc()
		return
	}

	err = encodeFindResponse(format, originalQuery, w, metrics)
	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		accessLogger.Error("render failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "find").Inc()
		return
	}
	accessLogger.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	Metrics.Responses.Add(1)
	prometheusMetrics.Responses.WithLabelValues("200", "find").Inc()
}

func encodeFindResponse(format, query string, w http.ResponseWriter, metrics []pb3.GlobMatch) error {
	var err error
	var b []byte
	switch format {
	case formatTypeProtobuf, formatTypeProtobuf3:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		var result pb3.GlobResponse
		result.Name = query
		result.Matches = metrics
		b, err = result.Marshal()
		/* #nosec */
		_, _ = w.Write(b)
	case formatTypeJSON:
		w.Header().Set("Content-Type", contentTypeJSON)
		jEnc := json.NewEncoder(w)
		err = jEnc.Encode(metrics)
	case formatTypeEmpty, formatTypePickle:
		w.Header().Set("Content-Type", contentTypePickle)

		var result []map[string]interface{}

		now := int32(time.Now().Unix() + 60)
		for _, metric := range metrics {
			// Tell graphite-web that we have everything
			var mm map[string]interface{}
			if config.GraphiteWeb09Compatibility {
				// graphite-web 0.9.x
				mm = map[string]interface{}{
					// graphite-web 0.9.x
					"metric_path": metric.Path,
					"isLeaf":      metric.IsLeaf,
				}
			} else {
				// graphite-web 1.0
				interval := &intervalset.IntervalSet{Start: 0, End: now}
				mm = map[string]interface{}{
					"is_leaf":   metric.IsLeaf,
					"path":      metric.Path,
					"intervals": interval,
				}
			}
			result = append(result, mm)
		}

		pEnc := pickle.NewEncoder(w)
		err = pEnc.Encode(result)
	}
	return err
}

func renderHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	memoryUsage := 0

	ctx, cancel := context.WithTimeout(req.Context(), config.Timeouts.Global)
	defer cancel()

	logger := zapwriter.Logger("render").With(
		zap.Int("memory_usage_bytes", memoryUsage),
		zap.String("handler", "render"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	if ce := logger.Check(zap.DebugLevel, "got render request"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	Metrics.Requests.Add(1)
	prometheusMetrics.Requests.Inc()
	Metrics.RenderRequests.Add(1)

	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "render"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	err := req.ParseForm()
	if err != nil {
		http.Error(w, "failed to parse arguments", http.StatusBadRequest)
		accessLogger.Error("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "failed to parse arguments"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
		return
	}

	target := req.FormValue("target")
	format := req.FormValue("format")
	accessLogger = accessLogger.With(
		zap.String("format", format),
		zap.String("target", target),
	)

	from, err := strconv.Atoi(req.FormValue("from"))
	if err != nil {
		http.Error(w, "from is not a integer", http.StatusBadRequest)
		accessLogger.Error("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "from is not a integer"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
		return
	}

	until, err := strconv.Atoi(req.FormValue("until"))
	if err != nil {
		http.Error(w, "until is not a integer", http.StatusBadRequest)
		accessLogger.Error("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "until is not a integer"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
		return
	}

	if target == "" {
		http.Error(w, "empty target", http.StatusBadRequest)
		accessLogger.Error("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "empty target"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
		return
	}

	metrics, stats, err := config.zipper.Render(ctx, logger, target, int32(from), int32(until))
	sendStats(stats)
	if err != nil {
		http.Error(w, "error fetching the data", http.StatusInternalServerError)
		accessLogger.Error("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "render").Inc()
		return
	}

	var b []byte
	switch format {
	case formatTypeProtobuf, formatTypeProtobuf3:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		b, err = metrics.Marshal()

		memoryUsage += len(b)
		/* #nosec */
		_, _ = w.Write(b)
	case formatTypeJSON:
		presponse := createRenderResponse(metrics, nil)
		w.Header().Set("Content-Type", contentTypeJSON)
		e := json.NewEncoder(w)
		err = e.Encode(presponse)
	case formatTypeEmpty, formatTypePickle:
		presponse := createRenderResponse(metrics, pickle.None{})
		w.Header().Set("Content-Type", contentTypePickle)
		e := pickle.NewEncoder(w)
		err = e.Encode(presponse)
	}

	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		accessLogger.Error("render failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "render").Inc()
		return
	}

	accessLogger.Info("request served",
		zap.Int("memory_usage_bytes", memoryUsage),
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	Metrics.Responses.Add(1)
	prometheusMetrics.Responses.WithLabelValues("200", "render").Inc()
}

func createRenderResponse(metrics *pb3.MultiFetchResponse, missing interface{}) []map[string]interface{} {

	var response []map[string]interface{}

	for _, metric := range metrics.GetMetrics() {

		var pvalues []interface{}
		for i, v := range metric.Values {
			if metric.IsAbsent[i] {
				pvalues = append(pvalues, missing)
			} else {
				pvalues = append(pvalues, v)
			}
		}

		// create the response
		presponse := map[string]interface{}{
			"start":  metric.StartTime,
			"step":   metric.StepTime,
			"end":    metric.StopTime,
			"name":   metric.Name,
			"values": pvalues,
		}
		response = append(response, presponse)
	}

	return response
}

func infoHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(req.Context(), config.Timeouts.Global)
	defer cancel()

	logger := zapwriter.Logger("info").With(
		zap.String("handler", "info"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	if ce := logger.Check(zap.DebugLevel, "request"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	Metrics.Requests.Add(1)
	prometheusMetrics.Requests.Inc()
	Metrics.InfoRequests.Add(1)

	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "info"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)
	err := req.ParseForm()
	if err != nil {
		http.Error(w, "failed to parse arguments", http.StatusBadRequest)
		accessLogger.Error("request failed",
			zap.String("reason", "failed to parse arguments"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "info").Inc()
		return
	}

	target := req.FormValue("target")
	format := req.FormValue("format")

	accessLogger = accessLogger.With(
		zap.String("target", target),
		zap.String("format", format),
	)

	if target == "" {
		accessLogger.Error("info failed",
			zap.Int("http_code", http.StatusBadRequest),
			zap.String("reason", "empty target"),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "info: empty target", http.StatusBadRequest)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "info").Inc()
		return
	}

	infos, stats, err := config.zipper.Info(ctx, logger, target)
	sendStats(stats)
	if err != nil {
		accessLogger.Error("info failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", err.Error()),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "info: error processing request", http.StatusInternalServerError)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "info").Inc()
		return
	}

	var b []byte
	switch format {
	case formatTypeProtobuf, formatTypeProtobuf3:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		var result pb3.ZipperInfoResponse
		result.Responses = make([]pb3.ServerInfoResponse, len(infos))
		for s, i := range infos {
			var r pb3.ServerInfoResponse
			r.Server = s
			r.Info = &i
			result.Responses = append(result.Responses, r)
		}
		b, err = result.Marshal()
		/* #nosec */
		_, _ = w.Write(b)
	case formatTypeEmpty, formatTypeJSON:
		w.Header().Set("Content-Type", contentTypeJSON)
		jEnc := json.NewEncoder(w)
		err = jEnc.Encode(infos)
	}
	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		accessLogger.Error("info failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "info").Inc()
		return
	}
	accessLogger.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	Metrics.Responses.Add(1)
	prometheusMetrics.Responses.WithLabelValues("200", "info").Inc()
}

func lbCheckHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := zapwriter.Logger("loadbalancer").With(zap.String("handler", "loadbalancer"))
	accessLogger := zapwriter.Logger("access").With(zap.String("handler", "loadbalancer"))

	if ce := logger.Check(zap.DebugLevel, "loadbalancer"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	Metrics.Requests.Add(1)
	prometheusMetrics.Requests.Inc()

	/* #nosec */
	fmt.Fprintf(w, "Ok\n")
	accessLogger.Info("lb request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	Metrics.Responses.Add(1)
	prometheusMetrics.Responses.WithLabelValues("200", "lbcheck").Inc()
}

func main() {
	err := zapwriter.ApplyConfig([]zapwriter.Config{cfg.DefaultLoggerConfig})
	if err != nil {
		log.Fatal("Failed to initialize logger with default configuration")

	}
	logger := zapwriter.Logger("main")

	configFile := flag.String("config", "", "config file (yaml)")
	pidFile := flag.String("pid", "", "pidfile (default: empty, don't create pidfile)")

	flag.Parse()

	expvar.NewString("GoVersion").Set(runtime.Version())
	expvar.NewString("BuildVersion").Set(BuildVersion)

	if *configFile == "" {
		logger.Fatal("missing config file option")
	}

	fh, err := os.Open(*configFile)
	if err != nil {
		logger.Fatal("unable to read config file:",
			zap.Error(err),
		)
	}

	config.Zipper, err = cfg.ParseZipperConfig(fh)
	if err != nil {
		logger.Fatal("failed to parse config",
			zap.String("config_path", *configFile),
			zap.Error(err),
		)
	}
	fh.Close()

	if len(config.Backends) == 0 {
		logger.Fatal("no Backends loaded -- exiting")
	}

	err = zapwriter.ApplyConfig(config.Logger)
	if err != nil {
		logger.Fatal("Failed to apply config",
			zap.Any("config", config.Logger),
			zap.Error(err),
		)
	}

	// Should print nicer stack traces in case of unexpected panic.
	defer func() {
		if r := recover(); r != nil {
			logger.Fatal("Recovered from unhandled panic",
				zap.Stack("stacktrace"),
			)
		}
	}()

	searchConfigured = len(config.CarbonSearch.Prefix) > 0 && len(config.CarbonSearch.Backend) > 0

	logger = zapwriter.Logger("main")
	logger.Info("starting carbonzipper",
		zap.String("build_version", BuildVersion),
		zap.Bool("carbonsearch_configured", searchConfigured),
		zap.Any("config", config),
	)

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

	Metrics.SearchCacheSize = expvar.Func(func() interface{} { return config.SearchCache.ECSize() })
	expvar.Publish("searchCacheSize", Metrics.SearchCacheSize)

	Metrics.SearchCacheItems = expvar.Func(func() interface{} { return config.SearchCache.ECItems() })
	expvar.Publish("searchCacheItems", Metrics.SearchCacheItems)

	config.zipper = zipper.NewZipper(sendStats, config.Zipper, zapwriter.Logger("zipper"))

	Metrics.LimiterUse = expvar.Func(func() interface{} {
		return config.zipper.LimiterUse()
	})
	expvar.Publish("limiter_use", Metrics.LimiterUse)

	Metrics.LimiterUseMax = expvar.Func(func() interface{} {
		return config.zipper.MaxLimiterUse()
	})
	expvar.Publish("limiter_use_max", Metrics.LimiterUseMax)

	r := http.NewServeMux()

	r.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(findHandler, bucketRequestTimes)))
	r.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(renderHandler, bucketRequestTimes)))
	r.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(infoHandler, bucketRequestTimes)))
	r.HandleFunc("/lb_check", lbCheckHandler)

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
		graphite.Register(fmt.Sprintf("%s.corruption", pattern), Metrics.Corruption)

		for i := 0; i <= config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("%s.requests_in_%dms_to_%dms", pattern, i*100, (i+1)*100), bucketEntry(i))
			lower, upper := util.Bounds(i)
			graphite.Register(fmt.Sprintf("%s.exp.requests_in_%05dms_to_%05dms", pattern, lower, upper), expBucketEntry(i))
		}

		graphite.Register(fmt.Sprintf("%s.cache_size", pattern), Metrics.CacheSize)
		graphite.Register(fmt.Sprintf("%s.cache_items", pattern), Metrics.CacheItems)

		graphite.Register(fmt.Sprintf("%s.search_cache_size", pattern), Metrics.SearchCacheSize)
		graphite.Register(fmt.Sprintf("%s.search_cache_items", pattern), Metrics.SearchCacheItems)

		graphite.Register(fmt.Sprintf("%s.cache_hits", pattern), Metrics.CacheHits)
		graphite.Register(fmt.Sprintf("%s.cache_misses", pattern), Metrics.CacheMisses)

		graphite.Register(fmt.Sprintf("%s.search_cache_hits", pattern), Metrics.SearchCacheHits)
		graphite.Register(fmt.Sprintf("%s.search_cache_misses", pattern), Metrics.SearchCacheMisses)

		go mstats.Start(config.Graphite.Interval)

		graphite.Register(fmt.Sprintf("%s.goroutines", pattern), Metrics.Goroutines)
		graphite.Register(fmt.Sprintf("%s.uptime", pattern), Metrics.Uptime)
		graphite.Register(fmt.Sprintf("%s.max_limiter_use", pattern), Metrics.LimiterUseMax)
		graphite.Register(fmt.Sprintf("%s.alloc", pattern), &mstats.Alloc)
		graphite.Register(fmt.Sprintf("%s.total_alloc", pattern), &mstats.TotalAlloc)
		graphite.Register(fmt.Sprintf("%s.num_gc", pattern), &mstats.NumGC)
		graphite.Register(fmt.Sprintf("%s.pause_ns", pattern), &mstats.PauseNS)
	}

	if *pidFile != "" {
		pidfile.SetPidfilePath(*pidFile)
		err = pidfile.Write()
		if err != nil {
			log.Fatalln("error during pidfile.Write():", err)
		}
	}

	go func() {
		prometheus.MustRegister(prometheusMetrics.Requests)
		prometheus.MustRegister(prometheusMetrics.Responses)
		prometheus.MustRegister(prometheusMetrics.Durations)

		writeTimeout := config.Timeouts.Global
		if writeTimeout < 30*time.Second {
			writeTimeout = time.Minute
		}

		r := http.NewServeMux()
		r.Handle("/metrics", promhttp.Handler())

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

func bucketRequestTimes(req *http.Request, t time.Duration) {
	logger := zapwriter.Logger("slow")

	ms := t.Nanoseconds() / int64(time.Millisecond)

	bucket := int(ms / 100)
	bucketIdx := findBucketIndex(timeBuckets, bucket)
	atomic.AddInt64(&timeBuckets[bucketIdx], 1)

	expBucket := util.Bucket(ms, config.Buckets)
	expBucketIdx := findBucketIndex(expTimeBuckets, expBucket)
	atomic.AddInt64(&expTimeBuckets[expBucketIdx], 1)

	prometheusMetrics.Durations.Observe(t.Seconds())

	// This seems slow enough to count as a slow request
	if bucket >= config.Buckets {
		logger.Warn("Slow Request",
			zap.Duration("time", t),
			zap.String("url", req.URL.String()),
		)
	}
}

func sendStats(stats *zipper.Stats) {
	Metrics.Timeouts.Add(stats.Timeouts)
	Metrics.FindErrors.Add(stats.FindErrors)
	Metrics.RenderErrors.Add(stats.RenderErrors)
	Metrics.InfoErrors.Add(stats.InfoErrors)
	Metrics.SearchRequests.Add(stats.SearchRequests)
	Metrics.SearchCacheHits.Add(stats.SearchCacheHits)
	Metrics.SearchCacheMisses.Add(stats.SearchCacheMisses)
	Metrics.CacheMisses.Add(stats.CacheMisses)
	Metrics.CacheHits.Add(stats.CacheHits)
	Metrics.Corruption.Add(stats.Corruption)
}
