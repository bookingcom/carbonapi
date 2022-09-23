// Package zipper exposes following non-monitoring endpoints:
//   - /metrics/find
//   - /render
//   - /info
//
// Error codes policy (applies to find, render, info endpoints)
//
//   - if at least one backend succeeds, it's a success with code 200.
//   - if all bakends fail
//     - if all errors are not-found, it's a not found. But code is 200 + a monitoring counter incremented.
//     - if errors are of mixed type we fail with code 500.
package zipper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/json"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/pickle"
	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"

	"go.uber.org/zap"
)

const (
	contentTypeJSON     = "application/json"
	contentTypeProtobuf = "application/x-protobuf"
	contentTypePickle   = "application/pickle"
)

const (
	formatTypeEmpty     = ""
	formatTypePickle    = "pickle"
	formatTypeJSON      = "json"
	formatTypeProtobuf  = "protobuf"
	formatTypeProtobuf3 = "protobuf3"
)

func (app *App) findHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, logger *zap.Logger) {
	t0 := time.Now()

	tExp := prometheus.NewTimer(app.Metrics.FindDurationExp)
	tLin := prometheus.NewTimer(app.Metrics.FindDurationLin)
	defer tExp.ObserveDuration()
	defer tLin.ObserveDuration()

	ctx, cancel := context.WithTimeout(req.Context(), app.Config.Timeouts.Global)
	defer cancel()
	span := trace.SpanFromContext(ctx)

	if ce := logger.Check(zap.DebugLevel, "got find request"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	originalQuery := req.FormValue("query")
	format := req.FormValue("format")

	app.Metrics.Requests.Inc()

	logger = logger.With(
		zap.String("handler", "find"),
		zap.String("format", format),
		zap.String("target", originalQuery),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	span.SetAttributes(
		kv.String("graphite.format", format),
		kv.String("graphite.target", originalQuery),
	)
	request := types.NewFindRequest(originalQuery)
	bs := app.filterBackendByTopLevelDomain([]string{originalQuery})
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, []string{originalQuery})
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	metrics, errs := backend.Finds(ctx, bs, request, app.Metrics.FindOutDuration)
	err := errorsFanIn(errs, len(bs))

	if ctx.Err() != nil {
		// context was cancelled even if some of the requests succeeded
		app.Metrics.RequestCancel.WithLabelValues(
			"find", ctx.Err().Error(),
		).Inc()
	}

	if err != nil {
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
		var notFound types.ErrNotFound
		if errors.As(err, &notFound) {
			// graphite-web 0.9.12 needs to get a 200 OK response with an empty
			// body to be happy with its life, so we can't 404 a /metrics/find
			// request that finds nothing. We are however interested in knowing
			// that we found nothing on the monitoring side, so we claim we
			// returned a 404 code to Prometheus.

			app.Metrics.FindNotFound.Inc()
			logger.Info("not found",
				zap.Error(err))
			// TODO (grzkv) Should we return here?
		} else {
			code := http.StatusInternalServerError
			logger.Error("find failed",
				zap.Int("http_code", code),
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.Error(err),
			)
			http.Error(w, err.Error(), code)
			app.Metrics.Responses.WithLabelValues(strconv.Itoa(code), "find").Inc()
			return
		}
	}

	sort.Slice(metrics.Matches, func(i, j int) bool {
		if metrics.Matches[i].Path < metrics.Matches[j].Path {
			return true
		}
		if metrics.Matches[i].Path > metrics.Matches[j].Path {
			return false
		}
		return metrics.Matches[i].Path < metrics.Matches[j].Path
	})

	span.SetAttribute("graphite.total_metric_count", len(metrics.Matches))

	var contentType string
	var blob []byte
	switch format {
	case formatTypeProtobuf, formatTypeProtobuf3:
		contentType = contentTypeProtobuf
		blob, err = carbonapi_v2.FindEncoder(metrics)
	case formatTypeJSON:
		contentType = contentTypeJSON
		blob, err = json.FindEncoder(metrics)
	case formatTypeEmpty, formatTypePickle:
		contentType = contentTypePickle
		if app.Config.GraphiteWeb09Compatibility {
			blob, err = pickle.FindEncoderV0_9(metrics)
		} else {
			blob, err = pickle.FindEncoderV1_0(metrics)
		}
	default:
		err = fmt.Errorf("Unknown format %s", format)
	}

	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		logger.Error("find failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusInternalServerError), "find").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
		return
	}

	w.Header().Set("Content-Type", contentType)
	_, writeErr := w.Write(blob)

	if writeErr != nil {
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(499), "find").Inc()
		logger.Warn("error writing the response",
			zap.Int("http_code", 499),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(writeErr),
		)
		return
	}

	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "find").Inc()
	logger.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
}

func (app *App) renderHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, logger *zap.Logger) {
	t0 := time.Now()

	tExp := prometheus.NewTimer(app.Metrics.RenderDurationExp)
	defer tExp.ObserveDuration()

	memoryUsage := 0

	ctx, cancel := context.WithTimeout(req.Context(), app.Config.Timeouts.Global)
	defer cancel()
	span := trace.SpanFromContext(ctx)

	if ce := logger.Check(zap.DebugLevel, "got render request"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	app.Metrics.Requests.Inc()

	logger = logger.With(
		zap.String("handler", "render"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	err := req.ParseForm()
	if err != nil {
		http.Error(w, "failed to parse arguments", http.StatusBadRequest)
		logger.Info("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "failed to parse arguments"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "render").Inc()
		return
	}

	target := req.FormValue("target")
	format := req.FormValue("format")
	logger = logger.With(
		zap.String("format", format),
		zap.String("target", target),
	)
	span.SetAttributes(
		kv.String("graphite.target", target),
		kv.String("graphite.format", format),
	)
	from, err := strconv.ParseInt(req.FormValue("from"), 10, 64)
	if err != nil {
		http.Error(w, "from is not a integer", http.StatusBadRequest)
		logger.Info("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "from is not a integer"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "render").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", "from is not a integer")
		return
	}

	until, err := strconv.ParseInt(req.FormValue("until"), 10, 64)
	if err != nil {
		http.Error(w, "until is not a integer", http.StatusBadRequest)
		logger.Info("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "until is not a integer"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "render").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", "until is not a integer")
		return
	}

	span.SetAttributes(
		kv.Int64("graphite.from", from),
		kv.Int64("graphite.until", until),
	)

	if target == "" {
		http.Error(w, "empty target", http.StatusBadRequest)
		logger.Info("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.String("reason", "empty target"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "render").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", "empty target")
		return
	}

	request := types.NewRenderRequest([]string{target}, int32(from), int32(until))
	request.Trace.OutDuration = app.Metrics.RenderOutDurationExp
	bs := app.filterBackendByTopLevelDomain(request.Targets)
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, request.Targets)
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	metrics, stats, errs := backend.Renders(ctx, bs, request, app.Config.RenderReplicaMismatchConfig, logger)
	app.Metrics.Renders.Add(float64(stats.DataPointCount))
	app.Metrics.RenderMismatches.Add(float64(stats.MismatchCount))
	app.Metrics.RenderFixedMismatches.Add(float64(stats.FixedMismatchCount))
	err = errorsFanIn(errs, len(bs))
	span.SetAttribute("graphite.metrics", len(metrics))

	if ctx.Err() != nil {
		// context was cancelled even if some of the requests succeeded
		app.Metrics.RequestCancel.WithLabelValues(
			"render", ctx.Err().Error(),
		).Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", ctx.Err().Error())
	}

	if err != nil {
		msg := "error fetching the data"
		code := http.StatusInternalServerError
		var notFound types.ErrNotFound
		if errors.As(err, &notFound) {
			msg = "not found"
			code = http.StatusNotFound
		}

		http.Error(w, msg, code)
		fields := []zap.Field{
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.Error(err),
			zap.Int("http_code", code),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Int64s("trace", request.Trace.Report()),
		}
		if code == http.StatusNotFound {
			logger.Info("request failed", fields...)
		} else {
			logger.Error("request failed", fields...)
		}

		app.Metrics.Responses.WithLabelValues(strconv.Itoa(code), "render").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
		return
	}

	var blob []byte
	var contentType string
	switch format {
	case formatTypeProtobuf, formatTypeProtobuf3:
		contentType = contentTypeProtobuf
		blob, err = carbonapi_v2.RenderEncoder(metrics)
	case formatTypeJSON:
		contentType = contentTypeJSON
		blob, err = json.RenderEncoder(metrics)
	case formatTypeEmpty, formatTypePickle:
		contentType = contentTypePickle
		blob, err = pickle.RenderEncoder(metrics)
	default:
		err = fmt.Errorf("Unknown format %s", format)
	}

	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		logger.Error("render failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.Error(err),
			zap.Int64s("trace", request.Trace.Report()),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusInternalServerError), "render").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())

		return
	}

	w.Header().Set("Content-Type", contentType)
	_, writeErr := w.Write(blob)

	if writeErr != nil {
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(499), "render").Inc()
		logger.Warn("error writing the response",
			zap.Int("http_code", 499),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(writeErr),
		)
		return
	}

	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "render").Inc()
	if stats.MismatchCount > stats.FixedMismatchCount {
		app.Metrics.RenderMismatchedResponses.Inc()
	}

	logger.Info("request served",
		zap.Int("memory_usage_bytes", memoryUsage),
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int64s("trace", request.Trace.Report()),
	)
}

func (app *App) infoHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, logger *zap.Logger) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(req.Context(), app.Config.Timeouts.Global)
	defer cancel()

	logger = logger.With(
		zap.String("handler", "info"),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	logger.Debug("request", zap.String("request", req.URL.RequestURI()))

	app.Metrics.Requests.Inc()

	err := req.ParseForm()
	if err != nil {
		http.Error(w, "failed to parse arguments", http.StatusBadRequest)
		logger.Info("request failed",
			zap.String("reason", "failed to parse arguments"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "info").Inc()
		return
	}

	target := req.FormValue("target")
	format := req.FormValue("format")

	logger = logger.With(
		zap.String("target", target),
		zap.String("format", format),
	)

	if target == "" {
		logger.Info("request failed",
			zap.Int("http_code", http.StatusBadRequest),
			zap.String("reason", "empty target"),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "info: empty target", http.StatusBadRequest)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "info").Inc()
		return
	}

	request := types.NewInfoRequest(target)
	bs := app.filterBackendByTopLevelDomain([]string{target})
	var filteredByPathCache bool
	bs, filteredByPathCache = backend.Filter(bs, []string{target})
	if filteredByPathCache {
		ms.PathCacheFilteredRequests.Inc()
	}
	infos, errs := backend.Infos(ctx, bs, request)
	err = errorsFanIn(errs, len(bs))
	if err != nil {

		var notFound types.ErrNotFound
		if errors.As(err, &notFound) {
			logger.Info("info not found",
				zap.Int("http_code", http.StatusNotFound),
				zap.Error(err),
				zap.Duration("runtime_seconds", time.Since(t0)),
			)
			http.Error(w, "info: not found", http.StatusNotFound)
			return
		}

		logger.Error("info failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "info: error processing request", http.StatusInternalServerError)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusInternalServerError), "info").Inc()
		return
	}

	var contentType string
	var blob []byte
	switch format {
	case formatTypeProtobuf, formatTypeProtobuf3:
		contentType = contentTypeProtobuf
		blob, err = carbonapi_v2.InfoEncoder(infos)
	case formatTypeEmpty, formatTypeJSON:
		contentType = contentTypeJSON
		blob, err = json.InfoEncoder(infos)
	default:
		err = fmt.Errorf("Unknown format %s", format)
	}

	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		logger.Error("info failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusInternalServerError), "info").Inc()
		return
	}

	w.Header().Set("Content-Type", contentType)
	_, writeErr := w.Write(blob)

	if writeErr != nil {
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(499), "info").Inc()
		logger.Warn("error writing the response",
			zap.Int("http_code", 499),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(writeErr),
		)
		return
	}

	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "info").Inc()

	logger.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
}

func (app *App) lbCheckHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, logger *zap.Logger) {
	t0 := time.Now()

	if ce := logger.Check(zap.DebugLevel, "loadbalancer"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	app.Metrics.Requests.Inc()

	fmt.Fprintf(w, "Ok\n")
	logger.Info("lb request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK),
		"lbcheck").Inc()
}

func (app *App) filterBackendByTopLevelDomain(targets []string) []backend.Backend {
	targetTlds := make([]string, 0, len(targets))
	for _, target := range targets {
		targetTlds = append(targetTlds, getTargetTopLevelDomain(target, app.TLDPrefixes))
	}

	bs := app.filterByTopLevelDomain(app.Backends, targetTlds)
	if len(bs) > 0 {
		return bs
	}
	return app.Backends
}

func (app *App) filterByTopLevelDomain(backends []backend.Backend, targetTLDs []string) []backend.Backend {
	bs := make([]backend.Backend, 0)
	allTLDBackends := make([]*backend.Backend, 0)

	topLevelDomainCache, _ := app.TopLevelDomainCache.Get("tlds")
	tldCache := make(map[string][]*backend.Backend)
	if x, ok := topLevelDomainCache.(map[string][]*backend.Backend); ok {
		tldCache = x
	}

	if tldCache == nil {
		return backends
	}
	alreadyAddedBackends := make(map[string]bool)
	for _, target := range targetTLDs {
		tldBackends := tldCache[target]
		for _, backend := range tldBackends {
			a := *backend
			if !alreadyAddedBackends[a.GetServerAddress()] {
				alreadyAddedBackends[a.GetServerAddress()] = true
				allTLDBackends = append(allTLDBackends, backend)
			}
		}
	}
	for _, tldBackend := range allTLDBackends {
		bs = append(bs, *tldBackend)
	}

	return bs
}

func errorsFanIn(errs []error, nBackends int) error {
	nErrs := len(errs)
	var counts = make(map[string]int)
	switch {
	case (nErrs == 0):
		return nil
	case (nErrs < nBackends):
		return nil
	case (nErrs > nBackends):
		return errors.New("got more errors than there are backends. Probably something is broken")
	default:
		// everything failed, nErrs == nBackends
		nNotNotFounds := 0
		for _, e := range errs {
			counts[e.Error()] += 1
			if _, ok := e.(types.ErrNotFound); !ok {
				nNotNotFounds += 1
			}
		}

		nMajority := (nBackends + 1) / 2

		if nNotNotFounds < nMajority {
			return types.ErrNotFound(fmt.Sprintf(
				"majority of backends returned not found. %d total errors, %d not found",
				nErrs, nErrs-nNotNotFounds))
		}
		message := fmt.Sprintf("all backends failed with mixed errors: %+v", counts)
		if len(message) > 300 {
			message = message[:300]
		}
		return errors.New(message)
	}
}
