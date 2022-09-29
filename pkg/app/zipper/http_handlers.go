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
	"strconv"
	"time"

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

	metrics, err := Find(app, ctx, originalQuery, ms, logger)
	if err != nil {
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
		code := http.StatusInternalServerError
		logger.Error("find failed", zap.Int("http_code", code), zap.Duration("runtime_seconds", time.Since(t0)), zap.Error(err))
		http.Error(w, err.Error(), code)
		ms.Responses.WithLabelValues(strconv.Itoa(code), "find").Inc()
		return
	}
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
		logger.Error("find failed", zap.Int("http_code", http.StatusInternalServerError), zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)), zap.Error(err))
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusInternalServerError), "find").Inc()
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
		return
	}

	w.Header().Set("Content-Type", contentType)
	_, writeErr := w.Write(blob)

	if writeErr != nil {
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(499), "find").Inc()
		logger.Warn("error writing the response", zap.Int("http_code", 499), zap.Duration("runtime_seconds", time.Since(t0)), zap.Error(writeErr))
		return
	}

	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "find").Inc()
	logger.Info("request served", zap.Int("http_code", http.StatusOK), zap.Duration("runtime_seconds", time.Since(t0)))
}

func (app *App) renderHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, logger *zap.Logger) {
	t0 := time.Now()

	t := prometheus.NewTimer(app.Metrics.RenderDurationExp)
	defer t.ObserveDuration()

	memoryUsage := 0

	ctx, cancel := context.WithTimeout(req.Context(), app.Config.Timeouts.Global)
	defer cancel()
	span := trace.SpanFromContext(ctx)

	if ce := logger.Check(zap.DebugLevel, "got render request"); ce != nil {
		ce.Write(zap.String("request", req.URL.RequestURI()))
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
	logger = logger.With(zap.String("format", format), zap.String("target", target))
	span.SetAttributes(kv.String("graphite.target", target), kv.String("graphite.format", format))

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

	span.SetAttributes(kv.Int64("graphite.from", from), kv.Int64("graphite.until", until))

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

	metrics, err := Render(app, ctx, target, from, until, ms, logger)

	span.SetAttribute("graphite.metrics", len(metrics))
	if ctx.Err() != nil {
		// context was cancelled even if some of the requests succeeded
		app.Metrics.RequestCancel.WithLabelValues("render", ctx.Err().Error()).Inc()
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
			zap.Int("memory_usage_bytes", memoryUsage), zap.Error(err), zap.Int("http_code", code), zap.Duration("runtime_seconds", time.Since(t0)),
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
		logger.Warn("error writing the response", zap.Int("http_code", 499), zap.Duration("runtime_seconds", time.Since(t0)), zap.Error(writeErr))
		return
	}

	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "render").Inc()

	logger.Info("request served", zap.Int("memory_usage_bytes", memoryUsage), zap.Int("http_code", http.StatusOK), zap.Duration("runtime_seconds", time.Since(t0)))
}

func (app *App) infoHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, lg *zap.Logger) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(req.Context(), app.Config.Timeouts.Global)
	defer cancel()

	lg = lg.With(zap.String("handler", "info"), zap.String("carbonapi_uuid", util.GetUUID(ctx)))

	lg.Debug("request", zap.String("request", req.URL.RequestURI()))

	app.Metrics.Requests.Inc()

	err := req.ParseForm()
	if err != nil {
		http.Error(w, "failed to parse arguments", http.StatusBadRequest)
		lg.Info("request failed",
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

	lg = lg.With(zap.String("target", target), zap.String("format", format))

	if target == "" {
		lg.Info("request failed",
			zap.Int("http_code", http.StatusBadRequest),
			zap.String("reason", "empty target"),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "info: empty target", http.StatusBadRequest)
		app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusBadRequest), "info").Inc()
		return
	}

	infos, err := Info(app, ctx, target, ms, lg)

	if err != nil {
		var notFound types.ErrNotFound
		if errors.As(err, &notFound) {
			lg.Info("info not found",
				zap.Int("http_code", http.StatusNotFound),
				zap.Error(err),
				zap.Duration("runtime_seconds", time.Since(t0)),
			)
			http.Error(w, "info: not found", http.StatusNotFound)
			return
		}

		lg.Error("info failed",
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
		lg.Error("info failed",
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
		lg.Warn("error writing the response",
			zap.Int("http_code", 499),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(writeErr),
		)
		return
	}

	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "info").Inc()

	lg.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
}

func (app *App) lbCheckHandler(w http.ResponseWriter, req *http.Request, ms *PrometheusMetrics, logger *zap.Logger) {
	t0 := time.Now()

	if ce := logger.Check(zap.DebugLevel, "loadbalancer"); ce != nil {
		ce.Write(zap.String("request", req.URL.RequestURI()))
	}

	app.Metrics.Requests.Inc()

	fmt.Fprintf(w, "Ok\n")
	logger.Info("lb request served", zap.Int("http_code", http.StatusOK), zap.Duration("runtime_seconds", time.Since(t0)))
	app.Metrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "lbcheck").Inc()
}
