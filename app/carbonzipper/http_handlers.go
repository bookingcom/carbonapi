package zipper

import (
	"context"
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
	"github.com/bookingcom/carbonapi/util"

	"github.com/lomik/zapwriter"
	"github.com/pkg/errors"
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

func (app *App) findHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(req.Context(), app.config.Timeouts.Global)
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
	app.prometheusMetrics.Requests.Inc()
	Metrics.FindRequests.Add(1)

	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "find"),
		zap.String("format", format),
		zap.String("target", originalQuery),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	request := types.NewFindRequest(originalQuery)
	bs := backend.Filter(app.backends, []string{originalQuery})
	metrics, err := backend.Finds(ctx, bs, request)
	if err != nil {
		if _, ok := errors.Cause(err).(types.ErrNotFound); ok {
			// graphite-web 0.9.12 needs to get a 200 OK response with an empty
			// body to be happy with its life, so we can't 404 a /metrics/find
			// request that finds nothing. We are however interested in knowing
			// that we found nothing on the monitoring side, so we claim we
			// returned a 404 code to Prometheus.
			Metrics.Errors.Add(1)
			app.prometheusMetrics.Responses.WithLabelValues("404", "find").Inc()
		} else {
			msg := "error fetching the data"
			code := http.StatusInternalServerError
			accessLogger.Error("find failed",
				zap.Int("http_code", code),
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.Error(err),
			)
			http.Error(w, msg, code)
			Metrics.Errors.Add(1)
			app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", code), "find").Inc()
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
		if app.config.GraphiteWeb09Compatibility {
			blob, err = pickle.FindEncoderV0_9(metrics)
		} else {
			blob, err = pickle.FindEncoderV1_0(metrics)
		}
	default:
		err = errors.Errorf("Unknown format %s", format)
	}

	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		accessLogger.Error("render failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "find").Inc()
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(blob)

	accessLogger.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	Metrics.Responses.Add(1)
	app.prometheusMetrics.Responses.WithLabelValues("200", "find").Inc()
}

func (app *App) renderHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	memoryUsage := 0

	ctx, cancel := context.WithTimeout(req.Context(), app.config.Timeouts.Global)
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
	app.prometheusMetrics.Requests.Inc()
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
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
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
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
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
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
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
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "render").Inc()
		return
	}

	request := types.NewRenderRequest([]string{target}, int32(from), int32(until))
	bs := backend.Filter(app.backends, request.Targets)
	metrics, err := backend.Renders(ctx, bs, request)
	// time in queue is converted to ms
	app.prometheusMetrics.TimeInQueue.Observe(float64(request.Trace.Report()[2]) / 1000 / 1000)

	if err != nil {
		msg := "error fetching the data"
		code := http.StatusInternalServerError
		if _, ok := errors.Cause(err).(types.ErrNotFound); ok {
			msg = "not found"
			code = http.StatusNotFound
		}

		http.Error(w, msg, code)
		accessLogger.Error("request failed",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.Error(err),
			zap.Int("http_code", code),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Int64s("trace", request.Trace.Report()),
		)

		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", code), "render").Inc()
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
		err = errors.Errorf("Unknown format %s", format)
	}

	if err != nil {
		http.Error(w, "error marshaling data", http.StatusInternalServerError)
		accessLogger.Error("render failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.String("reason", "error marshaling data"),
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.Error(err),
			zap.Int64s("trace", request.Trace.Report()),
		)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "render").Inc()
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(blob)

	accessLogger.Info("request served",
		zap.Int("memory_usage_bytes", memoryUsage),
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int64s("trace", request.Trace.Report()),
	)

	Metrics.Responses.Add(1)
	app.prometheusMetrics.Responses.WithLabelValues("200", "render").Inc()
}

func (app *App) infoHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(req.Context(), app.config.Timeouts.Global)
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
	app.prometheusMetrics.Requests.Inc()
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
			zap.Error(err),
		)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "info").Inc()
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
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusBadRequest), "info").Inc()
		return
	}

	request := types.NewInfoRequest(target)
	bs := backend.Filter(app.backends, []string{target})
	infos, err := backend.Infos(ctx, bs, request)
	if err != nil {
		accessLogger.Error("info failed",
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		http.Error(w, "info: error processing request", http.StatusInternalServerError)
		Metrics.Errors.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "info").Inc()
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
		err = errors.Errorf("Unknown format %s", format)
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
		app.prometheusMetrics.Responses.WithLabelValues(fmt.Sprintf("%d", http.StatusInternalServerError), "info").Inc()
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(blob)

	accessLogger.Info("request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	Metrics.Responses.Add(1)
	app.prometheusMetrics.Responses.WithLabelValues("200", "info").Inc()
}

func (app *App) lbCheckHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := zapwriter.Logger("loadbalancer").With(zap.String("handler", "loadbalancer"))
	accessLogger := zapwriter.Logger("access").With(zap.String("handler", "loadbalancer"))

	if ce := logger.Check(zap.DebugLevel, "loadbalancer"); ce != nil {
		ce.Write(
			zap.String("request", req.URL.RequestURI()),
		)
	}

	Metrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	/* #nosec */
	fmt.Fprintf(w, "Ok\n")
	accessLogger.Info("lb request served",
		zap.Int("http_code", http.StatusOK),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	Metrics.Responses.Add(1)
	app.prometheusMetrics.Responses.WithLabelValues("200", "lbcheck").Inc()
}
