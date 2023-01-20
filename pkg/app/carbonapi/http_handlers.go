package carbonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/date"
	"github.com/bookingcom/carbonapi/pkg/expr"
	"github.com/bookingcom/carbonapi/pkg/expr/functions/cairo/png"
	"github.com/bookingcom/carbonapi/pkg/expr/interfaces"
	"github.com/bookingcom/carbonapi/pkg/expr/metadata"
	"github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/handlerlog"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	ourJson "github.com/bookingcom/carbonapi/pkg/types/encoding/json"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/pickle"
	"github.com/bookingcom/carbonapi/pkg/util"
)

const (
	jsonFormat      = "json"
	treejsonFormat  = "treejson"
	pngFormat       = "png"
	csvFormat       = "csv"
	rawFormat       = "raw"
	svgFormat       = "svg"
	protobufFormat  = "protobuf"
	protobuf3Format = "protobuf3"
	pickleFormat    = "pickle"
	completerFormat = "completer"
)

func (app *App) validateRequest(h handlerlog.HandlerWithLogger, handler string, logger *zap.Logger) http.HandlerFunc {
	t0 := time.Now()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if app.requestBlocker.ShouldBlockRequest(r) {
			toLog := carbonapipb.NewAccessLogDetails(r, handler, &app.config)
			toLog.HttpCode = http.StatusForbidden
			defer func() {
				app.deferredAccessLogging(logger, r, &toLog, t0, zap.InfoLevel)
			}()
			w.WriteHeader(http.StatusForbidden)
		} else {
			h(w, r, logger)
		}
	})
}

func writeResponse(ctx context.Context, w http.ResponseWriter, b []byte, format string, jsonp string) error {
	var err error
	w.Header().Set("X-Carbonapi-UUID", util.GetUUID(ctx))
	switch format {
	case jsonFormat:
		if jsonp != "" {
			w.Header().Set("Content-Type", contentTypeJavaScript)
			if _, err = w.Write([]byte(jsonp)); err != nil {
				return err
			}
			if _, err = w.Write([]byte{'('}); err != nil {
				return err
			}
			if _, err = w.Write(b); err != nil {
				return err
			}
			if _, err = w.Write([]byte{')'}); err != nil {
				return err
			}
		} else {
			w.Header().Set("Content-Type", contentTypeJSON)
			if _, err = w.Write(b); err != nil {
				return err
			}
		}
	case protobufFormat, protobuf3Format:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		if _, err = w.Write(b); err != nil {
			return err
		}
	case rawFormat:
		w.Header().Set("Content-Type", contentTypeRaw)
		if _, err = w.Write(b); err != nil {
			return err
		}
	case pickleFormat:
		w.Header().Set("Content-Type", contentTypePickle)
		if _, err = w.Write(b); err != nil {
			return err
		}
	case csvFormat:
		w.Header().Set("Content-Type", contentTypeCSV)
		if _, err = w.Write(b); err != nil {
			return err
		}
	case pngFormat:
		w.Header().Set("Content-Type", contentTypePNG)
		if _, err = w.Write(b); err != nil {
			return err
		}
	case svgFormat:
		w.Header().Set("Content-Type", contentTypeSVG)
		if _, err = w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

const (
	contentTypeJSON       = "application/json"
	contentTypeProtobuf   = "application/x-protobuf"
	contentTypeJavaScript = "text/javascript"
	contentTypeRaw        = "text/plain"
	contentTypePickle     = "application/pickle"
	contentTypePNG        = "image/png"
	contentTypeCSV        = "text/csv"
	contentTypeSVG        = "image/svg+xml"
)

type RenderResponse struct {
	data  []*types.MetricData
	error error
}

func (app *App) renderHandler(w http.ResponseWriter, r *http.Request, lg *zap.Logger) {
	t0 := time.Now()
	defer func() {
		d := time.Since(t0).Seconds()
		app.ms.DurationTotal.WithLabelValues("render").Observe(d)
		app.ms.RenderDurationExp.Observe(d)
	}()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()
	uuid := util.GetUUID(ctx)

	lg = lg.With(zap.String("request_id", uuid), zap.String("request_type", "render"))
	Trace(lg, "received request")

	partiallyFailed := false
	toLog := carbonapipb.NewAccessLogDetails(r, "render", &app.config)

	logLevel := zap.InfoLevel
	defer func() {
		//2xx response code is treated as success
		if toLog.HttpCode/100 == 2 {
			if toLog.TotalMetricCount < int64(app.config.ResolveGlobs) {
				app.ms.RenderDurationExpSimple.Observe(time.Since(t0).Seconds())
				app.ms.RenderDurationLinSimple.Observe(time.Since(t0).Seconds())
			} else {
				app.ms.RenderDurationExpComplex.Observe(time.Since(t0).Seconds())
			}
		}
		app.deferredAccessLogging(lg, r, &toLog, t0, logLevel)
	}()

	app.ms.Requests.Inc()

	form, err := app.renderHandlerProcessForm(r, &toLog, lg)
	if err != nil {
		writeError(uuid, r, w, http.StatusBadRequest, err.Error(), form.format, &toLog)
		return
	}

	if form.from32 >= form.until32 {
		var clientErrMsgFmt string
		if form.from32 == form.until32 {
			clientErrMsgFmt = "parameter from=%s has the same value as parameter until=%s. Result time range is empty"
		} else {
			clientErrMsgFmt = "parameter from=%s greater than parameter until=%s. Result time range is empty"
		}
		clientErrMsg := fmt.Sprintf(clientErrMsgFmt, form.from, form.until)
		writeError(uuid, r, w, http.StatusBadRequest, clientErrMsg, form.format, &toLog)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = "invalid empty time range"
		return
	}

	if form.useCache {
		Trace(lg, "query request cache")

		response, cacheErr := app.queryCache.Get(form.cacheKey)

		toLog.CarbonzipperResponseSizeBytes = 0
		toLog.CarbonapiResponseSizeBytes = int64(len(response))

		if cacheErr == nil {
			Trace(lg, "request found in cache")

			writeErr := writeResponse(ctx, w, response, form.format, form.jsonp)
			if writeErr != nil {
				logLevel = zapcore.WarnLevel
			}
			toLog.FromCache = true
			toLog.HttpCode = http.StatusOK
			return
		}
		if cacheErr != cache.ErrNotFound {
			Trace(lg, "request cache error", zap.Error(cacheErr))

			addCacheErrorToLogDetails(&toLog, true, cacheErr)
		}
		Trace(lg, "request not found in cache")
	}

	metricMap := make(map[parser.MetricRequest][]*types.MetricData)

	var results []*types.MetricData

	size := 0
	for targetIdx := 0; targetIdx < len(form.targets); targetIdx++ {
		target := form.targets[targetIdx]

		lgt := lg.With(zap.String("target", target))
		Trace(lgt, "querying target")

		exp, e, parseErr := parser.ParseExpr(target)
		if parseErr != nil || e != "" {
			Trace(lgt, "parsing target expression failed", zap.Error(parseErr))
			msg := buildParseErrorString(target, e, parseErr)
			writeError(uuid, r, w, http.StatusBadRequest, msg, form.format, &toLog)
			return
		}

		getTargetData := func(ctx context.Context, exp parser.Expr, from, until int32, metricMap map[parser.MetricRequest][]*types.MetricData) (error, int) {
			return app.getTargetData(ctx, target, exp, metricMap, form.useCache, from, until, &toLog, lgt, &partiallyFailed)
		}

		targetErr, metricSize := app.getTargetData(ctx, target, exp, metricMap,
			form.useCache, form.from32, form.until32, &toLog, lgt, &partiallyFailed)

		// Continue query execution even though no metric is found in
		// prefetch as there are Graphite query functions that are able
		// to handle no data and users expect proper result returned. Example:
		//
		// 	fallbackSeries(metric.not.exist, constantLine(1))
		//
		// Reference behaviour in graphite-web: https://github.com/graphite-project/graphite-web/blob/1.1.8/webapp/graphite/render/evaluator.py#L14-L46
		var notFound dataTypes.ErrNotFound
		if targetErr == nil || errors.As(targetErr, &notFound) {
			targetErr = evalExprRender(ctx, exp, &results, metricMap, &form, app.config.PrintErrorStackTrace, getTargetData)
		}

		if targetErr != nil {
			// we can have 3 error types here
			// a) dataTypes.ErrNotFound  > Continue, at the end we check if all errors are 'not found' and we answer with http 404
			// b) parser.ParseError -> Return with this error(like above, but with less details )
			// c) anything else -> continue, answer will be 5xx if all targets have one error
			var parseError parser.ParseError
			switch {
			case errors.As(targetErr, &notFound):
				Trace(lgt, "target not found", zap.Error(targetErr))
				// When not found, graphite answers with  http 200 and []
			case errors.Is(targetErr, parser.ErrSeriesDoesNotExist):
				Trace(lgt, "target error: series does not exist; continuing execution", zap.Error(targetErr))
				// As now carbonapi continues query execution
				// when no metrics are returned, it's possible
				// to have evalExprRender returning this error.
				// carbonapi should continue executing other
				// queries in the API call to keep being backward-compatible.
				//
				// It would be nice to return the error message,
				// but it seems we do not have a way to
				// communicate it to grafana and other users of
				// carbonapi unless failing all the other queries in the same request:
				//
				// * https://github.com/grafana/grafana/blob/v7.5.10/pkg/tsdb/graphite/types.go\#L5-L8
				// * https://github.com/grafana/grafana/blob/v7.5.10/pkg/tsdb/graphite/graphite.go\#L162-L167
			case errors.As(targetErr, &parseError):
				// no tracing is needed as deferred log will have the error details
				writeError(uuid, r, w, http.StatusBadRequest, targetErr.Error(), form.format, &toLog)
				return
			case errors.Is(targetErr, context.DeadlineExceeded):
				// no tracing is needed as deferred log will have the error details
				writeError(uuid, r, w, http.StatusUnprocessableEntity, "request too complex", form.format, &toLog)
				logLevel = zapcore.ErrorLevel
				app.ms.RequestCancel.WithLabelValues("render", ctx.Err().Error()).Inc()
				return
			default:
				// no tracing is needed as deferred log will have the error details
				writeError(uuid, r, w, http.StatusInternalServerError, targetErr.Error(), form.format, &toLog)
				logLevel = zapcore.ErrorLevel
				return
			}
		}
		size += metricSize

		Trace(lgt, "target succeeded")
	}
	toLog.CarbonzipperResponseSizeBytes = int64(size * 8)

	if ctx.Err() != nil {
		app.ms.RequestCancel.WithLabelValues("render", ctx.Err().Error()).Inc()
	}

	body, err := app.renderWriteBody(results, form, r, lg)
	if err != nil {
		writeError(uuid, r, w, http.StatusInternalServerError, err.Error(), form.format, &toLog)
		logLevel = zapcore.ErrorLevel
		return
	}

	writeErr := writeResponse(ctx, w, body, form.format, form.jsonp)
	if writeErr != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
	} else {
		toLog.HttpCode = http.StatusOK
	}
	if len(results) != 0 {
		go func() {
			err := app.queryCache.Set(form.cacheKey, body, form.cacheTimeout)
			if err != nil {
				Trace(lg, "writing request to cache failed", zap.Error(err))
			}
		}()
	}

	if partiallyFailed {
		Trace(lg, "request partially failed")
		app.ms.RenderPartialFail.Inc()
	}
}

func writeError(uuid string,
	r *http.Request, w http.ResponseWriter,
	code int, s string, format string,
	accessLogDetails *carbonapipb.AccessLogDetails) {

	accessLogDetails.HttpCode = int32(code)
	accessLogDetails.Reason = s
	if format == pngFormat {
		shortErrStr := http.StatusText(code) + " (" + strconv.Itoa(code) + ")"
		w.Header().Set("X-Carbonapi-UUID", uuid)
		w.Header().Set("Content-Type", contentTypePNG)
		w.WriteHeader(code)
		body, _ := png.MarshalPNGRequestErr(r, shortErrStr, "default")
		_, err := w.Write(body)
		if err != nil {
			accessLogDetails.Reason += " 499"
		}
	} else {
		http.Error(w, http.StatusText(code)+" ("+strconv.Itoa(code)+") Details: "+s, code)
	}
}

func evalExprRender(ctx context.Context, exp parser.Expr, res *([]*types.MetricData),
	metricMap map[parser.MetricRequest][]*types.MetricData,
	form *renderForm, printErrorStackTrace bool, getTargetData interfaces.GetTargetData) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic during expr eval: %s", r)
			if printErrorStackTrace {
				debug.PrintStack()
			}
		}
	}()

	exprs, err := expr.EvalExpr(ctx, exp, form.from32, form.until32, metricMap, getTargetData)
	if err != nil {
		return err
	}

	*res = append(*res, exprs...)

	return nil
}

func (app *App) getTargetData(ctx context.Context, target string, exp parser.Expr,
	metricMap map[parser.MetricRequest][]*types.MetricData,
	useCache bool, from, until int32,
	toLog *carbonapipb.AccessLogDetails, lg *zap.Logger, partFail *bool) (error, int) {

	Trace(lg, "getting target data from upstream")

	size := 0
	metrics := 0
	var targetMetricFetches []parser.MetricRequest
	var metricErrs []error

	for _, m := range exp.Metrics() {
		lgm := lg.With(zap.String("metric", m.Metric))
		Trace(lgm, "getting metric data from upstream")

		mfetch := m
		mfetch.From += from
		mfetch.Until += until

		targetMetricFetches = append(targetMetricFetches, mfetch)
		if _, ok := metricMap[mfetch]; ok {
			// already fetched this metric for this request
			continue
		}

		// TODO: This is a hotfix. Most likely needs cleanup.
		for i := 0; i < len(m.Metric)-1; i++ {
			if m.Metric[i] == '#' {
				if m.Metric[i+1] >= 'A' && m.Metric[i+1] <= 'Z' {
					Trace(lgm, "returning not found for a #[A-Z] type metric")
					metricErrs = append(metricErrs, dataTypes.ErrMetricsNotFound)
					continue
				}
			}
		}

		// This _sometimes_ sends a *find* request
		useCacheForRenderResolveGlobs := useCache && app.config.EnableCacheForRenderResolveGlobs
		renderRequests, err := app.getRenderRequests(ctx, m, useCacheForRenderResolveGlobs, toLog, lgm)
		if err != nil {
			Trace(lgm, "failed getting sub-requests", zap.Error(err))
			metricErrs = append(metricErrs, err)
			continue
		} else if len(renderRequests) == 0 {
			Trace(lgm, "got no sub-requests")
			metricErrs = append(metricErrs, dataTypes.ErrMetricsNotFound)
			continue
		}
		Trace(lgm, "got sub-requests. sending them upstream", zap.Int("sub-requests", len(renderRequests)))

		renderRequestContext := ctx
		subrequestCount := len(renderRequests)
		if subrequestCount > 1 {
			renderRequestContext = util.WithPriority(ctx, subrequestCount)
		}
		app.ms.UpstreamSubRenderNum.Observe(float64(subrequestCount))
		rch := make(chan RenderResponse, len(renderRequests))
		for _, m := range renderRequests {
			// This blocks when the queue fills up, which is fine as the below result read would block anyway.
			//
			// TODO: Maybe handle record drops when the queue is full.
			req := &RenderReq{
				Path:  m,
				From:  mfetch.From,
				Until: mfetch.Until,

				Ctx:       renderRequestContext,
				ToLog:     toLog,
				StartTime: time.Now(),

				Results: rch,
			}

			if subrequestCount > app.config.LargeReqSize {
				app.slowQ <- req
				app.ms.UpstreamEnqueuedRequests.WithLabelValues("slow").Inc()
				app.ms.UpstreamRequestsInQueue.WithLabelValues("slow").Inc()
			} else {
				app.fastQ <- req
				app.ms.UpstreamEnqueuedRequests.WithLabelValues("fast").Inc()
				app.ms.UpstreamRequestsInQueue.WithLabelValues("fast").Inc()
			}
		}

		errs := make([]error, 0)
		for i := 0; i < len(renderRequests); i++ {
			resp := <-rch
			if resp.error != nil {
				errs = append(errs, resp.error)
				continue
			}

			for _, r := range resp.data {
				metrics++
				size += len(r.Values) // close enough
				metricMap[mfetch] = append(metricMap[mfetch], r)
			}
		}
		close(rch)

		Trace(lgm, "sub-requests returned", zap.Int("errors", len(errs)), zap.Int("total requests", len(renderRequests)))
		// We have to check it here because we don't want to return before closing rch
		select {
		case <-ctx.Done():
			Trace(lgm, "context done while getting target data", zap.Error(ctx.Err()))
			return ctx.Err(), 0
		default:
		}

		metricErr, metricErrStr := optimistFanIn(errs, len(renderRequests), "requests")
		*partFail = (*partFail) || (metricErrStr != "")
		if metricErr != nil {
			metricErrs = append(metricErrs, metricErr)
		}

		expr.SortMetrics(metricMap[mfetch], mfetch)
	} // range exp.Metrics

	Trace(lg, "got metrics for target", zap.Int("metrics", len(exp.Metrics())), zap.Int("errors", len(metricErrs)))

	targetErr, targetErrStr := optimistFanIn(metricErrs, len(exp.Metrics()), "metrics")
	*partFail = *partFail || (targetErrStr != "")

	logStepTimeMismatch(targetMetricFetches, metricMap, lg, target)

	return targetErr, size
}

// returns non-nil error when errors result in an error
// returns non-empty string when there are *some* errors, even when total err is nil
// returned string can be used to indicate partial failure
func optimistFanIn(errs []error, n int, subj string) (error, string) {
	nErrs := len(errs)
	if nErrs == 0 {
		return nil, ""
	}

	// everything failed.
	// If all the failures are not-founds, it's a not-found
	allErrorsNotFound := true
	ss := strings.Builder{}
	for _, e := range errs {
		var notFound dataTypes.ErrNotFound
		if ss.Len() < 500 {
			ss.WriteString(e.Error())
			ss.WriteString(", ")
		}
		if !errors.As(e, &notFound) {
			allErrorsNotFound = false
		}
	}

	errStr := ss.String()

	if nErrs < n {
		return nil, errStr
	}

	if allErrorsNotFound {
		return dataTypes.ErrNotFound("all " + subj + " not found; merged errs: (" + errStr + ")"), errStr
	}

	return errors.New("all " + subj + " failed; merged errs: (" + errStr + ")"), errStr
}

func sendRenderRequest(app *App, ctx context.Context, path string, from, until int32,
	toLog *carbonapipb.AccessLogDetails, lg *zap.Logger) RenderResponse {

	atomic.AddInt64(&toLog.ZipperRequests, 1)

	var err error
	var metrics []dataTypes.Metric

	app.ms.UpstreamRequests.WithLabelValues("render").Inc()
	t0 := time.Now()
	metrics, err = Render(app.TopLevelDomainCache, app.TopLevelDomainPrefixes, app.Backends,
		app.ZipperConfig.RenderReplicaMismatchConfig, ctx, path, int64(from), int64(until), app.ZipperMetrics, lg)
	app.ms.UpstreamDuration.WithLabelValues("render").Observe(time.Since(t0).Seconds())

	metricData := make([]*types.MetricData, 0)
	for i := range metrics {
		metricData = append(metricData, &types.MetricData{Metric: metrics[i]})
	}

	return RenderResponse{
		data:  metricData,
		error: err,
	}
}

type renderForm struct {
	targets      []string
	from         string
	until        string
	format       string
	template     string
	useCache     bool
	from32       int32
	until32      int32
	jsonp        string
	cacheKey     string
	cacheTimeout int32
	qtz          string
}

func (app *App) renderHandlerProcessForm(r *http.Request, accessLogDetails *carbonapipb.AccessLogDetails, logger *zap.Logger) (renderForm, error) {
	var res renderForm

	err := r.ParseForm()
	if err != nil {
		return res, err
	}

	res.targets = r.Form["target"]
	res.from = r.FormValue("from")
	res.until = r.FormValue("until")
	res.format = r.FormValue("format")
	res.template = r.FormValue("template")
	res.useCache = !parser.TruthyBool(r.FormValue("noCache"))

	if res.format == jsonFormat {
		// TODO(dgryski): check jsonp only has valid characters
		res.jsonp = r.FormValue("jsonp")
	}

	if res.format == "" && (parser.TruthyBool(r.FormValue("rawData")) || parser.TruthyBool(r.FormValue("rawdata"))) {
		res.format = rawFormat
	}

	if res.format == "" {
		res.format = pngFormat
	}

	res.cacheTimeout = app.config.Cache.DefaultTimeoutSec

	if tstr := r.FormValue("cacheTimeout"); tstr != "" {
		t, err := strconv.ParseInt(tstr, 10, 64)
		if err != nil {
			logger.Info("failed to parse cacheTimeout",
				zap.String("cache_string", tstr),
				zap.Error(err),
			)
		} else {
			res.cacheTimeout = int32(t)
		}
	}

	// make sure the cache key doesn't say noCache, because it will never hit
	r.Form.Del("noCache")

	// jsonp callback names are frequently autogenerated and hurt our cache
	r.Form.Del("jsonp")

	// Strip some cache-busters.  If you don't want to cache, use noCache=1
	r.Form.Del("_salt")
	r.Form.Del("_ts")
	r.Form.Del("_t") // Used by jquery.graphite.js

	res.cacheKey = r.Form.Encode()

	// normalize from and until values
	res.qtz = r.FormValue("tz")
	var errFrom, errUntil error
	res.from32, errFrom = date.DateParamToEpoch(res.from, res.qtz, time.Now().Add(-24*time.Hour).Unix(), app.defaultTimeZone)
	res.until32, errUntil = date.DateParamToEpoch(res.until, res.qtz, time.Now().Unix(), app.defaultTimeZone)

	accessLogDetails.UseCache = res.useCache
	accessLogDetails.FromRaw = res.from
	accessLogDetails.From = res.from32
	accessLogDetails.UntilRaw = res.until
	accessLogDetails.Until = res.until32
	accessLogDetails.Tz = res.qtz
	accessLogDetails.CacheTimeout = res.cacheTimeout
	accessLogDetails.Format = res.format
	accessLogDetails.Targets = res.targets

	if errFrom != nil || errUntil != nil {
		errFmt := "%s, invalid parameter %s=%s"
		if errFrom != nil {
			return res, fmt.Errorf(errFmt, errFrom.Error(), "from", res.from)
		}
		return res, fmt.Errorf(errFmt, errUntil.Error(), "until", res.until)
	}

	return res, nil
}

func (app *App) renderWriteBody(results []*types.MetricData, form renderForm, r *http.Request, logger *zap.Logger) ([]byte, error) {
	var body []byte
	var err error

	switch form.format {
	case jsonFormat:
		if maxDataPoints, _ := strconv.Atoi(r.FormValue("maxDataPoints")); maxDataPoints != 0 {
			results = types.ConsolidateJSON(maxDataPoints, results)
		}

		body = types.MarshalJSON(results)
	case protobufFormat, protobuf3Format:
		body, err = types.MarshalProtobuf(results)
		if err != nil {
			return body, fmt.Errorf("error while marshalling protobuf: %w", err)
		}
	case rawFormat:
		body = types.MarshalRaw(results)
	case csvFormat:
		tz := app.defaultTimeZone
		if form.qtz != "" {
			var z *time.Location
			z, err = time.LoadLocation(form.qtz)
			if err != nil {
				logger.Warn("Invalid time zone",
					zap.String("tz", form.qtz),
				)
			} else {
				tz = z
			}
		}
		body = types.MarshalCSV(results, tz)
	case pickleFormat:
		body, err = types.MarshalPickle(results)
		if err != nil {
			return body, fmt.Errorf("error while marshalling pickle: %w", err)
		}
	case pngFormat:
		body, err = png.MarshalPNGRequest(r, results, form.template)
		if err != nil {
			return body, fmt.Errorf("error while marshalling PNG: %w", err)
		}
	case svgFormat:
		body, err = png.MarshalSVGRequest(r, results, form.template)
		if err != nil {
			return body, fmt.Errorf("error while marshalling SVG: %w", err)
		}
	}

	return body, nil
}

func (app *App) sendGlobs(glob dataTypes.Matches) bool {
	if app.config.ResolveGlobs == 0 {
		return true
	} else {
		return len(glob.Matches) < app.config.ResolveGlobs
	}
}

func (app *App) resolveGlobsFromCache(metric string) (dataTypes.Matches, error) {
	blob, err := app.findCache.Get(metric)

	if err != nil {
		return dataTypes.Matches{}, err
	}

	matches, err := carbonapi_v2.FindDecoder(blob)
	if err != nil {
		return matches, err
	}

	return matches, nil
}

func (app *App) resolveGlobs(ctx context.Context, metric string, useCache bool, accessLogDetails *carbonapipb.AccessLogDetails, lg *zap.Logger) (dataTypes.Matches, bool, error) {
	lg.With(zap.String("find_query", metric))
	Trace(lg, "executing find")

	if useCache {
		Trace(lg, "query cache for find")
		matches, err := app.resolveGlobsFromCache(metric)
		if err == nil {
			Trace(lg, "find result found in cache")
			return matches, true, nil
		}
		if err != cache.ErrNotFound {
			Trace(lg, "cache error for find", zap.Error(err))
			addCacheErrorToLogDetails(accessLogDetails, true, err)
		}
		Trace(lg, "find not found in cache")
	}

	accessLogDetails.ZipperRequests++

	request := dataTypes.NewFindRequest(metric)

	var err error
	var matches dataTypes.Matches

	Trace(lg, "sending find request upstream")
	app.ms.UpstreamRequests.WithLabelValues("find").Inc()
	t0 := time.Now()
	matches, err = Find(app.TopLevelDomainCache, app.TopLevelDomainPrefixes, app.Backends, ctx, request.Query, app.ZipperMetrics, lg)
	app.ms.UpstreamDuration.WithLabelValues("find").Observe(time.Since(t0).Seconds())

	if err != nil {
		Trace(lg, "upstream find request failed", zap.Error(err))
		return matches, false, err
	}

	blob, err := carbonapi_v2.FindEncoder(matches)
	if err == nil {
		go func() {
			errCache := app.findCache.Set(metric, blob, app.config.Cache.DefaultTimeoutSec)
			if errCache != nil {
				Trace(lg, "writing find to cache failed", zap.Error(errCache))
			}
		}()
	} else {
		Trace(lg, "encoding find for caching failed", zap.Error(err))
	}

	return matches, false, nil
}

func (app *App) getRenderRequests(ctx context.Context, m parser.MetricRequest, useCache bool,
	toLog *carbonapipb.AccessLogDetails, lg *zap.Logger) ([]string, error) {
	Trace(lg, "getting sub-requests")

	if app.config.ResolveGlobs == 0 {
		return []string{m.Metric}, nil
	}
	if !strings.ContainsAny(m.Metric, "*{") {
		return []string{m.Metric}, nil
	}

	glob, fromCache, err := app.resolveGlobs(ctx, m.Metric, useCache, toLog, lg)
	toLog.TotalMetricCount += int64(len(glob.Matches))
	if err != nil {
		return nil, err
	}

	if app.sendGlobs(glob) {
		return []string{m.Metric}, nil
	}

	// If we reach here, it means that we will break the globs into single metring render
	// request, and they will be sent individually to zipper.
	// In order to populate backend caches in carbonzipper, we send the preflight find request
	// to backends. This is crucial for performance and avoids unnecessary overload.
	if fromCache {
		_, _, err := app.resolveGlobs(ctx, m.Metric, false, toLog, lg)
		if err != nil {
			return nil, err
		}
	}

	toLog.SendGlobs = false
	renderRequests := make([]string, 0, len(glob.Matches))
	for _, m := range glob.Matches {
		if m.IsLeaf {
			renderRequests = append(renderRequests, m.Path)
		}
	}

	return renderRequests, nil
}

func (app *App) findHandler(w http.ResponseWriter, r *http.Request, lg *zap.Logger) {
	t0 := time.Now()
	defer func() {
		d := time.Since(t0).Seconds()
		app.ms.DurationTotal.WithLabelValues("find").Observe(d)
		app.ms.FindDurationExp.Observe(d)
		app.ms.FindDurationLin.Observe(d)
	}()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()
	uuid := util.GetUUID(ctx)

	app.ms.Requests.Inc()

	format := r.FormValue("format")
	jsonp := r.FormValue("jsonp")
	query := r.FormValue("query")
	useCache := !parser.TruthyBool(r.FormValue("noCache"))

	toLog := carbonapipb.NewAccessLogDetails(r, "find", &app.config)
	toLog.Targets = []string{query}

	lg = lg.With(zap.String("request_id", uuid), zap.String("request_type", "find"), zap.String("target", query))
	Trace(lg, "received request")

	logLevel := zap.InfoLevel
	defer func() {
		if toLog.HttpCode/100 == 2 {
			if toLog.TotalMetricCount < int64(app.config.ResolveGlobs) {
				app.ms.FindDurationLinSimple.Observe(time.Since(t0).Seconds())
			} else {
				app.ms.FindDurationLinComplex.Observe(time.Since(t0).Seconds())
			}
		}
		app.deferredAccessLogging(lg, r, &toLog, t0, logLevel)
	}()

	if format == completerFormat {
		query = getCompleterQuery(query)
	}

	if format == "" {
		format = treejsonFormat
	}

	if query == "" {
		writeError(uuid, r, w, http.StatusBadRequest, "missing parameter `query`", "", &toLog)
		return
	}
	metrics, fromCache, err := app.resolveGlobs(ctx, query, useCache, &toLog, lg)
	toLog.FromCache = fromCache
	toLog.HttpCode = http.StatusOK
	if err == nil {
		toLog.TotalMetricCount = int64(len(metrics.Matches))
	} else {
		var notFound dataTypes.ErrNotFound

		switch {
		case errors.As(err, &notFound):
			Trace(lg, "not found")
			// graphite-web 0.9.12 needs to get a 200 OK response with an empty
			// body to be happy with its life, so we can't 404 a /metrics/find
			// request that finds nothing. We are however interested in knowing
			// that we found nothing on the monitoring side, so we claim we
			// returned a 404 code to Prometheus.
			app.ms.FindNotFound.Inc()
		case errors.Is(err, context.DeadlineExceeded):
			writeError(uuid, r, w, http.StatusUnprocessableEntity, "context deadline exceeded", "", &toLog)
			logLevel = zapcore.ErrorLevel
			return
		default:
			writeError(uuid, r, w, http.StatusUnprocessableEntity, err.Error(), "", &toLog)
			logLevel = zapcore.ErrorLevel
			return
		}
	}

	if ctx.Err() != nil {
		app.ms.RequestCancel.WithLabelValues("find", ctx.Err().Error()).Inc()
	}

	var blob []byte
	writeFormat := format
	switch format {
	case protobufFormat, protobuf3Format:
		blob, err = carbonapi_v2.FindEncoder(metrics)
	case treejsonFormat, jsonFormat:
		writeFormat = jsonFormat
		blob, err = ourJson.FindEncoder(metrics)
	case "", pickleFormat:
		writeFormat = pickleFormat
		if app.config.GraphiteWeb09Compatibility {
			blob, err = pickle.FindEncoderV0_9(metrics)
		} else {
			blob, err = pickle.FindEncoderV1_0(metrics)
		}
	case rawFormat:
		blob = findList(metrics)
	case completerFormat:
		writeFormat = jsonFormat
		blob, err = findCompleter(metrics)
	default:
		err = fmt.Errorf("Unknown format %s", format)
	}
	if err != nil {
		writeError(uuid, r, w, http.StatusInternalServerError, err.Error(), "", &toLog)
		logLevel = zapcore.ErrorLevel
		return
	}
	if writeResponse(ctx, w, blob, writeFormat, jsonp) != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
		return
	}
	toLog.HttpCode = http.StatusOK
}

func (app *App) expandHandler(w http.ResponseWriter, r *http.Request, lg *zap.Logger) {
	t0 := time.Now()
	defer func() {
		d := time.Since(t0).Seconds()
		app.ms.DurationTotal.WithLabelValues("expand").Observe(d)
	}()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()
	uuid := util.GetUUID(ctx)

	app.ms.Requests.Inc()

	leavesOnly := parser.TruthyBool(r.FormValue("leavesOnly"))
	groupByExpr := parser.TruthyBool(r.FormValue("groupByExpr"))
	jsonp := r.FormValue("jsonp")
	useCache := !parser.TruthyBool(r.FormValue("noCache"))

	toLog := carbonapipb.NewAccessLogDetails(r, "expand", &app.config)
	err := r.ParseForm()
	if err != nil {
		writeError(uuid, r, w, http.StatusBadRequest, "error parsing form", "", &toLog)
		return
	}
	queries := r.Form["query"]
	toLog.Targets = append(toLog.Targets, queries...)

	lg = lg.With(zap.String("request_id", uuid), zap.String("request_type", "expand"))
	Trace(lg, "received request")

	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(lg, r, &toLog, t0, logLevel)
	}()
	if len(queries) == 0 {
		writeError(uuid, r, w, http.StatusBadRequest, "missing parameter `query`", "", &toLog)
		return
	}

	var responses []dataTypes.Matches
	for _, query := range queries {
		metrics, fromCache, err := app.resolveGlobs(ctx, query, useCache, &toLog, lg)
		toLog.FromCache = fromCache
		if err == nil {
			toLog.TotalMetricCount = int64(len(metrics.Matches))
		} else {
			var notFound dataTypes.ErrNotFound
			switch {
			case errors.As(err, &notFound):
				// we can generate 404 for expand, it's graphite-web-1.0 only
				Trace(lg, "not found")
				writeError(uuid, r, w, http.StatusNotFound, err.Error(), "", &toLog)
				logLevel = zapcore.ErrorLevel
				return
			case errors.Is(err, context.DeadlineExceeded):
				writeError(uuid, r, w, http.StatusUnprocessableEntity, "context deadline exceeded", "", &toLog)
				logLevel = zapcore.ErrorLevel
				return
			default:
				writeError(uuid, r, w, http.StatusUnprocessableEntity, err.Error(), "", &toLog)
				logLevel = zapcore.ErrorLevel
				return
			}
		}
		if ctx.Err() != nil {
			app.ms.RequestCancel.WithLabelValues("expand", ctx.Err().Error()).Inc()
		}
		responses = append(responses, metrics)
	}

	blob, err := expandEncoder(responses, leavesOnly, groupByExpr)
	if err != nil {
		writeError(uuid, r, w, http.StatusInternalServerError, err.Error(), "", &toLog)
		logLevel = zapcore.ErrorLevel
		return
	}

	if writeResponse(ctx, w, blob, jsonFormat, jsonp) != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.ErrorLevel
		return
	}

	toLog.HttpCode = http.StatusOK
}

func expandEncoder(globs []dataTypes.Matches, leavesOnly bool, groupByExpr bool) ([]byte, error) {
	var b bytes.Buffer
	groups := make(map[string][]string)
	seen := make(map[string]bool)
	var err error
	for _, glob := range globs {
		paths := make([]string, 0, len(glob.Matches))
		for _, g := range glob.Matches {
			if leavesOnly && !g.IsLeaf {
				continue
			}
			if _, ok := seen[g.Path]; ok {
				continue
			}
			seen[g.Path] = true
			paths = append(paths, g.Path)
		}
		sort.Strings(paths)
		groups[glob.Name] = paths
	}
	if groupByExpr {
		// results are map[string][]string
		data := map[string]map[string][]string{
			"results": groups,
		}
		err = json.NewEncoder(&b).Encode(data)
	} else {
		// results are just []string otherwise
		// so, flatting map
		flatData := make([]string, 0)
		for _, group := range groups {
			flatData = append(flatData, group...)
		}
		// sorting flat list one more to mimic graphite-web
		sort.Strings(flatData)
		data := map[string][]string{
			"results": flatData,
		}
		err = json.NewEncoder(&b).Encode(data)
	}
	return b.Bytes(), err
}

func getCompleterQuery(query string) string {
	var replacer = strings.NewReplacer("/", ".")
	query = replacer.Replace(query)
	if query == "" || query == "/" || query == "." {
		query = ".*"
	} else {
		query += "*"
	}
	return query
}

type completer struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	IsLeaf string `json:"is_leaf"`
}

func findCompleter(globs dataTypes.Matches) ([]byte, error) {
	var b bytes.Buffer

	var complete = make([]completer, 0)

	for _, g := range globs.Matches {
		path := g.Path
		if !g.IsLeaf && path[len(path)-1:] != "." {
			path = g.Path + "."
		}
		c := completer{
			Path: path,
		}

		if g.IsLeaf {
			c.IsLeaf = "1"
		} else {
			c.IsLeaf = "0"
		}

		i := strings.LastIndex(c.Path, ".")

		if i != -1 {
			c.Name = c.Path[i+1:]
		} else {
			c.Name = g.Path
		}

		complete = append(complete, c)
	}

	err := json.NewEncoder(&b).Encode(struct {
		Metrics []completer `json:"metrics"`
	}{
		Metrics: complete},
	)
	return b.Bytes(), err
}

func findList(globs dataTypes.Matches) []byte {
	var b bytes.Buffer

	for _, g := range globs.Matches {

		var dot string
		// make sure non-leaves end in one dot
		if !g.IsLeaf && !strings.HasSuffix(g.Path, ".") {
			dot = "."
		}

		fmt.Fprintln(&b, g.Path+dot)
	}

	return b.Bytes()
}

func (app *App) infoHandler(w http.ResponseWriter, r *http.Request, lg *zap.Logger) {
	t0 := time.Now()
	// note: We don't measure duration to reduce the number of exposed metrics.

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()

	format := r.FormValue("format")

	app.ms.Requests.Inc()

	if format == "" {
		format = jsonFormat
	}

	toLog := carbonapipb.NewAccessLogDetails(r, "info", &app.config)
	toLog.Format = format

	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(lg, r, &toLog, t0, logLevel)
	}()

	query := r.FormValue("target")

	uuid := util.GetUUID(ctx)
	lg = lg.With(zap.String("request_id", uuid), zap.String("request_type", "info"), zap.String("target", query))
	Trace(lg, "received request")

	if query == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = "no target specified"
		return
	}

	var infos []dataTypes.Info
	var err error
	app.ms.UpstreamRequests.WithLabelValues("info").Inc()
	infos, err = Info(app.TopLevelDomainCache, app.TopLevelDomainPrefixes, app.Backends, ctx, query, app.ZipperMetrics, lg)
	// not counting the duration for info requests to reduce the number of exposed metrics

	if err != nil {
		var notFound dataTypes.ErrNotFound
		if errors.As(err, &notFound) {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			toLog.HttpCode = http.StatusNotFound
			toLog.Reason = "info not found"
			return
		}
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		toLog.HttpCode = http.StatusInternalServerError
		toLog.Reason = err.Error()
		logLevel = zapcore.ErrorLevel
		return
	}

	var b []byte
	var contentType string
	switch format {
	case jsonFormat:
		contentType = contentTypeJSON
		b, err = ourJson.InfoEncoder(infos)
	case protobufFormat, protobuf3Format:
		contentType = contentTypeProtobuf
		b, err = carbonapi_v2.InfoEncoder(infos)
	default:
		err = fmt.Errorf("unknown format %v", format)
	}

	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		toLog.HttpCode = http.StatusInternalServerError
		toLog.Reason = err.Error()
		logLevel = zapcore.ErrorLevel
		return
	}

	w.Header().Set("Content-Type", contentType)
	_, writeErr := w.Write(b)
	toLog.Runtime = time.Since(t0).Seconds()
	if writeErr != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
		return
	}

	toLog.HttpCode = http.StatusOK
}

func (app *App) lbcheckHandler(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	t0 := time.Now()

	app.ms.Requests.Inc()
	toLog := carbonapipb.NewAccessLogDetails(r, "lbcheck", &app.config)
	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(logger, r, &toLog, t0, logLevel)
	}()

	_, writeErr := w.Write([]byte("Ok\n"))

	toLog.Runtime = time.Since(t0).Seconds()
	toLog.HttpCode = http.StatusOK
	if writeErr != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
	}
}

func (app *App) versionHandler(w http.ResponseWriter, r *http.Request, lg *zap.Logger) {
	t0 := time.Now()

	app.ms.Requests.Inc()
	toLog := carbonapipb.NewAccessLogDetails(r, "version", &app.config)
	toLog.HttpCode = http.StatusOK
	logLevel := zap.InfoLevel
	// Use a specific version of graphite for grafana
	// This handler is queried by grafana, and if needed, an override can be provided
	if app.config.GraphiteVersionForGrafana != "" {
		_, err := w.Write([]byte(app.config.GraphiteVersionForGrafana))
		if err != nil {
			lg.Warn("error writing GraphiteVersionForGrafana", zap.Error(err))
		}
		return
	}
	defer func() {
		app.deferredAccessLogging(lg, r, &toLog, t0, logLevel)
	}()

	if app.config.GraphiteWeb09Compatibility {
		if _, err := w.Write([]byte("0.9.15\n")); err != nil {
			toLog.HttpCode = 499
			logLevel = zapcore.WarnLevel
		}
	} else {
		if _, err := w.Write([]byte("1.0.0\n")); err != nil {
			toLog.HttpCode = 499
			logLevel = zapcore.WarnLevel
		}
	}

	toLog.Runtime = time.Since(t0).Seconds()
}

func (app *App) functionsHandler(w http.ResponseWriter, r *http.Request, lg *zap.Logger) {
	// TODO: Implement helper for specific functions
	t0 := time.Now()

	app.ms.Requests.Inc()

	toLog := carbonapipb.NewAccessLogDetails(r, "functions", &app.config)
	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(lg, r, &toLog, t0, logLevel)
	}()

	err := r.ParseForm()
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = err.Error()
		return
	}

	grouped := false
	nativeOnly := false
	groupedStr := r.FormValue("grouped")
	prettyStr := r.FormValue("pretty")
	nativeOnlyStr := r.FormValue("nativeOnly")
	var marshaler func(interface{}) ([]byte, error)

	if groupedStr == "1" {
		grouped = true
	}

	if prettyStr == "1" {
		marshaler = func(v interface{}) ([]byte, error) {
			return json.MarshalIndent(v, "", "\t")
		}
	} else {
		marshaler = json.Marshal
	}

	if nativeOnlyStr == "1" {
		nativeOnly = true
	}

	path := strings.Split(r.URL.EscapedPath(), "/")
	function := ""
	if len(path) >= 3 {
		function = path[2]
	}

	var b []byte
	if !nativeOnly {
		metadata.FunctionMD.RLock()
		if function != "" {
			b, err = marshaler(metadata.FunctionMD.Descriptions[function])
		} else if grouped {
			b, err = marshaler(metadata.FunctionMD.DescriptionsGrouped)
		} else {
			b, err = marshaler(metadata.FunctionMD.Descriptions)
		}
		metadata.FunctionMD.RUnlock()
	} else {
		metadata.FunctionMD.RLock()
		if function != "" {
			if !metadata.FunctionMD.Descriptions[function].Proxied {
				b, err = marshaler(metadata.FunctionMD.Descriptions[function])
			} else {
				err = fmt.Errorf("%v is proxied to graphite-web and nativeOnly was specified", function)
			}
		} else if grouped {
			descGrouped := make(map[string]map[string]types.FunctionDescription)
			for groupName, description := range metadata.FunctionMD.DescriptionsGrouped {
				desc := make(map[string]types.FunctionDescription)
				for f, d := range description {
					if d.Proxied {
						continue
					}
					desc[f] = d
				}
				if len(desc) > 0 {
					descGrouped[groupName] = desc
				}
			}
			b, err = marshaler(descGrouped)
		} else {
			desc := make(map[string]types.FunctionDescription)
			for f, d := range metadata.FunctionMD.Descriptions {
				if d.Proxied {
					continue
				}
				desc[f] = d
			}
			b, err = marshaler(desc)
		}
		metadata.FunctionMD.RUnlock()
	}

	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		toLog.HttpCode = http.StatusInternalServerError
		toLog.Reason = err.Error()
		logLevel = zapcore.ErrorLevel
		return
	}

	_, err = w.Write(b)
	toLog.Runtime = time.Since(t0).Seconds()
	toLog.HttpCode = http.StatusOK
	if err != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
	}
}

// Add block rules on the basis of headers to block certain requests
// To be used to block read abusers
// The rules are added(appended) in the block headers config file
// Returns failure if handler is invoked and config entry is missing
// Otherwise, it creates the config file with the rule
func (app *App) blockHeaders(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	t0 := time.Now()

	toLog := carbonapipb.NewAccessLogDetails(r, "blockHeaders", &app.config)
	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(logger, r, &toLog, t0, logLevel)
	}()

	w.Header().Set("Content-Type", contentTypeJSON)

	failResponse := []byte(`{"success":"false"}`)
	if !app.requestBlocker.AddNewRules(r.URL.Query()) {
		w.WriteHeader(http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		if _, err := w.Write(failResponse); err != nil {
			toLog.HttpCode = 499
			logLevel = zapcore.WarnLevel
		}
		return
	}
	_, err := w.Write([]byte(`{"success":"true"}`))

	toLog.HttpCode = http.StatusOK
	if err != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
	}
}

// It deletes the block headers config file
// Use it to remove all blocking rules, or to restart adding rules
// from scratch
func (app *App) unblockHeaders(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	t0 := time.Now()
	toLog := carbonapipb.NewAccessLogDetails(r, "unblockHeaders", &app.config)
	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(logger, r, &toLog, t0, logLevel)
	}()

	w.Header().Set("Content-Type", contentTypeJSON)
	err := app.requestBlocker.Unblock()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		if _, writeErr := w.Write([]byte(`{"success":"false"}`)); writeErr != nil {
			toLog.HttpCode = 499
			logLevel = zapcore.WarnLevel
		}
		return
	}
	_, err = w.Write([]byte(`{"success":"true"}`))
	toLog.HttpCode = http.StatusOK
	if err != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
	}

}

func logStepTimeMismatch(targetMetricFetches []parser.MetricRequest, metricMap map[parser.MetricRequest][]*types.MetricData, logger *zap.Logger, target string) {
	var defaultStepTime int32 = -1
	for _, mfetch := range targetMetricFetches {
		values := metricMap[mfetch]
		if len(values) == 0 {
			continue
		}
		if defaultStepTime <= 0 {
			defaultStepTime = values[0].StepTime
		}
		if !isStepTimeMatching(values[:], defaultStepTime) {
			logger.Info("metrics with differing resolution", zap.Any("target", target))
			return
		}
	}
}

func isStepTimeMatching(value []*types.MetricData, defaultStepTime int32) bool {
	for _, val := range value {
		if defaultStepTime != val.StepTime {
			return false
		}
	}
	return true
}

var usageMsg = []byte(`
supported requests:
	/render/?target=
	/metrics/expand/?query=
	/metrics/find/?query=
	/info/?target=
	/functions/
	/tags/autoComplete/tags
`)

func (app *App) usageHandler(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	t0 := time.Now()
	app.ms.Requests.Inc()
	toLog := carbonapipb.NewAccessLogDetails(r, "usage", &app.config)
	logLevel := zap.InfoLevel
	defer func() {
		app.deferredAccessLogging(logger, r, &toLog, t0, logLevel)
	}()
	toLog.HttpCode = http.StatusOK
	_, err := w.Write(usageMsg)
	if err != nil {
		toLog.HttpCode = 499
		logLevel = zapcore.WarnLevel
	}
}

// TODO : Fix this handler if and when tag support is added
// This responds to grafana's tag requests, which were falling through to the usageHandler,
// preventing a random, garbage list of tags (constructed from usageMsg) being added to the metrics list
func (app *App) tagsHandler(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	app.ms.Requests.Inc()
	defer func() {
		app.ms.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "tags", "false").Inc()
	}()
}

func buildParseErrorString(target, e string, err error) string {
	msg := fmt.Sprintf("%s\n\n%-20s: %s\n", http.StatusText(http.StatusBadRequest), "Target", target)
	if err != nil {
		msg += fmt.Sprintf("%-20s: %s\n", "Error", err.Error())
	}
	if e != "" {
		msg += fmt.Sprintf("%-20s: %s\n%-20s: %s\n",
			"Parsed so far", target[0:len(target)-len(e)],
			"Could not parse", e)
	}
	return msg
}

func addCacheErrorToLogDetails(d *carbonapipb.AccessLogDetails, isRead bool, err error) {
	if err == nil {
		return
	}
	prefix := "set: "
	if isRead {
		prefix = "get: "
	}
	d.CacheErrs += prefix + err.Error() + ","
}
