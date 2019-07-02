package carbonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bookingcom/carbonapi/carbonapipb"
	"github.com/bookingcom/carbonapi/date"
	"github.com/bookingcom/carbonapi/expr"
	"github.com/bookingcom/carbonapi/expr/functions/cairo/png"
	"github.com/bookingcom/carbonapi/expr/metadata"
	"github.com/bookingcom/carbonapi/expr/types"
	nt "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/parser"
	dataTypes "github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	ourJson "github.com/bookingcom/carbonapi/pkg/types/encoding/json"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/pickle"
	"github.com/bookingcom/carbonapi/util"

	"github.com/lomik/zapwriter"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
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
)

// for testing
// TODO (grzkv): Clean up
var timeNow = time.Now

// Rule is a request blocking rule
type Rule map[string]string

// RuleConfig represents the request blocking rules
type RuleConfig struct {
	Rules []Rule
}

// TODO (grzkv): Move out of global scope
var fileLock sync.Mutex

func (app *App) validateRequest(h http.Handler, handler string) http.HandlerFunc {
	t0 := time.Now()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blockingRules := app.blockHeaderRules.Load().(RuleConfig)
		if shouldBlockRequest(r, blockingRules.Rules) {
			toLog := carbonapipb.NewAccessLogDetails(r, handler, &app.config)
			toLog.HttpCode = http.StatusForbidden
			defer func() {
				app.deferredAccessLogging(r, &toLog, t0, true)
			}()
			w.WriteHeader(http.StatusForbidden)
		} else {
			h.ServeHTTP(w, r)
		}
	})
}

func writeResponse(ctx context.Context, w http.ResponseWriter, b []byte, format string, jsonp string) {
	w.Header().Set("X-Carbonapi-UUID", util.GetUUID(ctx))
	switch format {
	case jsonFormat:
		if jsonp != "" {
			w.Header().Set("Content-Type", contentTypeJavaScript)
			w.Write([]byte(jsonp))
			w.Write([]byte{'('})
			w.Write(b)
			w.Write([]byte{')'})
		} else {
			w.Header().Set("Content-Type", contentTypeJSON)
			w.Write(b)
		}
	case protobufFormat, protobuf3Format:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		w.Write(b)
	case rawFormat:
		w.Header().Set("Content-Type", contentTypeRaw)
		w.Write(b)
	case pickleFormat:
		w.Header().Set("Content-Type", contentTypePickle)
		w.Write(b)
	case csvFormat:
		w.Header().Set("Content-Type", contentTypeCSV)
		w.Write(b)
	case pngFormat:
		w.Header().Set("Content-Type", contentTypePNG)
		w.Write(b)
	case svgFormat:
		w.Header().Set("Content-Type", contentTypeSVG)
		w.Write(b)
	}
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

type renderResponse struct {
	data  []*types.MetricData
	error error
}

func (app *App) renderHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()

	partiallyFailed := false
	toLog := carbonapipb.NewAccessLogDetails(r, "render", &app.config)
	// TODO (grzkv): Replace with access logger
	logger := zapwriter.Logger("render").With(
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
		zap.String("username", toLog.Username),
	)

	logAsError := false
	defer func() {
		// TODO (grzkv) logging duplicated in many places
		app.deferredAccessLogging(r, &toLog, t0, logAsError)
	}()

	size := 0
	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	form, err := app.renderHandlerProcessForm(r, &toLog, logger)
	if err != nil {
		writeError(ctx, r, w, http.StatusBadRequest, err.Error(), form)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = err.Error()
		logAsError = true
		return
	}
	if form.from32 == form.until32 {
		writeError(ctx, r, w, http.StatusBadRequest, "Invalid empty time range", form)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = "invalid empty time range"
		logAsError = true
		return
	}

	if form.useCache {
		tc := time.Now()
		response, err := app.queryCache.Get(form.cacheKey)
		td := time.Since(tc).Nanoseconds()
		apiMetrics.RenderCacheOverheadNS.Add(td)

		toLog.CarbonzipperResponseSizeBytes = 0
		toLog.CarbonapiResponseSizeBytes = int64(len(response))

		if err == nil {
			apiMetrics.RequestCacheHits.Add(1)
			writeResponse(ctx, w, response, form.format, form.jsonp)
			toLog.FromCache = true
			toLog.HttpCode = http.StatusOK
			return
		}
		apiMetrics.RequestCacheMisses.Add(1)
	}

	metricMap := make(map[parser.MetricRequest][]*types.MetricData)

	var results []*types.MetricData
	var targetErrs []error
	// TODO (grzkv) Modification of *form* inside the loop is never applied
	for _, target := range form.targets {
		exp, e, err := parser.ParseExpr(target)
		if err != nil || e != "" {
			msg := buildParseErrorString(target, e, err)
			writeError(ctx, r, w, http.StatusBadRequest, msg, form)
			toLog.Reason = msg
			toLog.HttpCode = http.StatusBadRequest
			logAsError = true
			return
		}
		targetErr, metricSize := app.getTargetData(ctx, target, exp, metricMap,
			&results, &form, &toLog, logger, &partiallyFailed)
		if targetErr != nil {
			targetErrs = append(targetErrs, targetErr)
		}
		size += metricSize
	}

	if ctx.Err() != nil {
		app.prometheusMetrics.RequestCancel.WithLabelValues(
			"render", nt.ContextCancelCause(ctx.Err()),
		).Inc()
	}

	// TODO (grzkv): This breaks if targets rewrite breaks (which is broken now)
	totalErr, totalErrStr := optimistFanIn(targetErrs, len(form.targets), "targets")
	partiallyFailed = partiallyFailed || (totalErrStr != "")

	if totalErr != nil {
		toLog.Reason = totalErr.Error()
		if _, ok := totalErr.(dataTypes.ErrNotFound); ok {
			writeError(ctx, r, w, http.StatusNotFound, totalErr.Error(), form)
			toLog.HttpCode = http.StatusNotFound
			logAsError = true
			return
		}
		errOut, ok := totalErr.(*nt.ErrHTTPCode)
		if ok {
			writeError(ctx, r, w, errOut.Code(), errOut.Error(), form)
			toLog.HttpCode = int32(errOut.Code())
			logAsError = true
		} else {
			writeError(ctx, r, w, http.StatusInternalServerError, totalErr.Error(), form)
			toLog.HttpCode = http.StatusInternalServerError
			logAsError = true
		}
		return
	}

	body, err := app.renderWriteBody(results, form, r, logger)
	if err != nil {
		writeError(ctx, r, w, http.StatusInternalServerError, err.Error(), form)
		toLog.Reason = err.Error()
		toLog.HttpCode = http.StatusInternalServerError
		logAsError = true
		return
	}

	writeResponse(ctx, w, body, form.format, form.jsonp)

	if len(results) != 0 {
		tc := time.Now()
		app.queryCache.Set(form.cacheKey, body, form.cacheTimeout)
		td := time.Since(tc).Nanoseconds()
		apiMetrics.RenderCacheOverheadNS.Add(td)
	}

	if partiallyFailed {
		app.prometheusMetrics.RenderPartialFail.Inc()
	}
	toLog.HttpCode = http.StatusOK
}

func writeError(ctx context.Context, r *http.Request, w http.ResponseWriter,
	code int, s string, form renderForm) {
	// TODO (grzkv) Maybe add SVG format handling
	if form.format == pngFormat {
		shortErrStr := http.StatusText(code) + " (" + strconv.Itoa(code) + ")"
		w.Header().Set("X-Carbonapi-UUID", util.GetUUID(ctx))
		w.Header().Set("Content-Type", contentTypePNG)
		w.WriteHeader(code)
		w.Write(png.MarshalPNGRequestErr(r, shortErrStr, form.template))
	} else {
		http.Error(w, http.StatusText(code)+" ("+strconv.Itoa(code)+") Details: "+s, code)
	}
}

func evalExprRender(exp parser.Expr, res *([]*types.MetricData), metricMap map[parser.MetricRequest][]*types.MetricData,
	form *renderForm) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic duting expr eval: %s", r)
		}
	}()

	exprs, err := expr.EvalExpr(exp, form.from32, form.until32, metricMap)
	if err != nil {
		return err
	}

	*res = append(*res, exprs...)

	return nil
}

func (app *App) getTargetData(ctx context.Context, target string, exp parser.Expr,
	metricMap map[parser.MetricRequest][]*types.MetricData, results *([]*types.MetricData),
	form *renderForm, toLog *carbonapipb.AccessLogDetails, lg *zap.Logger, partFail *bool) (error, int) {

	size := 0

	var targetMetricFetches []parser.MetricRequest
	var metricErrs []error
	for _, m := range exp.Metrics() {
		mfetch := m
		mfetch.From += form.from32
		mfetch.Until += form.until32

		targetMetricFetches = append(targetMetricFetches, mfetch)
		if _, ok := metricMap[mfetch]; ok {
			// already fetched this metric for this request
			continue
		}

		// This _sometimes_ sends a *find* request
		renderRequests, err := app.getRenderRequests(ctx, m, form.useCache, toLog, lg)
		if err != nil {
			metricErrs = append(metricErrs, err)
			continue
		} else if len(renderRequests) == 0 {
			metricErrs = append(metricErrs, dataTypes.ErrMetricsNotFound)
			continue
		}

		// TODO(dgryski): group the render requests into batches
		rch := make(chan renderResponse, len(renderRequests))
		for _, m := range renderRequests {
			// TODO (grzkv) Refactor to enable premature cancel
			go app.sendRenderRequest(ctx, rch, m, mfetch.From, mfetch.Until, toLog)
		}

		errs := make([]error, 0)
		for i := 0; i < len(renderRequests); i++ {
			resp := <-rch
			if resp.error != nil {
				errs = append(errs, resp.error)
				continue
			}

			for _, r := range resp.data {
				size += 8 * len(r.Values) // close enough
				metricMap[mfetch] = append(metricMap[mfetch], r)
			}
		}
		toLog.CarbonzipperResponseSizeBytes += int64(size)
		close(rch)

		metricErr, metricErrStr := optimistFanIn(errs, len(renderRequests), "requests")
		*partFail = (*partFail) || (metricErrStr != "")
		if metricErr != nil {
			metricErrs = append(metricErrs, metricErr)
		}

		expr.SortMetrics(metricMap[mfetch], mfetch)
	} // range exp.Metrics

	targetErr, targetErrStr := optimistFanIn(metricErrs, len(exp.Metrics()), "metrics")
	*partFail = *partFail || (targetErrStr != "")

	var rewritten bool
	var newTargets []string
	logStepTimeMismatch(targetMetricFetches, metricMap, lg, target)
	rewritten, newTargets, err := expr.RewriteExpr(exp, form.from32, form.until32, metricMap)
	if err != nil && err != parser.ErrSeriesDoesNotExist {
		return errors.Wrap(err, "expression rewrite failed"), size
	}

	if rewritten {
		form.targets = append(form.targets, newTargets...)

		return targetErr, size
	}

	if targetErr != nil {
		return targetErr, size
	}

	err = evalExprRender(exp, results, metricMap, form)
	if err != nil && err != parser.ErrSeriesDoesNotExist {
		return errors.Wrap(err, "expression eval failed"), size
	}

	return nil, size
}

func pessimistFanIn(errs []error) error {
	if errs == nil || len(errs) == 0 {
		return nil
	}

	errStr := ""
	allErrorsNotFound := true
	for _, e := range errs {
		errStr = errStr + e.Error() + ", "
		if _, ok := e.(dataTypes.ErrNotFound); !ok {
			allErrorsNotFound = false
		}
	}

	if len(errStr) > 200 {
		errStr = errStr[0:200]
	}

	if allErrorsNotFound {
		return dataTypes.ErrNotFound("all not found; merged errs: (" + errStr + ")")
	}

	return errors.New("all failed with mixed errrors; merged errs: (" + errStr + ")")
}

// returns non-nil error when errors result in an error
// returns non-empty string when there are *some* errors, even when total err is nil
// returned string can be used to indicate partial failure
// returns HTTP error code (for return to client)
func optimistFanIn(errs []error, n int, subj string) (error, string) {
	nErrs := len(errs)
	if nErrs == 0 {
		return nil, ""
	}
	optimisticCode := http.StatusOK

	// everything failed.
	// If all the failures are not-founds, it's a not-found
	allErrorsNotFound := true
	errStr := ""
	for _, e := range errs {
		if _, ok := e.(dataTypes.ErrNotFound); !ok {
			allErrorsNotFound = false
		}
		eCode, ok := e.(*nt.ErrHTTPCode)
		var err string
		if subj == "requests" && ok {
			err = eCode.Message()
		} else {
			err = e.Error()
		}

		if len(errStr) == 0 {
			errStr = err
		} else {
			if err != errStr {
				errStr = errStr + ", " + err
			}
		}
		if !ok {
			if optimisticCode == http.StatusOK {
				optimisticCode = http.StatusServiceUnavailable
			}
		} else if optimisticCode == http.StatusOK || optimisticCode == -1 || optimisticCode > eCode.Code() {
			optimisticCode = eCode.Code()
		}
	}

	if len(errStr) > 200 {
		errStr = errStr[0:200]
	}

	if nErrs < n {
		return nil, errStr
	}

	var err string
	if allErrorsNotFound {
		if subj == "requests" {
			err = "[" + errStr + "]"
		} else {
			err = "all " + subj +
				" not found: (" + errStr + ")"
		}
		return dataTypes.ErrNotFound(err), errStr
	}
	if subj == "requests" {
		err = "[" + errStr + "]"
	} else {
		err = "all " + subj +
			" failed: (" + errStr + ")"
	}
	return nt.NewErrHTTPCode(optimisticCode, err), errStr
}

func (app *App) sendRenderRequest(ctx context.Context, ch chan<- renderResponse,
	path string, from, until int32, toLog *carbonapipb.AccessLogDetails) {

	apiMetrics.RenderRequests.Add(1)
	atomic.AddInt64(&toLog.ZipperRequests, 1)

	request := dataTypes.NewRenderRequest([]string{path}, from, until)
	metrics, err := app.backend.Render(ctx, request)

	// time in queue is converted to ms
	app.prometheusMetrics.TimeInQueueExp.Observe(float64(request.Trace.Report()[2]) / 1000 / 1000)
	app.prometheusMetrics.TimeInQueueLin.Observe(float64(request.Trace.Report()[2]) / 1000 / 1000)

	metricData := make([]*types.MetricData, 0)
	for i := range metrics {
		metricData = append(metricData, &types.MetricData{
			Metric: metrics[i],
		})
	}

	ch <- renderResponse{
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
		t, err := strconv.Atoi(tstr)
		if err != nil {
			logger.Error("failed to parse cacheTimeout",
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
	res.from32 = date.DateParamToEpoch(res.from, res.qtz, timeNow().Add(-24*time.Hour).Unix(), app.defaultTimeZone)
	res.until32 = date.DateParamToEpoch(res.until, res.qtz, timeNow().Unix(), app.defaultTimeZone)

	accessLogDetails.UseCache = res.useCache
	accessLogDetails.FromRaw = res.from
	accessLogDetails.From = res.from32
	accessLogDetails.UntilRaw = res.until
	accessLogDetails.Until = res.until32
	accessLogDetails.Tz = res.qtz
	accessLogDetails.CacheTimeout = res.cacheTimeout
	accessLogDetails.Format = res.format
	accessLogDetails.Targets = res.targets

	return res, nil
}

func (app *App) renderWriteBody(results []*types.MetricData, form renderForm, r *http.Request, logger *zap.Logger) ([]byte, error) {
	var body []byte
	var err error

	switch form.format {
	case jsonFormat:
		if maxDataPoints, _ := strconv.Atoi(r.FormValue("maxDataPoints")); maxDataPoints != 0 {
			types.ConsolidateJSON(maxDataPoints, results)
		}

		body = types.MarshalJSON(results)
	case protobufFormat, protobuf3Format:
		body, err = types.MarshalProtobuf(results)
		if err != nil {
			return body, errors.Wrap(err, "error while marshalling protobuf")
		}
	case rawFormat:
		body = types.MarshalRaw(results)
	case csvFormat:
		tz := app.defaultTimeZone
		if form.qtz != "" {
			z, err := time.LoadLocation(form.qtz)
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
		body = types.MarshalPickle(results)
	case pngFormat:
		body = png.MarshalPNGRequest(r, results, form.template)
	case svgFormat:
		body = png.MarshalSVGRequest(r, results, form.template)
	}

	return body, nil
}

func (app *App) sendGlobs(glob dataTypes.Matches) bool {
	if app.config.AlwaysSendGlobsAsIs {
		return true
	}

	return app.config.SendGlobsAsIs && len(glob.Matches) < app.config.MaxBatchSize
}

func (app *App) resolveGlobsFromCache(metric string) (dataTypes.Matches, error) {
	tc := time.Now()
	blob, err := app.findCache.Get(metric)
	td := time.Since(tc).Nanoseconds()
	apiMetrics.FindCacheOverheadNS.Add(td)

	if err != nil {
		return dataTypes.Matches{}, err
	}

	matches, err := carbonapi_v2.FindDecoder(blob)
	if err != nil {
		return matches, err
	}

	apiMetrics.FindCacheHits.Add(1)

	return matches, nil
}

func (app *App) resolveGlobs(ctx context.Context, metric string, useCache bool, accessLogDetails *carbonapipb.AccessLogDetails, logger *zap.Logger) (dataTypes.Matches, error) {
	if useCache {
		matches, err := app.resolveGlobsFromCache(metric)
		if err == nil {
			return matches, nil
		}
	}

	apiMetrics.FindCacheMisses.Add(1)
	apiMetrics.FindRequests.Add(1)
	accessLogDetails.ZipperRequests++

	request := dataTypes.NewFindRequest(metric)
	request.IncCall()
	matches, err := app.backend.Find(ctx, request)
	if err != nil {
		return matches, err
	}

	blob, err := carbonapi_v2.FindEncoder(matches)
	if err == nil {
		tc := time.Now()
		app.findCache.Set(metric, blob, 5*60)
		td := time.Since(tc).Nanoseconds()
		apiMetrics.FindCacheOverheadNS.Add(td)
	}

	return matches, nil
}

func (app *App) getRenderRequests(ctx context.Context, m parser.MetricRequest, useCache bool,
	toLog *carbonapipb.AccessLogDetails, logger *zap.Logger) ([]string, error) {
	if app.config.AlwaysSendGlobsAsIs {
		toLog.SendGlobs = true
		return []string{m.Metric}, nil
	}
	if !strings.ContainsAny(m.Metric, "*{") {
		return []string{m.Metric}, nil
	}

	glob, err := app.resolveGlobs(ctx, m.Metric, useCache, toLog, logger)
	if err != nil {
		return nil, err
	}

	if app.sendGlobs(glob) {
		toLog.SendGlobs = true
		return []string{m.Metric}, nil
	}

	renderRequests := make([]string, 0, len(glob.Matches))
	for _, m := range glob.Matches {
		if m.IsLeaf {
			renderRequests = append(renderRequests, m.Path)
		}
	}

	return renderRequests, nil
}

func (app *App) findHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()

	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	format := r.FormValue("format")
	jsonp := r.FormValue("jsonp")
	query := r.FormValue("query")

	toLog := carbonapipb.NewAccessLogDetails(r, "find", &app.config)

	// TODO (grzkv): Pass logger in from above
	logger := zapwriter.Logger("find").With(
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
		zap.String("username", toLog.Username),
	)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &toLog, t0, logAsError)
	}()

	if format == "completer" {
		query = getCompleterQuery(query)
	}

	if format == "" {
		format = treejsonFormat
	}

	if query == "" {
		http.Error(w, "missing parameter `query`", http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = "missing parameter `query`"
		logAsError = true
		return
	}

	request := dataTypes.NewFindRequest(query)
	request.IncCall()
	metrics, err := app.backend.Find(ctx, request)

	if ctx.Err() != nil {
		app.prometheusMetrics.RequestCancel.WithLabelValues(
			"find", nt.ContextCancelCause(ctx.Err()),
		).Inc()
	}

	if err != nil {
		logger.Warn("zipper returned erro in find request",
			zap.String("uuid", util.GetUUID(ctx)),
			zap.Error(err),
		)
		if _, ok := errors.Cause(err).(dataTypes.ErrNotFound); ok {
			// graphite-web 0.9.12 needs to get a 200 OK response with an empty
			// body to be happy with its life, so we can't 404 a /metrics/find
			// request that finds nothing. We are however interested in knowing
			// that we found nothing on the monitoring side, so we claim we
			// returned a 404 code to Prometheus.
			app.prometheusMetrics.FindNotFound.Inc()
		} else {
			eCode, ok := err.(*nt.ErrHTTPCode)
			var code int
			if ok {
				code = eCode.Code()
			} else {
				code = http.StatusInternalServerError
			}

			toLog.HttpCode = int32(code)
			toLog.Reason = err.Error()
			logAsError = true

			http.Error(w, err.Error(), code)
			apiMetrics.Errors.Add(1)
			return
		}
	}

	var contentType string
	var blob []byte
	switch format {
	case protobufFormat, protobuf3Format:
		contentType = contentTypeProtobuf
		blob, err = carbonapi_v2.FindEncoder(metrics)
	case treejsonFormat, jsonFormat:
		contentType = contentTypeJSON
		blob, err = ourJson.FindEncoder(metrics)
	case "", pickleFormat:
		contentType = contentTypePickle
		if app.config.GraphiteWeb09Compatibility {
			blob, err = pickle.FindEncoderV0_9(metrics)
		} else {
			blob, err = pickle.FindEncoderV1_0(metrics)
		}
	case rawFormat:
		blob, err = findList(metrics)
		contentType = rawFormat
	case "completer":
		blob, err = findCompleter(metrics)
		contentType = jsonFormat
	default:
		err = errors.Errorf("Unknown format %s", format)
	}

	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		toLog.HttpCode = http.StatusInternalServerError
		toLog.Reason = err.Error()
		logAsError = true
		return
	}

	if contentType == jsonFormat && jsonp != "" {
		w.Header().Set("Content-Type", contentTypeJavaScript)
		w.Write([]byte(jsonp))
		w.Write([]byte{'('})
		w.Write(blob)
		w.Write([]byte{')'})
	} else {
		w.Header().Set("Content-Type", contentType)
		w.Write(blob)
	}

	toLog.HttpCode = http.StatusOK
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

func findList(globs dataTypes.Matches) ([]byte, error) {
	var b bytes.Buffer

	for _, g := range globs.Matches {

		var dot string
		// make sure non-leaves end in one dot
		if !g.IsLeaf && !strings.HasSuffix(g.Path, ".") {
			dot = "."
		}

		fmt.Fprintln(&b, g.Path+dot)
	}

	return b.Bytes(), nil
}

func (app *App) infoHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()

	format := r.FormValue("format")

	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	if format == "" {
		format = jsonFormat
	}

	toLog := carbonapipb.NewAccessLogDetails(r, "info", &app.config)
	toLog.Format = format

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &toLog, t0, logAsError)
	}()

	query := r.FormValue("target")
	if query == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = "no target specified"
		logAsError = true
		return
	}

	request := dataTypes.NewInfoRequest(query)
	request.IncCall()
	infos, err := app.backend.Info(ctx, request)
	if err != nil {
		eCode, ok := err.(*nt.ErrHTTPCode)
		var code int
		if ok {
			code = eCode.Code()
		} else {
			code = http.StatusInternalServerError
		}

		toLog.HttpCode = int32(code)
		toLog.Reason = err.Error()
		logAsError = true
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
		logAsError = true
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(b)

	toLog.Runtime = time.Since(t0).Seconds()
	toLog.HttpCode = http.StatusOK
}

func (app *App) lbcheckHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()

	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()
	defer func() {
		apiMetrics.Responses.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "lbcheck", "false").Inc()
	}()

	w.Write([]byte("Ok\n"))

	toLog := carbonapipb.NewAccessLogDetails(r, "lbcheck", &app.config)
	toLog.Runtime = time.Since(t0).Seconds()
	toLog.HttpCode = http.StatusOK
	// TODO (grzkv): Pass logger from above
	zapwriter.Logger("access").Info("request served", zap.Any("data", toLog))
}

func (app *App) versionHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()

	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()
	defer func() {
		apiMetrics.Responses.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "version", "false").Inc()
	}()
	// Use a specific version of graphite for grafana
	// This handler is queried by grafana, and if needed, an override can be provided
	if app.config.GraphiteVersionForGrafana != "" {
		w.Write([]byte(app.config.GraphiteVersionForGrafana))
		return
	}

	if app.config.GraphiteWeb09Compatibility {
		w.Write([]byte("0.9.15\n"))
	} else {
		w.Write([]byte("1.0.0\n"))
	}

	toLog := carbonapipb.NewAccessLogDetails(r, "version", &app.config)
	toLog.Runtime = time.Since(t0).Seconds()
	// TODO (grzkv): Pass logger from above
	zapwriter.Logger("access").Info("request served", zap.Any("data", toLog))
}

func (app *App) functionsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement helper for specific functions
	t0 := time.Now()

	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	toLog := carbonapipb.NewAccessLogDetails(r, "functions", &app.config)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &toLog, t0, logAsError)
	}()

	err := r.ParseForm()
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		toLog.Reason = err.Error()
		logAsError = true
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
		logAsError = true
		return
	}

	w.Write(b)
	toLog.Runtime = time.Since(t0).Seconds()
	toLog.HttpCode = http.StatusOK
}

// Add block rules on the basis of headers to block certain requests
// To be used to block read abusers
// The rules are added(appended) in the block headers config file
// Returns failure if handler is invoked and config entry is missing
// Otherwise, it creates the config file with the rule
func (app *App) blockHeaders(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	// TODO (grzkv): Pass logger from above
	logger := zapwriter.Logger("logger")

	apiMetrics.Requests.Add(1)

	toLog := carbonapipb.NewAccessLogDetails(r, "blockHeaders", &app.config)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &toLog, t0, logAsError)
	}()

	queryParams := r.URL.Query()

	m := make(Rule)
	for k, v := range queryParams {
		if k == "" || v[0] == "" {
			continue
		}
		m[k] = v[0]
	}
	w.Header().Set("Content-Type", contentTypeJSON)

	failResponse := []byte(`{"success":"false"}`)
	if app.config.BlockHeaderFile == "" {
		w.WriteHeader(http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		w.Write(failResponse)
		return
	}

	var ruleConfig RuleConfig
	var err1 error
	if len(m) == 0 {
		logger.Error("couldn't create a rule from params")
	} else {
		fileData, err := loadBlockRuleConfig(app.config.BlockHeaderFile)
		if err == nil {
			yaml.Unmarshal(fileData, &ruleConfig)
		}
		err1 = appendRuleToConfig(ruleConfig, m, logger, app.config.BlockHeaderFile)
	}

	if len(m) == 0 || err1 != nil {
		w.WriteHeader(http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		w.Write(failResponse)
		return
	}
	w.Write([]byte(`{"success":"true"}`))

	toLog.HttpCode = http.StatusOK
}

func appendRuleToConfig(ruleConfig RuleConfig, m Rule, logger *zap.Logger, blockHeaderFile string) error {
	ruleConfig.Rules = append(ruleConfig.Rules, m)
	output, err := yaml.Marshal(ruleConfig)
	if err == nil {
		logger.Info("updating file", zap.String("ruleConfig", string(output[:])))
		err = writeBlockRuleToFile(output, blockHeaderFile)
		if err != nil {
			logger.Error("couldn't write rule to file")
		}
	}
	return err
}

func writeBlockRuleToFile(output []byte, blockHeaderFile string) error {
	fileLock.Lock()
	defer fileLock.Unlock()
	err := ioutil.WriteFile(blockHeaderFile, output, 0644)
	return err
}

// It deletes the block headers config file
// Use it to remove all blocking rules, or to restart adding rules
// from scratch
func (app *App) unblockHeaders(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	apiMetrics.Requests.Add(1)
	toLog := carbonapipb.NewAccessLogDetails(r, "unblockHeaders", &app.config)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &toLog, t0, logAsError)
	}()

	w.Header().Set("Content-Type", contentTypeJSON)
	err := os.Remove(app.config.BlockHeaderFile)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		toLog.HttpCode = http.StatusBadRequest
		w.Write([]byte(`{"success":"false"}`))
		return
	}
	w.Write([]byte(`{"success":"true"}`))

	toLog.HttpCode = http.StatusOK
}

func isBlockingHeaderRule(r *http.Request, rule Rule) bool {
	for k, v := range rule {
		if r.Header.Get(k) != v {
			return false
		}
	}
	return true
}

func shouldBlockRequest(r *http.Request, rules []Rule) bool {
	for _, rule := range rules {
		if isBlockingHeaderRule(r, rule) {
			return true
		}
	}
	return false
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
	/metrics/find/?query=
	/info/?target=
	/functions/
	/tags/autoComplete/tags
`)

func (app *App) usageHandler(w http.ResponseWriter, r *http.Request) {
	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()
	defer func() {
		apiMetrics.Responses.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "usage", "false").Inc()
	}()

	w.Write(usageMsg)
}

//TODO : Fix this handler if and when tag support is added
// This responds to grafana's tag requests, which were falling through to the usageHandler,
// preventing a random, garbage list of tags (constructed from usageMsg) being added to the metrics list
func (app *App) tagsHandler(w http.ResponseWriter, r *http.Request) {
	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()
	defer func() {
		apiMetrics.Responses.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "tags", "false").Inc()
	}()
}

func (app *App) debugVersionHandler(w http.ResponseWriter, r *http.Request) {
	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()
	defer func() {
		apiMetrics.Responses.Add(1)
		app.prometheusMetrics.Responses.WithLabelValues(strconv.Itoa(http.StatusOK), "debugversion", "false").Inc()
	}()

	fmt.Fprintf(w, "GIT_TAG: %s\n", BuildVersion)
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
