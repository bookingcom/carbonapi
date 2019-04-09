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
	"github.com/bookingcom/carbonapi/pkg/backend"
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

var fileLock sync.Mutex

func (app *App) validateRequest(h http.Handler, handler string) http.HandlerFunc {
	t0 := time.Now()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blockingRules := app.blockHeaderRules.Load().(RuleConfig)
		if shouldBlockRequest(r, blockingRules.Rules) {
			accessLogDetails := carbonapipb.NewAccessLogDetails(r, handler, &app.config)
			accessLogDetails.HttpCode = http.StatusForbidden
			defer func() {
				app.deferredAccessLogging(r, &accessLogDetails, t0, true)
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

// TODO (grzkv): This is an intermediate stub. Remove it
func findErrorsFanIn(ctx context.Context, err error, logger *zap.Logger) error {
	errs := []error{err}
	return backend.CheckErrs(ctx, errs, 1, logger)
}

// TODO (grzkv): This is an intermediate stub. Remove it
func renderErrorsFanIn(ctx context.Context, err error, logger *zap.Logger) error {
	errs := []error{err}
	return backend.CheckErrs(ctx, errs, 1, logger)
}

func (app *App) renderHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(r.Context(), app.config.Timeouts.Global)
	defer cancel()

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "render", &app.config)
	// TODO (grzkv): Pass logger from above
	logger := zapwriter.Logger("render").With(
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
		zap.String("username", accessLogDetails.Username),
	)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &accessLogDetails, t0, logAsError)
	}()

	size := 0
	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	// TODO (grzkv): Move form management to a separate function
	err := r.ParseForm()
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		accessLogDetails.HttpCode = http.StatusBadRequest
		accessLogDetails.Reason = err.Error()
		logAsError = true
		return
	}

	targets := r.Form["target"]
	from := r.FormValue("from")
	until := r.FormValue("until")
	format := r.FormValue("format")
	template := r.FormValue("template")
	useCache := !parser.TruthyBool(r.FormValue("noCache"))

	var jsonp string

	if format == jsonFormat {
		// TODO(dgryski): check jsonp only has valid characters
		jsonp = r.FormValue("jsonp")
	}

	if format == "" && (parser.TruthyBool(r.FormValue("rawData")) || parser.TruthyBool(r.FormValue("rawdata"))) {
		format = rawFormat
	}

	if format == "" {
		format = pngFormat
	}

	cacheTimeout := app.config.Cache.DefaultTimeoutSec

	if tstr := r.FormValue("cacheTimeout"); tstr != "" {
		t, err := strconv.Atoi(tstr)
		if err != nil {
			logger.Error("failed to parse cacheTimeout",
				zap.String("cache_string", tstr),
				zap.Error(err),
			)
		} else {
			cacheTimeout = int32(t)
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

	cacheKey := r.Form.Encode()

	// normalize from and until values
	qtz := r.FormValue("tz")
	from32 := date.DateParamToEpoch(from, qtz, timeNow().Add(-24*time.Hour).Unix(), app.defaultTimeZone)
	until32 := date.DateParamToEpoch(until, qtz, timeNow().Unix(), app.defaultTimeZone)

	accessLogDetails.UseCache = useCache
	accessLogDetails.FromRaw = from
	accessLogDetails.From = from32
	accessLogDetails.UntilRaw = until
	accessLogDetails.Until = until32
	accessLogDetails.Tz = qtz
	accessLogDetails.CacheTimeout = cacheTimeout
	accessLogDetails.Format = format
	accessLogDetails.Targets = targets
	if useCache {
		tc := time.Now()
		response, err := app.queryCache.Get(cacheKey)
		td := time.Since(tc).Nanoseconds()
		apiMetrics.RenderCacheOverheadNS.Add(td)

		accessLogDetails.CarbonzipperResponseSizeBytes = 0
		accessLogDetails.CarbonapiResponseSizeBytes = int64(len(response))

		if err == nil {
			apiMetrics.RequestCacheHits.Add(1)
			writeResponse(ctx, w, response, format, jsonp)
			accessLogDetails.FromCache = true
			return
		}
		apiMetrics.RequestCacheMisses.Add(1)
	}

	if from32 == until32 {
		http.Error(w, "Invalid empty time range", http.StatusBadRequest)
		accessLogDetails.HttpCode = http.StatusBadRequest
		accessLogDetails.Reason = "invalid empty time range"
		logAsError = true
		return
	}

	var results []*types.MetricData
	errors := make(map[string]string)
	metricMap := make(map[parser.MetricRequest][]*types.MetricData)

	var metrics []string
	var targetIdx = 0
	// TODO(gmagnusson): Put the body of this loop in a select { } and cancel work
	for targetIdx < len(targets) {
		var target = targets[targetIdx]
		targetIdx++

		exp, e, err := parser.ParseExpr(target)
		if err != nil || e != "" {
			msg := buildParseErrorString(target, e, err)
			http.Error(w, msg, http.StatusBadRequest)
			accessLogDetails.Reason = msg
			accessLogDetails.HttpCode = http.StatusBadRequest
			logAsError = true
			return
		}

		var targetMetricFetches []parser.MetricRequest
		for _, m := range exp.Metrics() {
			metrics = append(metrics, m.Metric)
			mfetch := m
			mfetch.From += from32
			mfetch.Until += until32

			targetMetricFetches = append(targetMetricFetches, mfetch)
			if _, ok := metricMap[mfetch]; ok {
				// already fetched this metric for this request
				continue
			}

			// This _sometimes_ sends a *find* request
			renderRequests, err := app.getRenderRequests(ctx, m, useCache, &accessLogDetails, logger)
			if err != nil {
				logger.Error("error expanding globs for render request",
					zap.String("metric", m.Metric),
					zap.Error(err),
				)
				continue
			}

			// TODO(dgryski): group the render requests into batches
			rch := make(chan renderResponse, len(renderRequests))
			for _, m := range renderRequests {
				go func(path string, from, until int32) {
					apiMetrics.RenderRequests.Add(1)
					atomic.AddInt64(&accessLogDetails.ZipperRequests, 1)

					request := dataTypes.NewRenderRequest([]string{path}, from, until)
					metrics, err := app.backend.Render(ctx, request)
					err = renderErrorsFanIn(ctx, err, logger)

					// time in queue is converted to ms
					app.prometheusMetrics.TimeInQueueExp.Observe(float64(request.Trace.Report()[2]) / 1000 / 1000)
					app.prometheusMetrics.TimeInQueueLin.Observe(float64(request.Trace.Report()[2]) / 1000 / 1000)

					metricData := make([]*types.MetricData, 0)
					for i := range metrics {
						metricData = append(metricData, &types.MetricData{
							Metric: metrics[i],
						})
					}

					rch <- renderResponse{
						data:  metricData,
						error: err,
					}
				}(m, mfetch.From, mfetch.Until)
			}

			errors := make([]error, 0)
			for i := 0; i < len(renderRequests); i++ {
				resp := <-rch
				if resp.error != nil {
					errors = append(errors, resp.error)
					continue
				}

				for _, r := range resp.data {
					size += 8 * len(r.Values) // close enough
					metricMap[mfetch] = append(metricMap[mfetch], r)
				}
			}
			accessLogDetails.CarbonzipperResponseSizeBytes += int64(size)
			close(rch)

			if len(errors) != 0 {
				logger.Error("render error occurred while fetching data",
					zap.Any("errors", errors),
				)
			}

			expr.SortMetrics(metricMap[mfetch], mfetch)
		}
		accessLogDetails.Metrics = metrics

		var rewritten bool
		var newTargets []string
		logStepTimeMismatch(targetMetricFetches, metricMap, logger, target)
		rewritten, newTargets, err = expr.RewriteExpr(exp, from32, until32, metricMap)
		if err != nil && err != parser.ErrSeriesDoesNotExist {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			errors[target] = err.Error()
			accessLogDetails.Reason = err.Error()
			logAsError = true
			return
		}

		if rewritten {
			// TODO(gmagnusson): Have the loop be
			//
			//		for i := 0; i < total; i++
			//
			// and update total here with len(newTargets) so we actually
			// end up looking at any of the things in there.
			//
			// Ugh, I'm now paranoid that the compiler or the runtime will
			// inline 'total' at some point in the future as an optimization.
			// Maybe have the loop instead be:
			//
			// for {
			//		if len(targets) == 0 {
			//			break
			//		}
			//
			//		target = targets[0]
			//		targets = targets[1:]
			// }
			//
			// If it walks like a stack, and it quacks like a stack ...

			targets = append(targets, newTargets...)
			continue
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("panic during eval:",
						zap.String("cache_key", cacheKey),
						zap.Any("reason", r),
						zap.Stack("stack"),
					)
				}
			}()

			exprs, err := expr.EvalExpr(exp, from32, until32, metricMap)
			if err != nil {
				if err != parser.ErrSeriesDoesNotExist {
					errors[target] = err.Error()
					accessLogDetails.Reason = err.Error()
					logAsError = true
				}

				// If err == parser.ErrSeriesDoesNotExist, exprs == nil, so we
				// can just return here.
				return
			}

			results = append(results, exprs...)
		}()
	}

	var body []byte

	switch format {
	case jsonFormat:
		if maxDataPoints, _ := strconv.Atoi(r.FormValue("maxDataPoints")); maxDataPoints != 0 {
			types.ConsolidateJSON(maxDataPoints, results)
		}

		body = types.MarshalJSON(results)
	case protobufFormat, protobuf3Format:
		body, err = types.MarshalProtobuf(results)
		if err != nil {
			logger.Info("request failed",
				zap.Int("http_code", http.StatusInternalServerError),
				zap.String("reason", err.Error()),
				zap.Duration("runtime", time.Since(t0)),
			)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			accessLogDetails.HttpCode = http.StatusInternalServerError
			logAsError = true
			return
		}
	case rawFormat:
		body = types.MarshalRaw(results)
	case csvFormat:
		tz := app.defaultTimeZone
		if qtz != "" {
			z, err := time.LoadLocation(qtz)
			if err != nil {
				logger.Warn("Invalid time zone",
					zap.String("tz", qtz),
				)
			} else {
				tz = z
			}
		}
		body = types.MarshalCSV(results, tz)
	case pickleFormat:
		body = types.MarshalPickle(results)
	case pngFormat:
		body = png.MarshalPNGRequest(r, results, template)
	case svgFormat:
		body = png.MarshalSVGRequest(r, results, template)
	}

	writeResponse(ctx, w, body, format, jsonp)

	if len(results) != 0 {
		tc := time.Now()
		app.queryCache.Set(cacheKey, body, cacheTimeout)
		td := time.Since(tc).Nanoseconds()
		apiMetrics.RenderCacheOverheadNS.Add(td)
	}

	accessLogDetails.HaveNonFatalErrors = len(errors) > 0
}

func (app *App) sendGlobs(glob dataTypes.Matches) bool {
	// Yay globals
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
	err = findErrorsFanIn(ctx, err, logger)
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

func (app *App) getRenderRequests(ctx context.Context, m parser.MetricRequest, useCache bool, accessLogDetails *carbonapipb.AccessLogDetails, logger *zap.Logger) ([]string, error) {
	if app.config.AlwaysSendGlobsAsIs {
		accessLogDetails.SendGlobs = true
		return []string{m.Metric}, nil
	}

	glob, err := app.resolveGlobs(ctx, m.Metric, useCache, accessLogDetails, logger)
	if err != nil {
		return nil, err
	}

	if app.sendGlobs(glob) {
		accessLogDetails.SendGlobs = true
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

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "find", &app.config)

	// TODO (grzkv): Pass logger in from above
	logger := zapwriter.Logger("find").With(
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
		zap.String("username", accessLogDetails.Username),
	)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &accessLogDetails, t0, logAsError)
	}()

	if format == "completer" {
		query = getCompleterQuery(query)
	}

	if format == "" {
		format = treejsonFormat
	}

	if query == "" {
		http.Error(w, "missing parameter `query`", http.StatusBadRequest)
		accessLogDetails.HttpCode = http.StatusBadRequest
		accessLogDetails.Reason = "missing parameter `query`"
		logAsError = true
		return
	}

	request := dataTypes.NewFindRequest(query)
	request.IncCall()
	metrics, err := app.backend.Find(ctx, request)
	err = findErrorsFanIn(ctx, err, logger)
	if err != nil {
		if _, ok := errors.Cause(err).(dataTypes.ErrNotFound); ok {
			// graphite-web 0.9.12 needs to get a 200 OK response with an empty
			// body to be happy with its life, so we can't 404 a /metrics/find
			// request that finds nothing. We are however interested in knowing
			// that we found nothing on the monitoring side, so we claim we
			// returned a 404 code to Prometheus.
			// TODO (grzkv) This creates confusion: people see 404 in metrics, when
			// in reality it does not exist. We need to find an alternative way to mark this.
			apiMetrics.Errors.Add(1) // TODO (grzkv): Not sure if this is really an error
			// app.prometheusMetrics.Responses.WithLabelValues("404", "find").Inc()
		} else {
			msg := "error fetching the data"
			code := http.StatusInternalServerError

			accessLogDetails.HttpCode = int32(code)
			accessLogDetails.Reason = err.Error()
			logAsError = true

			http.Error(w, msg, code)
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
		accessLogDetails.HttpCode = http.StatusInternalServerError
		accessLogDetails.Reason = err.Error()
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

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "info", &app.config)
	accessLogDetails.Format = format

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &accessLogDetails, t0, logAsError)
	}()

	query := r.FormValue("target")
	if query == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		accessLogDetails.HttpCode = http.StatusBadRequest
		accessLogDetails.Reason = "no target specified"
		logAsError = true
		return
	}

	request := dataTypes.NewInfoRequest(query)
	request.IncCall()
	infos, err := app.backend.Info(ctx, request)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		accessLogDetails.HttpCode = http.StatusInternalServerError
		accessLogDetails.Reason = err.Error()
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
		accessLogDetails.HttpCode = http.StatusInternalServerError
		accessLogDetails.Reason = err.Error()
		logAsError = true
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(b)

	accessLogDetails.Runtime = time.Since(t0).Seconds()
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

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "lbcheck", &app.config)
	accessLogDetails.Runtime = time.Since(t0).Seconds()
	// TODO (grzkv): Pass logger from above
	zapwriter.Logger("access").Info("request served", zap.Any("data", accessLogDetails))
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

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "version", &app.config)
	accessLogDetails.Runtime = time.Since(t0).Seconds()
	// TODO (grzkv): Pass logger from above
	zapwriter.Logger("access").Info("request served", zap.Any("data", accessLogDetails))
}

func (app *App) functionsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement helper for specific functions
	t0 := time.Now()

	apiMetrics.Requests.Add(1)
	app.prometheusMetrics.Requests.Inc()

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "functions", &app.config)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &accessLogDetails, t0, logAsError)
	}()

	err := r.ParseForm()
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		accessLogDetails.HttpCode = http.StatusBadRequest
		accessLogDetails.Reason = err.Error()
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
		accessLogDetails.HttpCode = http.StatusInternalServerError
		accessLogDetails.Reason = err.Error()
		logAsError = true
		return
	}

	w.Write(b)
	accessLogDetails.Runtime = time.Since(t0).Seconds()
	accessLogDetails.HttpCode = http.StatusOK
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

	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "blockHeaders", &app.config)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &accessLogDetails, t0, logAsError)
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
		accessLogDetails.HttpCode = http.StatusBadRequest
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
		accessLogDetails.HttpCode = http.StatusBadRequest
		w.Write(failResponse)
		return
	}
	w.Write([]byte(`{"success":"true"}`))
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
	accessLogDetails := carbonapipb.NewAccessLogDetails(r, "unblockHeaders", &app.config)

	logAsError := false
	defer func() {
		app.deferredAccessLogging(r, &accessLogDetails, t0, logAsError)
	}()

	w.Header().Set("Content-Type", contentTypeJSON)
	err := os.Remove(app.config.BlockHeaderFile)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		accessLogDetails.HttpCode = http.StatusBadRequest
		w.Write([]byte(`{"success":"false"}`))
		return
	}
	w.Write([]byte(`{"success":"true"}`))
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
