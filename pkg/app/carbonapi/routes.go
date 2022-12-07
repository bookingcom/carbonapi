package carbonapi

import (
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/bookingcom/carbonapi/pkg/handlerlog"
	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/dgryski/httputil"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxtrace "go.opentelemetry.io/contrib/instrumentation/gorilla/mux"
)

func initHandlersInternal(app *App, logger *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/block-headers", httputil.TimeHandler(handlerlog.WithLogger(app.blockHeaders, logger), app.bucketRequestTimes))
	r.HandleFunc("/unblock-headers", httputil.TimeHandler(handlerlog.WithLogger(app.unblockHeaders, logger), app.bucketRequestTimes))

	r.Handle("/metrics", promhttp.Handler())

	r.HandleFunc("/debug/pprof", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return removeTrailingSlash(r)
}

func initHandlers(app *App, lg *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.Use(handlers.CompressHandler)
	r.Use(handlers.CORS())
	r.Use(handlers.ProxyHeaders)
	r.Use(util.UUIDHandler)
	r.Use(muxtrace.Middleware("carbonapi"))

	r.HandleFunc("/render", httputil.TimeHandler(
		app.validateRequest(app.renderHandler, "render", lg),
		app.bucketRequestTimes))

	r.HandleFunc("/metrics/find", httputil.TimeHandler(
		app.validateRequest(app.findHandler, "find", lg),
		app.bucketRequestTimes))

	r.HandleFunc("/info", httputil.TimeHandler(
		app.validateRequest(app.infoHandler, "info", lg),
		app.bucketRequestTimes))

	r.HandleFunc("/lb_check", httputil.TimeHandler(
		handlerlog.WithLogger(app.lbcheckHandler, lg),
		app.bucketRequestTimes))

	r.HandleFunc("/version", httputil.TimeHandler(
		handlerlog.WithLogger(app.versionHandler, lg),
		app.bucketRequestTimes))

	r.HandleFunc("/functions", httputil.TimeHandler(
		handlerlog.WithLogger(app.functionsHandler, lg),
		app.bucketRequestTimes))

	r.HandleFunc("/tags/autoComplete/tags", httputil.TimeHandler(
		handlerlog.WithLogger(app.tagsHandler, lg),
		app.bucketRequestTimes))

	r.HandleFunc("/", httputil.TimeHandler(
		handlerlog.WithLogger(app.usageHandler, lg),
		app.bucketRequestTimes))

	r.NotFoundHandler = httputil.TimeHandler(
		handlerlog.WithLogger(app.usageHandler, lg),
		app.bucketRequestTimes)

	return removeTrailingSlash(r)
}

func removeTrailingSlash(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			r.URL.Path = strings.TrimSuffix(r.URL.Path, "/")
		}
		next.ServeHTTP(w, r)
	})
}
