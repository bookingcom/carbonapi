package carbonapi

import (
	"expvar"
	"github.com/bookingcom/carbonapi/pkg/handlerlog"
	"go.uber.org/zap"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/bookingcom/carbonapi/util"
	"github.com/dgryski/httputil"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxtrace "go.opentelemetry.io/contrib/instrumentation/gorilla/mux"
)

func initHandlersInternal(app *App, accessLogger, handlerLogger *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/block-headers", httputil.TimeHandler(handlerlog.WithLogger(app.blockHeaders, accessLogger, handlerLogger), app.bucketRequestTimes))

	r.HandleFunc("/unblock-headers", httputil.TimeHandler(handlerlog.WithLogger(app.unblockHeaders, accessLogger, handlerLogger), app.bucketRequestTimes))

	r.HandleFunc("/debug/version", app.debugVersionHandler)

	r.Handle("/debug/vars", expvar.Handler())
	r.PathPrefix("/debug/pprof").HandlerFunc(pprof.Index)

	r.Handle("/metrics", promhttp.Handler())

	return routeMiddleware(r)
}

func initHandlers(app *App, accessLogger, handlerLogger *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.Use(handlers.CompressHandler)
	r.Use(handlers.CORS())
	r.Use(handlers.ProxyHeaders)
	r.Use(util.UUIDHandler)
	r.Use(muxtrace.Middleware("carbonapi"))

	r.HandleFunc("/render", httputil.TimeHandler(
		app.validateRequest(app.renderHandler, "render", accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/metrics/find", httputil.TimeHandler(
		app.validateRequest(app.findHandler, "find", accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/info", httputil.TimeHandler(
		app.validateRequest(app.infoHandler, "info", accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/lb_check", httputil.TimeHandler(
		handlerlog.WithLogger(app.lbcheckHandler, accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/version", httputil.TimeHandler(
		handlerlog.WithLogger(app.versionHandler, accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/functions", httputil.TimeHandler(
		handlerlog.WithLogger(app.functionsHandler, accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/tags/autoComplete/tags", httputil.TimeHandler(
		handlerlog.WithLogger(app.tagsHandler, accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.HandleFunc("/", httputil.TimeHandler(
		handlerlog.WithLogger(app.usageHandler, accessLogger, handlerLogger),
		app.bucketRequestTimes))

	r.NotFoundHandler = httputil.TimeHandler(
		handlerlog.WithLogger(app.usageHandler, accessLogger, handlerLogger),
		app.bucketRequestTimes)

	return routeMiddleware(r)
}

// routeHelper formats the route using regex to accept optional trailing slash
func routeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			r.URL.Path = strings.TrimSuffix(r.URL.Path, "/")
		}
		next.ServeHTTP(w, r)
	})
}
