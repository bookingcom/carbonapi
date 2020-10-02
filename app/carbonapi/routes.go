package carbonapi

import (
	"expvar"
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

func initHandlersInternal(app *App) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/block-headers", httputil.TimeHandler(app.blockHeaders, app.bucketRequestTimes))

	r.HandleFunc("/unblock-headers", httputil.TimeHandler(app.unblockHeaders, app.bucketRequestTimes))

	r.HandleFunc("/debug/version", app.debugVersionHandler)

	r.Handle("/debug/vars", expvar.Handler())
	r.PathPrefix("/debug/pprof").HandlerFunc(pprof.Index)

	r.Handle("/metrics", promhttp.Handler())

	return routeMiddleware(r)
}

func initHandlers(app *App) http.Handler {
	r := mux.NewRouter()

	r.Use(handlers.CompressHandler)
	r.Use(handlers.CORS())
	r.Use(handlers.ProxyHeaders)
	r.Use(util.UUIDHandler)
	r.Use(muxtrace.Middleware("carbonapi"))

	r.HandleFunc("/render", httputil.TimeHandler(
		app.validateRequest(http.HandlerFunc(app.renderHandler), "render"), app.bucketRequestTimes))

	r.HandleFunc("/metrics/find", httputil.TimeHandler(
		app.validateRequest(http.HandlerFunc(app.findHandler), "find"), app.bucketRequestTimes))

	r.HandleFunc("/info", httputil.TimeHandler(
		app.validateRequest(http.HandlerFunc(app.infoHandler), "info"), app.bucketRequestTimes))

	r.HandleFunc("/lb_check", httputil.TimeHandler(app.lbcheckHandler, app.bucketRequestTimes))

	r.HandleFunc("/version", httputil.TimeHandler(app.versionHandler, app.bucketRequestTimes))

	r.HandleFunc("/functions", httputil.TimeHandler(app.functionsHandler, app.bucketRequestTimes))

	r.HandleFunc("/tags/autoComplete/tags", httputil.TimeHandler(app.tagsHandler, app.bucketRequestTimes))

	r.HandleFunc("/", httputil.TimeHandler(app.usageHandler, app.bucketRequestTimes))

	r.NotFoundHandler = httputil.TimeHandler(app.usageHandler, app.bucketRequestTimes)

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
