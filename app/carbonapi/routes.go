package carbonapi

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/dgryski/httputil"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func initHandlersInternal(app *App) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc(handleTrailingSlash("block-headers"), httputil.TimeHandler(app.blockHeaders, app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("/unblock-headers"), httputil.TimeHandler(app.unblockHeaders, app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("debug/version"), app.debugVersionHandler)

	r.Handle(handleTrailingSlash("debug/vars"), expvar.Handler())
	r.HandleFunc(handleTrailingSlash("debug/pprof"), pprof.Index)
	s := r.Host(handleTrailingSlash("debug/pprof")).Subrouter()
	s.HandleFunc(handleTrailingSlash("cmdline"), pprof.Cmdline)
	s.HandleFunc(handleTrailingSlash("profile"), pprof.Profile)
	s.HandleFunc(handleTrailingSlash("symbol"), pprof.Symbol)
	s.HandleFunc(handleTrailingSlash("trace"), pprof.Trace)

	r.Handle(handleTrailingSlash("metrics"), promhttp.Handler())

	return r
}

func initHandlers(app *App) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc(handleTrailingSlash("render"), httputil.TimeHandler(
		app.validateRequest(http.HandlerFunc(app.renderHandler), "render"), app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("metrics/find"), httputil.TimeHandler(
		app.validateRequest(http.HandlerFunc(app.findHandler), "find"), app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("info"), httputil.TimeHandler(
		app.validateRequest(http.HandlerFunc(app.infoHandler), "info"), app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("lb_check"), httputil.TimeHandler(app.lbcheckHandler, app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("version"), httputil.TimeHandler(app.versionHandler, app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("functions"), httputil.TimeHandler(app.functionsHandler, app.bucketRequestTimes))

	r.HandleFunc(handleTrailingSlash("tags/autoComplete/tags"), httputil.TimeHandler(app.tagsHandler, app.bucketRequestTimes))

	r.HandleFunc("/", httputil.TimeHandler(app.usageHandler, app.bucketRequestTimes))

	return r
}

// routeHelper formats the route using regex to accept optional trailing slash
func handleTrailingSlash(route string) string {
	return fmt.Sprintf("/{%s:%s\\/?}", route, route)
}
