package carbonapi

import (
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/dgryski/httputil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func initHandlersInternal(app *App) http.Handler {
	r := http.NewServeMux()

	r.HandleFunc("/block-headers/", httputil.TimeHandler(app.blockHeaders, app.bucketRequestTimes))
	r.HandleFunc("/block-headers", httputil.TimeHandler(app.blockHeaders, app.bucketRequestTimes))

	r.HandleFunc("/unblock-headers/", httputil.TimeHandler(app.unblockHeaders, app.bucketRequestTimes))
	r.HandleFunc("/unblock-headers", httputil.TimeHandler(app.unblockHeaders, app.bucketRequestTimes))

	r.HandleFunc("/debug/version", app.debugVersionHandler)

	r.Handle("/debug/vars", expvar.Handler())
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	r.Handle("/metrics", promhttp.Handler())

	return r
}

func initHandlers(app *App) http.Handler {
	r := http.NewServeMux()

	r.HandleFunc("/render/", httputil.TimeHandler(app.validateRequest(http.HandlerFunc(app.renderHandler), "render"), app.bucketRequestTimes))
	r.HandleFunc("/render", httputil.TimeHandler(app.validateRequest(http.HandlerFunc(app.renderHandler), "render"), app.bucketRequestTimes))

	r.HandleFunc("/metrics/find/", httputil.TimeHandler(app.validateRequest(http.HandlerFunc(app.findHandler), "find"), app.bucketRequestTimes))
	r.HandleFunc("/metrics/find", httputil.TimeHandler(app.validateRequest(http.HandlerFunc(app.findHandler), "find"), app.bucketRequestTimes))

	r.HandleFunc("/info/", httputil.TimeHandler(app.validateRequest(http.HandlerFunc(app.infoHandler), "info"), app.bucketRequestTimes))
	r.HandleFunc("/info", httputil.TimeHandler(app.validateRequest(http.HandlerFunc(app.infoHandler), "info"), app.bucketRequestTimes))

	r.HandleFunc("/lb_check", httputil.TimeHandler(app.lbcheckHandler, app.bucketRequestTimes))

	r.HandleFunc("/version", httputil.TimeHandler(app.versionHandler, app.bucketRequestTimes))
	r.HandleFunc("/version/", httputil.TimeHandler(app.versionHandler, app.bucketRequestTimes))

	r.HandleFunc("/functions", httputil.TimeHandler(app.functionsHandler, app.bucketRequestTimes))
	r.HandleFunc("/functions/", httputil.TimeHandler(app.functionsHandler, app.bucketRequestTimes))

	r.HandleFunc("/tags/autoComplete/tags", httputil.TimeHandler(app.tagsHandler, app.bucketRequestTimes))

	r.HandleFunc("/", httputil.TimeHandler(app.usageHandler, app.bucketRequestTimes))

	return r
}
