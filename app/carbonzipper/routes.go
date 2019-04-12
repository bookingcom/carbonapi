package zipper

import (
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/dgryski/httputil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func initHandlers(app *App) http.Handler {
	r := http.NewServeMux()

	r.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(app.findHandler, app.bucketRequestTimes)))
	r.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(app.renderHandler, app.bucketRequestTimes)))
	r.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(app.infoHandler, app.bucketRequestTimes)))
	r.HandleFunc("/lb_check", app.lbCheckHandler)

	return r
}

func initMetricHandlers(app *App) http.Handler {
	r := http.NewServeMux()

	r.Handle("/metrics", promhttp.Handler())

	r.Handle("/debug/vars", expvar.Handler())
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return r
}
