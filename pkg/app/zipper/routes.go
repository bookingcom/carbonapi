package zipper

import (
	"net/http"
	"net/http/pprof"

	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/dgryski/httputil"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxtrace "go.opentelemetry.io/contrib/instrumentation/gorilla/mux"
)

func initHandlers(app *App, ms *PrometheusMetrics, lg *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.Use(util.UUIDHandler)
	r.Use(muxtrace.Middleware("carbonzipper"))

	r.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(withMetricsAndLogger(app.findHandler, ms, lg), app.bucketRequestTimes)))
	r.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(withMetricsAndLogger(app.renderHandler, ms, lg), app.bucketRequestTimes)))
	r.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(withMetricsAndLogger(app.infoHandler, ms, lg), app.bucketRequestTimes)))
	r.HandleFunc("/lb_check", withMetricsAndLogger(app.lbCheckHandler, ms, lg))

	return r
}

func initMetricHandlers() http.Handler {
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	r.PathPrefix("/debug/pprof").HandlerFunc(pprof.Index)

	return r
}

func withMetricsAndLogger(handlerFunc func(w http.ResponseWriter, r *http.Request, ms *PrometheusMetrics, lg *zap.Logger),
	ms *PrometheusMetrics, lg *zap.Logger) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(w, r, ms, lg)
	}
}
