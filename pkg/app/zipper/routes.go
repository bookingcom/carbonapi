package zipper

import (
	"net/http"
	"net/http/pprof"

	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxtrace "go.opentelemetry.io/contrib/instrumentation/gorilla/mux"
)

func initHandlers(app *App, ms *PrometheusMetrics, lg *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.Use(util.UUIDHandler)
	r.Use(muxtrace.Middleware("carbonzipper"))

	r.HandleFunc("/metrics/find/", withMetricsAndLogger(app.findHandler, ms, lg))
	r.HandleFunc("/render/", withMetricsAndLogger(app.renderHandler, ms, lg))
	r.HandleFunc("/info/", withMetricsAndLogger(app.infoHandler, ms, lg))
	r.HandleFunc("/lb_check", withMetricsAndLogger(app.lbCheckHandler, ms, lg))

	return r
}

func initMetricHandlers() http.Handler {
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	r.HandleFunc("/debug/pprof", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return r
}

func withMetricsAndLogger(handlerFunc func(w http.ResponseWriter, r *http.Request, ms *PrometheusMetrics, lg *zap.Logger),
	ms *PrometheusMetrics, lg *zap.Logger) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(w, r, ms, lg)
	}
}
