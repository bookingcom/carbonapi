package zipper

import (
	"expvar"
	"github.com/bookingcom/carbonapi/pkg/handlerlog"
	"go.uber.org/zap"
	"net/http"
	"net/http/pprof"

	"github.com/bookingcom/carbonapi/util"
	"github.com/dgryski/httputil"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxtrace "go.opentelemetry.io/contrib/instrumentation/gorilla/mux"
)

func initHandlers(app *App, accessLogger, handlerLogger *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.Use(util.UUIDHandler)
	r.Use(muxtrace.Middleware("carbonzipper"))

	r.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(handlerlog.WithLogger(app.findHandler, accessLogger, handlerLogger), app.bucketRequestTimes)))
	r.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(handlerlog.WithLogger(app.renderHandler, accessLogger, handlerLogger), app.bucketRequestTimes)))
	r.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(handlerlog.WithLogger(app.infoHandler, accessLogger, handlerLogger), app.bucketRequestTimes)))
	r.HandleFunc("/lb_check", handlerlog.WithLogger(app.lbCheckHandler, accessLogger, handlerLogger))

	return r
}

func initMetricHandlers() http.Handler {
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	r.Handle("/debug/vars", expvar.Handler())
	r.PathPrefix("/debug/pprof").HandlerFunc(pprof.Index)

	return r
}
