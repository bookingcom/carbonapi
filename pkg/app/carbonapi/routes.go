package carbonapi

import (
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/bookingcom/carbonapi/pkg/handlerlog"
	"go.uber.org/zap"

	"github.com/bookingcom/carbonapi/pkg/util"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxtrace "go.opentelemetry.io/contrib/instrumentation/gorilla/mux"
)

func initHandlersInternal(app *App, logger *zap.Logger) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/block-headers", handlerlog.WithLogger(app.blockHeaders, logger))
	r.HandleFunc("/unblock-headers", handlerlog.WithLogger(app.unblockHeaders, logger))

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

	r.HandleFunc("/render", app.validateRequest(app.renderHandler, "render", lg))
	r.HandleFunc("/metrics/find", app.validateRequest(app.findHandler, "find", lg))
	r.HandleFunc("/info", app.validateRequest(app.infoHandler, "info", lg))
	r.HandleFunc("/lb_check", handlerlog.WithLogger(app.lbcheckHandler, lg))
	r.HandleFunc("/version", handlerlog.WithLogger(app.versionHandler, lg))
	r.HandleFunc("/functions", handlerlog.WithLogger(app.functionsHandler, lg))
	r.HandleFunc("/tags/autoComplete/tags", handlerlog.WithLogger(app.tagsHandler, lg))
	r.HandleFunc("/", handlerlog.WithLogger(app.usageHandler, lg))

	r.NotFoundHandler = handlerlog.WithLogger(app.usageHandler, lg)

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
