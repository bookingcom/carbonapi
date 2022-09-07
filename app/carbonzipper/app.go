package zipper

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"

	"github.com/dgryski/go-expirecache"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/pkg/backend"

	"github.com/dgryski/httputil"
	"github.com/facebookgo/grace/gracehttp"
	"go.uber.org/zap"
)

// App represents the main zipper runnable
// TODO: Remove.
type App struct {
	Config              cfg.Zipper
	Metrics             *PrometheusMetrics
	Backends            []backend.Backend
	TopLevelDomainCache *expirecache.Cache
}

// Start start launches the goroutines starts the app execution
func (app *App) Start(logger *zap.Logger) {
	timeBuckets = make([]int64, app.Config.Buckets+1)
	expTimeBuckets = make([]int64, app.Config.Buckets+1)

	httputil.PublishTrackedConnections("httptrack")

	handler := initHandlers(app, logger)

	if app.Config.Graphite.Pattern == "" {
		app.Config.Graphite.Pattern = "{prefix}.{fqdn}"
	}

	if app.Config.Graphite.Prefix == "" {
		app.Config.Graphite.Prefix = "carbon.zipper"
	}

	// only register g2g if we have a graphite host
	if app.Config.Graphite.Host != "" {
		initGraphite(app)
	}

	go app.probeTopLevelDomains()
	metricsServer := metricsServer(app)

	gracehttp.SetLogger(zap.NewStdLog(logger))
	err := gracehttp.Serve(&http.Server{
		Addr:         app.Config.Listen,
		Handler:      handler,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: app.Config.Timeouts.Global * 2, // It has to be greater than Timeout.Global because we use that value as per-request context timeout
	}, metricsServer)

	if err != nil {
		log.Fatal("error during gracehttp.Serve()", zap.Error(err))
	}
}

// InitBackends inits backends.
// TODO: Move to where the main func is.
func InitBackends(config cfg.Zipper, logger *zap.Logger) ([]backend.Backend, error) {
	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		IdleConnTimeout:     3 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   config.Timeouts.Connect,
			KeepAlive: config.KeepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	configBackendList := config.GetBackends()
	backends := make([]backend.Backend, 0, len(configBackendList))
	for _, host := range configBackendList {
		if host.Http == "" {
			return nil, fmt.Errorf("backend without http address was provided: %+v", host)
		}
		dc, cluster, _ := config.InfoOfBackend(host.Http)
		var b backend.Backend
		var err error
		bConf := bnet.Config{
			Address:            host.Http,
			DC:                 dc,
			Cluster:            cluster,
			Client:             client,
			Timeout:            config.Timeouts.AfterStarted,
			Limit:              config.ConcurrencyLimitPerServer,
			PathCacheExpirySec: uint32(config.ExpireDelaySec),
			Logger:             logger,
		}
		if host.Grpc != "" {
			b, err = bnet.NewGrpc(bnet.GrpcConfig{
				Config:      bConf,
				GrpcAddress: host.Grpc,
			})
		} else {
			b, err = bnet.New(bConf)
		}

		if err != nil {
			return backends, fmt.Errorf("Couldn't create backend for '%s'", host)
		}

		backends = append(backends, b)
	}

	return backends, nil
}
