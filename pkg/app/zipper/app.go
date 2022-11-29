package zipper

import (
	"fmt"
	"net"
	"net/http"
	"time"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"

	"github.com/dgryski/go-expirecache"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/cfg"

	"go.uber.org/zap"
)

// App represents the main zipper runnable
// TODO: Remove after merge.
type App struct {
	Config              cfg.Zipper
	Backends            []backend.Backend
	TopLevelDomainCache *expirecache.Cache
	TLDPrefixes         []tldPrefix

	Metrics *PrometheusMetrics
	Lg      *zap.Logger
}

// InitBackends inits backends.
func InitBackends(config cfg.Zipper, ms *PrometheusMetrics, logger *zap.Logger) ([]backend.Backend, error) {
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
			PathCacheExpirySec: uint32(config.ExpireDelaySec),
			QHist:              ms.TimeInQueueSeconds,
			Responses:          ms.BackendResponses,
			Logger:             logger,
		}
		var be backend.BackendImpl
		if host.Grpc != "" {
			be, err = bnet.NewGrpc(bnet.GrpcConfig{
				Config:      bConf,
				GrpcAddress: host.Grpc,
			})
		} else {
			be, err = bnet.New(bConf)
		}
		b = backend.NewBackend(be,
			config.BackendQueueSize,
			config.ConcurrencyLimitPerServer,
			ms.BackendRequestsInQueue,
			ms.BackendSemaphoreSaturation,
			ms.BackendTimeInQSec,
			ms.BackendEnqueuedRequests)

		if err != nil {
			return backends, fmt.Errorf("Couldn't create backend for '%s'", host)
		}

		backends = append(backends, b)
	}

	return backends, nil
}
