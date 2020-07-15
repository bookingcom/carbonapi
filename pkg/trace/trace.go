/*
  Package trace defined functions that we use for collecting traces.
*/

package trace

import (
	"github.com/bookingcom/carbonapi/cfg"

	"log"
	"net/http"
	"os"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/propagation"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// InitTracer creates a new trace provider instance and registers it as global trace provider.
func InitTracer(serviceName string, logger *zap.Logger, config cfg.Traces) func() {

	endpoint := os.Getenv("JAEGER_ENDPOINT")
	if endpoint == "" {
		endpoint = config.JaegerEndpoint
	}
	logger.Info("Traces", zap.String("jaegerEndpoint", endpoint))
	if endpoint == "" {
		// create and register NoopTracer
		provider := trace.NoopProvider{}
		global.SetTraceProvider(provider)
		return func() {} // Nothing to flush
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: nil,
		},
		Timeout: config.Timeout,
	}

	fqdn, _ := os.Hostname()
	// Create and install Jaeger export pipeline
	_, flush, err := jaeger.NewExportPipeline(
		jaeger.WithCollectorEndpoint(endpoint, jaeger.WithHTTPClient((client))),
		jaeger.WithProcess(jaeger.Process{
			ServiceName: serviceName,
			Tags: []kv.KeyValue{
				kv.String("exporter", "jaeger"),
				kv.String("host.hostname", fqdn),
			},
		}),
		jaeger.RegisterAsGlobal(),
		jaeger.WithSDK(&sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
	)
	if err != nil {
		log.Fatal(err)
	}

	propagator := trace.B3{}
	// Grafana propagates traces over b3 headers
	oldProps := global.Propagators()
	props := propagation.New(
		propagation.WithExtractors(propagator),
		propagation.WithExtractors(oldProps.HTTPExtractors()...),
		propagation.WithInjectors(oldProps.HTTPInjectors()...),
	)
	global.SetPropagators(props)

	return flush
}
