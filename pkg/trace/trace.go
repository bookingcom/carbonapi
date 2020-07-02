/*
  Package trace defined functions that we use for collecting traces.
*/

package trace

import (
	"log"
	"net/http"
	"os"

	"github.com/motemen/go-loghttp"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/propagation"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// InitTracer creates a new trace provider instance and registers it as global trace provider.
func InitTracer(serviceName string, logger *zap.Logger) func() {
	// TODO timeouts. Default value is 60s!!!
	client := &http.Client{
		Transport: &loghttp.Transport{
			Transport: &http.Transport{
				Proxy: nil,
			},
		},
	}

	// Create and install Jaeger export pipeline
	endpoint := os.Getenv("JAEGER_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:14268/api/traces"
	}

	_, flush, err := jaeger.NewExportPipeline(
		jaeger.WithCollectorEndpoint(endpoint, jaeger.WithHTTPClient((client))),
		jaeger.WithProcess(jaeger.Process{
			ServiceName: serviceName,
			Tags: []kv.KeyValue{
				kv.String("exporter", "jaeger"),
			},
		}),
		jaeger.RegisterAsGlobal(),
		jaeger.WithSDK(&sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
	)
	if err != nil {
		log.Fatal(err)
	}

	propagator := trace.B3{SingleHeader: false}
	oldProps := global.Propagators()
	props := propagation.New(
		propagation.WithExtractors(propagator),
		propagation.WithExtractors(oldProps.HTTPExtractors()...),
		propagation.WithInjectors(oldProps.HTTPInjectors()...),
	)
	global.SetPropagators(props)

	return func() {
		flush()
	}
}
