package backend

import (
	"context"
	"fmt"
	"github.com/bookingcom/carbonapi/cfg"
	"go.uber.org/zap"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/types"

	"github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func BenchmarkRenders(b *testing.B) {
	secPerHour := int(12 * time.Hour / time.Second)

	metrics := carbonapi_v2_pb.MultiFetchResponse{
		Metrics: []carbonapi_v2_pb.FetchResponse{
			carbonapi_v2_pb.FetchResponse{
				Name:     "foo",
				Values:   make([]float64, secPerHour),
				IsAbsent: make([]bool, secPerHour),
			},
		},
	}
	blob, err := metrics.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(blob)
	}))
	defer server.Close()

	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: 100,
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: time.Second,
			DualStack: true,
		}).DialContext,
	}

	bk, err := bnet.New(bnet.Config{
		Address: server.URL,
		Client:  client,
	})
	if err != nil {
		b.Fatal(err)
	}

	backends := make([]Backend, 0)
	for i := 0; i < 3; i++ {
		backends = append(backends, bk)
	}

	ctx := context.Background()
	renderReplicaMismatchConfigs := []cfg.RenderReplicaMismatchConfig{
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeNormal,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeCheck,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: true,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeCheck,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: true,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
			RenderReplicaMismatchReportLimit:      0,
		},
	}
	logger, _ := zap.NewDevelopment()
	for _, replicaMatchMode := range renderReplicaMismatchConfigs {
		cc := replicaMatchMode
		b.Run(fmt.Sprintf("BenchmarkRenders/ReplicaMatchMode-%s", cc.String()), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Renders(ctx, backends, types.NewRenderRequest(nil, 0, 0), cc, logger)
			}
		})
	}
}

func createRegularMismatches(backendNumber int, metrics []carbonapi_v2_pb.FetchResponse) []carbonapi_v2_pb.FetchResponse {
	MismatchFreq := 30000
	for mIndex := range metrics {
		mismatchCount := (len(metrics[mIndex].Values) - backendNumber) / MismatchFreq
		for i := 0; i < mismatchCount; i++ {
			vIndex := backendNumber + (i * MismatchFreq)
			metrics[mIndex].Values[vIndex] += 1.0
		}
	}
	return metrics
}

func createFullMismatches(backendNumber int, metrics []carbonapi_v2_pb.FetchResponse) []carbonapi_v2_pb.FetchResponse {
	for mIndex := range metrics {
		for i := 0; i < len(metrics[mIndex].Values); i++ {
			metrics[mIndex].Values[i] += float64(backendNumber)
		}
	}
	return metrics
}

func createSingleBackendMetrics() []carbonapi_v2_pb.FetchResponse {
	metricsCount := 600
	metrics := make([]carbonapi_v2_pb.FetchResponse, metricsCount)
	dpCount := 10800
	for i := 0; i < 600; i++ {
		metrics[i] = carbonapi_v2_pb.FetchResponse{
			Name:     fmt.Sprintf("metric.foo%d", i),
			Values:   make([]float64, dpCount),
			IsAbsent: make([]bool, dpCount),
		}
	}
	return metrics
}

func BenchmarkRendersStorm(b *testing.B) {
	metricsByBackend := []carbonapi_v2_pb.MultiFetchResponse{
		{
			Metrics: createRegularMismatches(1, createSingleBackendMetrics()),
		},
		{
			Metrics: createRegularMismatches(2, createSingleBackendMetrics()),
		},
		{
			Metrics: createRegularMismatches(3, createSingleBackendMetrics()),
		},
	}

	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: 100,
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: time.Second,
			DualStack: true,
		}).DialContext,
	}

	backends := make([]Backend, 0)
	for i := 0; i < len(metricsByBackend); i++ {
		blob, err := metricsByBackend[i].Marshal()
		if err != nil {
			b.Fatal(err)
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.Write(blob)
		}))
		defer server.Close()
		bk, err := bnet.New(bnet.Config{
			Address: server.URL,
			Client:  client,
			Limit:   1,
		})
		if err != nil {
			b.Fatal(err)
		}

		backends = append(backends, bk)
	}

	ctx := context.Background()
	wg := sync.WaitGroup{}
	n := 50
	errs := make(chan []error, n)

	renderReplicaMismatchConfigs := []cfg.RenderReplicaMismatchConfig{
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeNormal,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeCheck,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: true,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeCheck,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: true,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
			RenderReplicaMismatchReportLimit:      0,
		},
	}
	logger, _ := zap.NewDevelopment()
	for _, replicaMatchMode := range renderReplicaMismatchConfigs {
		cc := replicaMatchMode
		b.Run(fmt.Sprintf("ReplicaMatchMode-%s", cc.String()), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _, err := Renders(ctx, backends, types.NewRenderRequest(nil, 0, 0), cc, logger)
						errs <- err
					}()
				}

				wg.Wait()
				for j := 0; j < n; j++ {
					err := <-errs
					if len(err) != 0 {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkRendersMismatchStorm(b *testing.B) {
	metricsByBackend := []carbonapi_v2_pb.MultiFetchResponse{
		{
			Metrics: createFullMismatches(1, createSingleBackendMetrics()),
		},
		{
			Metrics: createFullMismatches(2, createSingleBackendMetrics()),
		},
		{
			Metrics: createFullMismatches(3, createSingleBackendMetrics()),
		},
	}

	client := &http.Client{}
	client.Transport = &http.Transport{
		MaxIdleConnsPerHost: 100,
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: time.Second,
			DualStack: true,
		}).DialContext,
	}

	backends := make([]Backend, 0)
	for i := 0; i < len(metricsByBackend); i++ {
		blob, err := metricsByBackend[i].Marshal()
		if err != nil {
			b.Fatal(err)
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.Write(blob)
		}))
		defer server.Close()
		bk, err := bnet.New(bnet.Config{
			Address: server.URL,
			Client:  client,
			Limit:   1,
		})
		if err != nil {
			b.Fatal(err)
		}

		backends = append(backends, bk)
	}

	ctx := context.Background()
	wg := sync.WaitGroup{}
	n := 50
	errs := make(chan []error, n)

	renderReplicaMismatchConfigs := []cfg.RenderReplicaMismatchConfig{
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeNormal,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeCheck,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: false,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: true,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeCheck,
			RenderReplicaMismatchReportLimit:      0,
		},
		{
			RenderReplicaMismatchApproximateCheck: true,
			RenderReplicaMatchMode:                cfg.ReplicaMatchModeMajority,
			RenderReplicaMismatchReportLimit:      0,
		},
	}
	logger, _ := zap.NewDevelopment()
	for _, replicaMatchMode := range renderReplicaMismatchConfigs {
		cc := replicaMatchMode
		b.Run(fmt.Sprintf("ReplicaMatchMode-%s", cc.String()), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _, err := Renders(ctx, backends, types.NewRenderRequest(nil, 0, 0), cc, logger)
						errs <- err
					}()
				}

				wg.Wait()
				for j := 0; j < n; j++ {
					err := <-errs
					if len(err) != 0 {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
