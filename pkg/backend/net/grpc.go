package net

import (
	"context"
	"github.com/bookingcom/carbonapi/pkg/types"
	capi_v2_grpc "github.com/go-graphite/protocol/carbonapi_v2_grpc"
	"github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"time"
)

// GrpcBackend represents a host that accepts requests for metrics over gRPC and HTTP.
// This struct overrides Backend interface functions to use gRPC.
type GrpcBackend struct {
	*Backend
	carbonV2Client capi_v2_grpc.CarbonV2Client
}

type GrpcConfig struct {
	Config
	GrpcAddress string
}

// NewGrpc creates a new gRPC backend from the given configuration.
// This backend will fall back to normal backend when the gRPC backend function is not declared.
func NewGrpc(cfg GrpcConfig) (*GrpcBackend, error) {
	b, err := New(cfg.Config)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(cfg.GrpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := capi_v2_grpc.NewCarbonV2Client(conn)

	return &GrpcBackend{
		Backend:        b,
		carbonV2Client: c,
	}, nil
}

func makeMultiFetchRequestFromRenderRequest(request types.RenderRequest) *carbonapi_v2_pb.MultiFetchRequest {
	frs := make([]*carbonapi_v2_pb.FetchRequest, 0, len(request.Targets))
	for _, m := range request.Targets {
		frs = append(frs, &carbonapi_v2_pb.FetchRequest{
			Name:      m,
			StartTime: request.From,
			StopTime:  request.Until,
		})
	}
	return &carbonapi_v2_pb.MultiFetchRequest{
		Metrics: frs,
	}
}

// Render fetches raw metrics from a backend.
func (gb *GrpcBackend) Render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	t0 := time.Now()

	multiFetchRequest := makeMultiFetchRequestFromRenderRequest(request)
	request.Trace.AddMarshal(t0)

	ctx, cancel := gb.setTimeout(ctx)
	defer cancel()
	stream, err := gb.carbonV2Client.Render(ctx, multiFetchRequest)
	if err != nil {
		return nil, err
	}

	t1 := time.Now()
	err = gb.enter(ctx)
	request.Trace.AddLimiter(t1)
	if err != nil {
		return nil, err
	}

	var fetchedMetrics []types.Metric
	for {
		fetchResponse, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		fetchedMetrics = append(fetchedMetrics, types.Metric{
			Name:      fetchResponse.Name,
			StartTime: fetchResponse.StartTime,
			StopTime:  fetchResponse.StopTime,
			StepTime:  fetchResponse.StepTime,
			Values:    fetchResponse.Values,
			IsAbsent:  fetchResponse.IsAbsent,
		})
	}

	for _, metric := range fetchedMetrics {
		gb.cache.Set(metric.Name, struct{}{}, 0, gb.cacheExpirySec)
	}

	return fetchedMetrics, nil
}
