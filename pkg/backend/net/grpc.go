package net

import (
	"context"
	"io"
	"strconv"
	"time"

	capi_v2_grpc "github.com/go-graphite/protocol/carbonapi_v2_grpc"
	"github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/util"
)

// GrpcBackend represents a host that accepts requests for metrics over gRPC and HTTP.
// This struct overrides Backend interface functions to use gRPC.
type GrpcBackend struct {
	*Backend
	GrpcAddress    string
	carbonV2Client capi_v2_grpc.CarbonV2Client
	maxRecvMsgSize int
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
	conn, err := grpc.Dial(cfg.GrpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		return nil, err
	}
	c := capi_v2_grpc.NewCarbonV2Client(conn)

	return &GrpcBackend{
		Backend:        b,
		GrpcAddress:    cfg.GrpcAddress,
		carbonV2Client: c,
		maxRecvMsgSize: 100 * 1024 * 1024, // TODO: Make configurable
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
	ctx = util.MarshalGrpcCtx(ctx)
	multiFetchRequest := makeMultiFetchRequestFromRenderRequest(request)
	request.Trace.AddMarshal(t0)

	ctx, cancel := gb.setTimeout(ctx)
	defer cancel()

	t1 := time.Now()
	err := gb.enter(ctx)
	request.Trace.AddLimiter(t1)
	if err != nil {
		return nil, err
	}
	defer func() {
		if limiterErr := gb.leave(); limiterErr != nil {
			gb.logger.Error("Backend limiter full",
				zap.String("host", gb.GrpcAddress),
				zap.String("uuid", util.GetUUID(ctx)),
				zap.Error(limiterErr),
			)
		}
	}()

	stream, err := gb.carbonV2Client.Render(ctx, multiFetchRequest, grpc.MaxCallRecvMsgSize(gb.maxRecvMsgSize))
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
			gb.countResponse(err, "render")
			if code := status.Code(err); code == codes.NotFound {
				return nil, types.ErrMetricsNotFound
			}
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
	gb.countResponse(nil, "render")

	for _, metric := range fetchedMetrics {
		gb.cache.Set(metric.Name, struct{}{}, 0, gb.cacheExpirySec)
	}

	return fetchedMetrics, nil
}

// Find resolves globs and finds metrics in a backend.
func (gb *GrpcBackend) Find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	t0 := time.Now()
	ctx = util.MarshalGrpcCtx(ctx)
	globRequest := &carbonapi_v2_pb.GlobRequest{
		Query: request.Query,
	}
	request.Trace.AddMarshal(t0)

	ctx, cancel := gb.setTimeout(ctx)
	defer cancel()

	t1 := time.Now()
	err := gb.enter(ctx)
	request.Trace.AddLimiter(t1)
	if err != nil {
		return types.Matches{}, err
	}
	defer func() {
		if limiterErr := gb.leave(); limiterErr != nil {
			gb.logger.Error("Backend limiter full",
				zap.String("host", gb.GrpcAddress),
				zap.String("uuid", util.GetUUID(ctx)),
				zap.Error(limiterErr),
			)
		}
	}()

	globResponse, err := gb.carbonV2Client.Find(ctx, globRequest, grpc.MaxCallRecvMsgSize(gb.maxRecvMsgSize))
	gb.countResponse(err, "find")
	if err != nil {
		if code := status.Code(err); code == codes.NotFound {
			return types.Matches{}, types.ErrMatchesNotFound
		}
		return types.Matches{}, err
	}

	matches := types.Matches{
		Name: globResponse.Name,
	}
	for _, g := range globResponse.Matches {
		matches.Matches = append(matches.Matches, types.Match{
			Path:   g.Path,
			IsLeaf: g.IsLeaf,
		})
	}

	if len(matches.Matches) == 0 {
		return matches, types.ErrMatchesNotFound
	}

	for _, match := range matches.Matches {
		if match.IsLeaf {
			gb.cache.Set(match.Path, struct{}{}, 0, gb.cacheExpirySec)
		}
	}

	return matches, nil
}

func (gb *GrpcBackend) Info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	t0 := time.Now()
	ctx = util.MarshalGrpcCtx(ctx)
	infoRequest := &carbonapi_v2_pb.InfoRequest{
		Name: request.Target,
	}
	request.Trace.AddMarshal(t0)

	ctx, cancel := gb.setTimeout(ctx)
	defer cancel()

	t1 := time.Now()
	err := gb.enter(ctx)
	request.Trace.AddLimiter(t1)
	if err != nil {
		return nil, err
	}
	defer func() {
		if limiterErr := gb.leave(); limiterErr != nil {
			gb.logger.Error("Backend limiter full",
				zap.String("host", gb.GrpcAddress),
				zap.String("uuid", util.GetUUID(ctx)),
				zap.Error(limiterErr),
			)
		}
	}()

	resp, err := gb.carbonV2Client.Info(ctx, infoRequest, grpc.MaxCallRecvMsgSize(gb.maxRecvMsgSize))
	gb.countResponse(err, "info")
	if err != nil {
		if code := status.Code(err); code == codes.NotFound {
			return nil, types.ErrInfoNotFound
		}
		return nil, err
	}

	var rets []types.Retention
	for _, r := range resp.Retentions {
		rets = append(rets, types.Retention{
			SecondsPerPoint: r.SecondsPerPoint,
			NumberOfPoints:  r.NumberOfPoints,
		})
	}
	return []types.Info{
		{
			Host:              gb.GrpcAddress,
			Name:              resp.Name,
			AggregationMethod: resp.AggregationMethod,
			MaxRetention:      resp.MaxRetention,
			XFilesFactor:      resp.XFilesFactor,
			Retentions:        rets,
		},
	}, nil
}

func (gb *GrpcBackend) countResponse(err error, request string) {
	if gb.responses == nil {
		return
	}
	if err == nil {
		gb.responses.WithLabelValues(strconv.Itoa(int(codes.OK)), request).Inc()
	} else {
		code := status.Code(err)
		if code == codes.Unknown {
			return
		}
		gb.responses.WithLabelValues(strconv.Itoa(int(code)), request).Inc()
	}
}
