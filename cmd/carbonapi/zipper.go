package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/util"
	realZipper "github.com/bookingcom/carbonapi/zipper"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"

	"go.uber.org/zap"
)

var errNoMetrics = errors.New("no metrics")

type zipper struct {
	z *realZipper.Zipper

	logger      *zap.Logger
	statsSender func(*realZipper.Stats)
}

// The CarbonZipper interface exposes access to realZipper
// Exposes the functionality to find, get info or render metrics.
type CarbonZipper interface {
	Find(ctx context.Context, metric string) (pb.GlobResponse, error)
	Info(ctx context.Context, metric string) (map[string]pb.InfoResponse, error)
	Render(ctx context.Context, metric string, from, until int32) ([]*types.MetricData, error)
}

func newZipper(sender func(*realZipper.Stats), config cfg.Zipper, logger *zap.Logger) *zipper {
	z := &zipper{
		z:           realZipper.NewZipper(sender, config, logger),
		logger:      logger,
		statsSender: sender,
	}

	return z
}

func (z zipper) Find(ctx context.Context, metric string) (pb.GlobResponse, error) {
	var pbresp pb.GlobResponse
	logger := z.logger.With(zap.String("carbonapi_uuid", util.GetUUID(ctx, util.API)))
	res, stats, err := z.z.Find(ctx, logger, metric)
	if err != nil {
		return pbresp, err
	}

	pbresp.Name = metric
	pbresp.Matches = res

	z.statsSender(stats)

	return pbresp, err
}

func (z zipper) Info(ctx context.Context, metric string) (map[string]pb.InfoResponse, error) {
	logger := z.logger.With(zap.String("carbonapi_uuid", util.GetUUID(ctx, util.API)))
	resp, stats, err := z.z.Info(ctx, logger, metric)
	if err != nil {
		return nil, fmt.Errorf("http.Get: %+v", err)
	}

	z.statsSender(stats)

	return resp, nil
}

func (z zipper) Render(ctx context.Context, metric string, from, until int32) ([]*types.MetricData, error) {
	var result []*types.MetricData

	logger := z.logger.With(zap.String("carbonapi_uuid", util.GetUUID(ctx, util.API)))
	pbresp, stats, err := z.z.Render(ctx, logger, metric, from, until)
	if err != nil {
		return result, err
	}

	z.statsSender(stats)

	if m := pbresp.Metrics; len(m) == 0 {
		return result, errNoMetrics
	}

	for i := range pbresp.Metrics {
		result = append(result, &types.MetricData{FetchResponse: pbresp.Metrics[i]})
	}

	return result, nil
}
