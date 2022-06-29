package grpc

import (
	"github.com/bookingcom/carbonapi/pkg/backend/net"
)

// Backend represents a host that accepts requests for metrics over gRPC.
type Backend struct {
	net.Backend
}

//func (b Backend) Render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
//
//}
