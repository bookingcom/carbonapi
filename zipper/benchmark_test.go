package zipper

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pathcache"

	"github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

func BenchmarkRender(b *testing.B) {
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

	backends := make([]string, 0)
	for i := 0; i < 10; i++ {
		backends = append(backends, server.URL)
	}

	zipper := &Zipper{
		storageClient: client,
		backends:      backends,
		pathCache:     pathcache.NewPathCache(60),
		logger:        zap.New(nil),
	}

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_, _, err := zipper.Render(ctx, zipper.logger, "", 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
