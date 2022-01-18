package backend

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	consistencyChecks := []bool{false, true}
	for _, consistencyCheck := range consistencyChecks {
		cc := consistencyCheck
		b.Run(fmt.Sprintf("BenchmarkRenders/ConsistencyCheck%s", strconv.FormatBool(cc)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Renders(ctx, backends, types.NewRenderRequest(nil, 0, 0), cc)
			}
		})
	}
}

func BenchmarkRendersStorm(b *testing.B) {
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

	backends := make([]Backend, 0)
	for i := 0; i < 3; i++ {
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

	consistencyChecks := []bool{false, true}
	for _, consistencyCheck := range consistencyChecks {
		cc := consistencyCheck
		b.Run(fmt.Sprintf("BenchmarkRendersStorm/ConsistencyCheck%s", strconv.FormatBool(cc)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _, _, err := Renders(ctx, backends, types.NewRenderRequest(nil, 0, 0), cc)
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
