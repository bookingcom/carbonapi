package net

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	wire "github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	capi "github.com/go-graphite/protocol/carbonapi_v2_grpc"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const routingPath = "flights.slo"

type routingStore struct {
	ready   atomic.Bool
	renders atomic.Int64
	finds   atomic.Int64
	metric  types.Metric
}

func (s *routingStore) render() ([]types.Metric, error) {
	s.renders.Add(1)
	if !s.ready.Load() {
		return nil, types.ErrMetricsNotFound
	}
	m := s.metric
	m.Values = append([]float64(nil), m.Values...)
	m.IsAbsent = append([]bool(nil), m.IsAbsent...)
	return []types.Metric{m}, nil
}

func (s *routingStore) find() (types.Matches, error) {
	s.finds.Add(1)
	if !s.ready.Load() {
		return types.Matches{}, types.ErrMatchesNotFound
	}
	return types.Matches{Name: routingPath, Matches: []types.Match{{Path: routingPath, IsLeaf: true}}}, nil
}

// The client double exercises the real GrpcBackend decoders and cache writes
// without a listener, timeout or wall-clock dependency.
type routingGRPCClient struct {
	capi.CarbonV2Client
	store *routingStore
}

type routingGRPCStream struct {
	grpc.ClientStream
	metrics []types.Metric
}

func (c routingGRPCClient) Render(context.Context, *pb.MultiFetchRequest, ...grpc.CallOption) (capi.CarbonV2_RenderClient, error) {
	ms, err := c.store.render()
	if err != nil {
		return nil, err
	}
	return &routingGRPCStream{metrics: ms}, nil
}

func (s *routingGRPCStream) Recv() (*pb.FetchResponse, error) {
	if len(s.metrics) == 0 {
		return nil, io.EOF
	}
	m := s.metrics[0]
	s.metrics = s.metrics[1:]
	return &pb.FetchResponse{Name: m.Name, StartTime: m.StartTime, StopTime: m.StopTime, StepTime: m.StepTime, Values: m.Values, IsAbsent: m.IsAbsent}, nil
}

func (c routingGRPCClient) Find(context.Context, *pb.GlobRequest, ...grpc.CallOption) (*pb.GlobResponse, error) {
	ms, err := c.store.find()
	if err != nil {
		return nil, err
	}
	return &pb.GlobResponse{Name: ms.Name, Matches: []*pb.GlobMatch{{Path: routingPath, IsLeaf: true}}}, nil
}

func routingBackend(t *testing.T, mode string, clk *fakeClock, expiry time.Duration, s *routingStore) backend.Backend {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		var err error
		if r.URL.Path == "/metrics/find/" {
			var matches types.Matches
			matches, err = s.find()
			if err == nil {
				body, err = wire.FindEncoder(matches)
			}
		} else {
			var metrics []types.Metric
			metrics, err = s.render()
			if err == nil {
				body, err = wire.RenderEncoder(metrics)
			}
		}
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	b, err := New(Config{Address: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	b.cache = newPathCacheWithClock(expiry, clk.now)
	b.streamRender = mode != "buffered"
	b.packRender = mode == "packed"
	if mode == "grpc" {
		return backend.Backend{BackendImpl: &GrpcBackend{NetBackend: b, carbonV2Client: routingGRPCClient{store: s}}}
	}
	return backend.Backend{BackendImpl: b}
}

func routingRender(t *testing.T, bs []backend.Backend, packed bool) []types.Metric {
	t.Helper()
	request := types.NewRenderRequest([]string{routingPath}, 1, 4)
	selected, filtered := backend.Filter(bs, request.Targets)
	request.PathCacheFiltered = filtered
	var replies [][]types.Metric
	for _, b := range selected {
		var ms []types.Metric
		var err error
		if packed {
			ms, err = b.BackendImpl.(*NetBackend).RenderPacked(context.Background(), request)
		} else {
			ms, err = b.Render(context.Background(), request)
		}
		if err == nil {
			replies = append(replies, ms)
		} else if !errors.Is(err, types.ErrMetricsNotFound) {
			t.Fatal(err)
		}
	}
	ms, stats := types.MergeMetrics(replies, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	if stats.MismatchCount != 0 {
		t.Fatal("complementary donor/survivor points must not mismatch")
	}
	return ms
}

func routingFind(t *testing.T, bs []backend.Backend) {
	t.Helper()
	request := types.NewFindRequest(routingPath)
	selected, filtered := backend.Filter(bs, []string{routingPath})
	request.PathCacheFiltered = filtered
	for _, b := range selected {
		if _, err := b.Find(context.Background(), request); err != nil && !errors.Is(err, types.ErrMatchesNotFound) {
			t.Fatal(err)
		}
	}
}

func TestPathCacheHotRoutingRediscovery(t *testing.T) {
	for _, mode := range []string{"buffered", "stream", "packed", "grpc"} {
		for _, traffic := range []string{"render", "find", "mixed"} {
			t.Run(mode+"/"+traffic, func(t *testing.T) {
				clk := &fakeClock{t: time.Unix(1000, 0)}
				const expiry = 120 * time.Second // expireDelaySec in the incident
				donor := &routingStore{metric: types.Metric{Name: routingPath, StartTime: 1, StopTime: 4, StepTime: 1, Values: []float64{11, 0, 0}, IsAbsent: []bool{false, true, true}}}
				donor.ready.Store(true)
				survivor := &routingStore{metric: types.Metric{Name: routingPath, StartTime: 1, StopTime: 4, StepTime: 1, Values: []float64{0, 22, 0}, IsAbsent: []bool{true, false, true}}}
				bs := []backend.Backend{routingBackend(t, mode, clk, expiry, donor), routingBackend(t, mode, clk, expiry, survivor)}
				// A full discovery runs while B's index still lacks the name.
				routingFind(t, bs)
				if !bs[0].Contains([]string{routingPath}) || bs[1].Contains([]string{routingPath}) {
					t.Fatal("initial discovery did not cache only the donor")
				}
				survivor.ready.Store(true)
				for elapsed := 5 * time.Second; elapsed <= expiry; elapsed += 5 * time.Second {
					clk.advance(5 * time.Second)
					if traffic == "find" || traffic == "mixed" {
						routingFind(t, bs)
					}
					if traffic != "find" {
						ms := routingRender(t, bs, mode == "packed")
						if elapsed < expiry && (len(ms) != 1 || !ms[0].IsAbsent[1]) {
							t.Fatal("fixture must route hot reads only to the donor before expiry")
						}
					}
				}
				ms := routingRender(t, bs, mode == "packed")
				if len(ms) != 1 || !reflect.DeepEqual(ms[0].Values, []float64{11, 22, 0}) || !reflect.DeepEqual(ms[0].IsAbsent, []bool{false, false, true}) {
					t.Fatalf("survivor not rediscovered by %s; merged history/recent/absent points: %+v", expiry, ms)
				}
				if survivor.renders.Load() == 0 {
					t.Fatal("survivor was never rendered")
				}
			})
		}
	}
}

// Both replicas hold the series. While one is unreachable for longer than the
// expiry (a store restart or read block), hot reads renew only the other. The
// returning replica must rejoin within one more expiry; otherwise the other
// replica stays the only route and its next miss drops the series.
func TestPathCacheReplicaRejoinsAfterOutage(t *testing.T) {
	for _, mode := range []string{"buffered", "stream", "packed", "grpc"} {
		t.Run(mode, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(1000, 0)}
			const expiry = 120 * time.Second
			const interval = 5 * time.Second
			m := types.Metric{Name: routingPath, StartTime: 1, StopTime: 4, StepTime: 1, Values: []float64{1, 2, 3}, IsAbsent: []bool{false, false, false}}
			a, b := &routingStore{metric: m}, &routingStore{metric: m}
			a.ready.Store(true)
			b.ready.Store(true)
			bs := []backend.Backend{routingBackend(t, mode, clk, expiry, a), routingBackend(t, mode, clk, expiry, b)}
			packed := mode == "packed"
			hot := func(d time.Duration) {
				for elapsed := interval; elapsed <= d; elapsed += interval {
					clk.advance(interval)
					if ms := routingRender(t, bs, packed); len(ms) != 1 {
						t.Fatalf("series missing while a replica was serving: %+v", ms)
					}
				}
			}
			routingRender(t, bs, packed) // discovery caches both replicas
			a.ready.Store(false)
			hot(expiry + interval)
			a.ready.Store(true)
			hot(expiry + interval)
			if !bs[0].Contains([]string{routingPath}) {
				t.Fatal("replica not rediscovered within one expiry after its outage ended")
			}
			b.ready.Store(false)
			if ms := routingRender(t, bs, packed); len(ms) != 1 {
				t.Fatalf("series dropped when the other replica missed one read: %+v", ms)
			}
		})
	}
}

func TestPathCacheSteadyStateFanout(t *testing.T) {
	for _, expiry := range []time.Duration{120 * time.Second, 300 * time.Second} {
		for _, replicas := range []int{1, 3} {
			t.Run(fmt.Sprintf("ttl=%s/replicas=%d", expiry, replicas), func(t *testing.T) {
				const stores = 8
				const interval = 5 * time.Second
				clk := &fakeClock{t: time.Unix(1000, 0)}
				var bs []backend.Backend
				var fixtures []*routingStore
				for i := 0; i < stores; i++ {
					s := &routingStore{metric: types.Metric{Name: routingPath, StartTime: 1, StopTime: 2, StepTime: 1, Values: []float64{42}, IsAbsent: []bool{false}}}
					s.ready.Store(i < replicas)
					fixtures = append(fixtures, s)
					bs = append(bs, routingBackend(t, "stream", clk, expiry, s))
				}
				routingRender(t, bs, false) // exclude the common cold-start fan-out
				for _, s := range fixtures {
					s.renders.Store(0)
				}
				renders := int64(2 * expiry / interval)
				for range renders {
					clk.advance(interval)
					routingRender(t, bs, false)
				}
				var requests int64
				for _, s := range fixtures {
					requests += s.renders.Load()
				}
				want := renders*int64(replicas) + 2*int64(stores-replicas)
				if requests != want {
					t.Fatalf("got %d requests for %d renders, want %d", requests, renders, want)
				}
				t.Logf("%d requests / %d renders = %.6f requests/render; sliding-cache baseline %d; all-backend fan-out %d", requests, renders, float64(requests)/float64(renders), replicas, stores)
			})
		}
	}
}
