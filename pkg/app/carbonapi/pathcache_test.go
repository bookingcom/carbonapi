package carbonapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/backend"
	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

type routingRequestRecorder struct {
	backend.BackendImpl
	t      *testing.T
	cached bool
	mu     sync.Mutex
	calls  int
}

func (b *routingRequestRecorder) Contains(paths []string) bool {
	return b.cached && strings.HasPrefix(paths[0], "cached.")
}

func (b *routingRequestRecorder) SupportsTargetCoalescing() bool { return true }
func (b *routingRequestRecorder) Logger() *zap.Logger            { return zap.NewNop() }
func (b *routingRequestRecorder) GetServerAddress() string {
	if b.cached {
		return "donor"
	}
	return "survivor"
}
func (b *routingRequestRecorder) BackendInfo() (string, string, string) {
	return b.GetServerAddress(), "fixture", "local"
}

func (b *routingRequestRecorder) check(path string, filtered bool) {
	b.t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if want := strings.HasPrefix(path, "cached."); filtered != want {
		b.t.Errorf("path %s on %s: PathCacheFiltered=%v, want %v", path, b.GetServerAddress(), filtered, want)
	}
}

func (b *routingRequestRecorder) Render(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
	var ms []types.Metric
	for _, path := range r.Targets {
		b.check(path, r.PathCacheFiltered)
		ms = append(ms, types.Metric{Name: path, StartTime: 1, StopTime: 2, StepTime: 1, Values: []float64{42}, IsAbsent: []bool{false}})
	}
	return ms, nil
}

func (b *routingRequestRecorder) Find(_ context.Context, r types.FindRequest) (types.Matches, error) {
	b.check(r.Query, r.PathCacheFiltered)
	return types.Matches{Name: r.Query, Matches: []types.Match{{Path: r.Query, IsLeaf: true}}}, nil
}

func TestUpstreamRoutingCacheMetadata(t *testing.T) {
	for _, operation := range []string{"render", "grouped", "find", "expand"} {
		t.Run(operation, func(t *testing.T) {
			config := cfg.DefaultAPIConfig()
			z := cfg.DefaultZipperConfig()
			app := &App{config: config, ZipperConfig: z, ms: newPrometheusMetrics(config), findCache: cache.NullCache{}, TopLevelDomainCache: expirecache.New(0), NotFoundWhenTLDCacheMiss: true}
			app.ZipperMetrics = NewZipperPrometheusMetrics(z)
			recorders := []*routingRequestRecorder{{t: t, cached: true}, {t: t}}
			for _, recorder := range recorders {
				app.Backends = append(app.Backends, backend.NewBackend(recorder, 32, 16,
					app.ZipperMetrics.BackendRequestsInQueue, app.ZipperMetrics.BackendSemaphoreSaturation,
					app.ZipperMetrics.BackendTimeInQSec, app.ZipperMetrics.BackendEnqueuedRequests,
					app.ms.BackendDuration.MustCurryWith(map[string]string{"dc": "local", "cluster": "fixture"})))
			}
			app.TopLevelDomainCache.Set("tlds", map[string][]*backend.Backend{
				"cached": {&app.Backends[0], &app.Backends[1]},
				"cold":   {&app.Backends[0], &app.Backends[1]},
			}, 0, 600)
			paths := []string{"cached.1", "cached.2", "cold.1", "cold.2"}
			if operation == "grouped" {
				for _, reply := range sendGroupedRenderRequests(app, context.Background(), paths, 1, 2, &carbonapipb.AccessLogDetails{}, zap.NewNop()) {
					if reply.error != nil || len(reply.data) != 1 {
						t.Fatalf("grouped reply: %+v", reply)
					}
				}
			} else {
				for _, path := range paths {
					var err error
					switch operation {
					case "render":
						_, err = Render(app.TopLevelDomainCache, nil, true, app.Backends, z.RenderReplicaMismatchConfig, context.Background(), path, 1, 2, app.ZipperMetrics, zap.NewNop())
					case "find":
						_, err = Find(app.TopLevelDomainCache, nil, true, app.Backends, context.Background(), path, app.ZipperMetrics, zap.NewNop())
					case "expand":
						response := httptest.NewRecorder()
						app.expandHandler(response, httptest.NewRequest(http.MethodGet, "/metrics/expand?query="+path+"&noCache=1", nil), zap.NewNop())
						if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), path) {
							t.Fatalf("expand: %d %s", response.Code, response.Body.String())
						}
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			for i, recorder := range recorders {
				recorder.mu.Lock()
				calls := recorder.calls
				recorder.mu.Unlock()
				if want := 4 - 2*i; calls != want {
					t.Errorf("backend %d: got %d target calls, want %d", i, calls, want)
				}
			}
		})
	}
}
