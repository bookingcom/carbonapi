package carbonapi

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/cache"
	"github.com/bookingcom/carbonapi/pkg/carbonapipb"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	exprtypes "github.com/bookingcom/carbonapi/pkg/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
	"github.com/bookingcom/carbonapi/pkg/types"
	wire "github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/dgryski/go-expirecache"
	"go.uber.org/zap"
)

func TestStableGroupsRealDriverParityAndBoundedProgress(t *testing.T) {
	var calls atomic.Int64
	var mu sync.Mutex
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw []byte
		var err error
		if strings.HasPrefix(r.URL.Path, "/metrics/find") {
			matches := types.Matches{Name: "m.*"}
			for i := 0; i < 40; i++ {
				matches.Matches = append(matches.Matches, types.Match{Path: fmt.Sprintf("m.%02d", i), IsLeaf: true})
			}
			matches.Matches = append(matches.Matches, types.Match{Path: "m.missing", IsLeaf: true})
			raw, err = wire.FindEncoder(matches)
		} else if strings.HasPrefix(r.URL.Path, "/render") {
			calls.Add(1)
			targets := r.URL.Query()["target"]
			mu.Lock()
			keys = append(keys, strings.Join(targets, ","))
			mu.Unlock()
			var metrics []types.Metric
			for _, path := range targets {
				if path != "m.missing" {
					metrics = append(metrics, types.Metric{Name: path, StartTime: 1, StopTime: 4, StepTime: 1, Values: []float64{math.Copysign(0, -1), 42, math.Float64frombits(0x7ff8000000000015)}, IsAbsent: []bool{false, false, true}})
				}
			}
			if len(metrics) == 0 {
				http.NotFound(w, r)
				return
			}
			raw, err = wire.RenderEncoder(metrics)
		} else {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			t.Error(err)
			http.Error(w, "encode", 500)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(raw)
	}))
	defer server.Close()
	makeApp := func(stable bool) *App {
		config := cfg.DefaultAPIConfig()
		config.ResolveGlobs = 1
		config.RenderBatchSize = 1
		if stable {
			config.RenderBatchSize = 16
		}
		config.MaxConcurrentUpstreamRequests = 4
		config.ProcWorkers = 8
		config.QueueSize = 64
		z := cfg.DefaultZipperConfig()
		z.Backends = []string{server.URL}
		z.ProtocolBackends = nil
		z.BackendsByCluster = nil
		z.BackendsByDC = nil
		z.BackendQueueSize = 256
		z.ConcurrencyLimitPerServer = 128
		app := &App{config: config, ZipperConfig: z, stableRenderBatch: stable, ms: newPrometheusMetrics(config), queryCache: cache.NullCache{}, findCache: cache.NullCache{}, TopLevelDomainCache: expirecache.New(0), fastQ: make(chan *RenderReq, 64), slowQ: make(chan *RenderReq, 64)}
		app.ZipperMetrics = NewZipperPrometheusMetrics(z)
		var err error
		app.Backends, err = InitBackends(z, app.ZipperMetrics, &app.ms, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		ProcessRequests(app, zap.NewNop())
		return app
	}
	request := func(app *App) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		exp, _, err := parser.ParseExpr("m.*")
		if err != nil {
			return nil, err
		}
		metrics := make(map[parser.MetricRequest][]*exprtypes.MetricData)
		partial := false
		err, _ = app.getTargetData(ctx, "m.*", exp, metrics, false, 1, 4, &carbonapipb.AccessLogDetails{}, zap.NewNop(), &partial)
		if err != nil || !partial {
			return nil, fmt.Errorf("error/partial changed: %v %v", err, partial)
		}
		var plain []types.Metric
		for _, series := range metrics {
			for _, m := range series {
				plain = append(plain, m.Metric)
			}
		}
		if len(plain) != 40 {
			return nil, fmt.Errorf("got %d series", len(plain))
		}
		return wire.RenderEncoder(plain)
	}
	reference := makeApp(false)
	want, err := request(reference)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 41 {
		t.Fatalf("reference made %d calls", calls.Load())
	}
	candidate := makeApp(true)
	before := calls.Load()
	got, err := request(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("grouping changed point bits, absence, names or times")
	}
	if delta := calls.Load() - before; delta != 11 {
		t.Fatalf("group limit lost: got %d calls, want 11", delta)
	}
	mu.Lock()
	firstKeys := append([]string(nil), keys[41:]...)
	sort.Strings(firstKeys)
	mu.Unlock()
	for range 3 {
		got, err = request(candidate)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("repeat changed output", err)
		}
	}
	mu.Lock()
	for repeat := 1; repeat < 4; repeat++ {
		current := append([]string(nil), keys[41+repeat*11:41+(repeat+1)*11]...)
		sort.Strings(current)
		if strings.Join(current, "|") != strings.Join(firstKeys, "|") {
			mu.Unlock()
			t.Fatal("backend cache keys changed across repeats")
		}
	}
	mu.Unlock()
	done := make(chan error, 8)
	for range 8 {
		go func() {
			got, err := request(candidate)
			if err == nil && !bytes.Equal(got, want) {
				err = fmt.Errorf("concurrent output changed")
			}
			done <- err
		}()
	}
	for range 8 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("real request driver stalled with grouped concurrency limit")
		}
	}
}
