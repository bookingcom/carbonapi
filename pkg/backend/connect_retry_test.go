package backend

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

var refusedDial = &url.Error{Op: "Get", URL: "http://store/render/", Err: &net.OpError{Op: "dial", Net: "tcp",
	Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}

type retryTestBackend struct {
	coalesceTestBackend
	cluster, dc string
}

func (b retryTestBackend) BackendInfo() (string, string, string) { return b.cluster, b.cluster, b.dc }

// retryStore fails its first refusals calls with failure, then answers.
func retryStore(cluster string, refusals int32, failure error, answer error) (Backend, *atomic.Int32) {
	return retryStoreIn("dc1", cluster, refusals, failure, answer)
}

// retryStoreIn is retryStore in the replica group of dc.
func retryStoreIn(dc, cluster string, refusals int32, failure error, answer error) (Backend, *atomic.Int32) {
	return retryStoreFunc(dc, cluster, refusals, failure, func(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
		if answer != nil {
			return nil, answer
		}
		return coalesceMetrics(r.Targets), nil
	})
}

func retryStoreFunc(dc, cluster string, refusals int32, failure error,
	serve func(context.Context, types.RenderRequest) ([]types.Metric, error)) (Backend, *atomic.Int32) {
	calls := new(atomic.Int32)
	b := coalesceBackend(func(ctx context.Context, r types.RenderRequest) ([]types.Metric, error) {
		if calls.Add(1) <= refusals {
			return nil, failure
		}
		return serve(ctx, r)
	}, 4)
	b.BackendImpl = retryTestBackend{coalesceTestBackend: b.BackendImpl.(coalesceTestBackend), cluster: cluster, dc: dc}
	b.Proc()
	return b, calls
}

func counterValue(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		panic(err)
	}
	return m.GetCounter().GetValue()
}

func fastConnectRetry(t *testing.T, window time.Duration) (retries, recoveries prometheus.Counter) {
	t.Helper()
	retries, recoveries, _ = fastConnectRetryCancels(t, window)
	return retries, recoveries
}

func fastConnectRetryCancels(t *testing.T, window time.Duration) (retries, recoveries, cancels prometheus.Counter) {
	t.Helper()
	retries = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_retries"})
	recoveries = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_recoveries"})
	cancels = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_cancels"})
	SetConnectRetry(ConnectRetry{Window: window, InitialDelay: 10 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		Retries: retries, Recoveries: recoveries, Cancellations: cancels})
	t.Cleanup(func() { connectRetry.Store(nil) })
	return retries, recoveries, cancels
}

// fetchBoth runs the ordinary and the grouped (stable batch) fan-in.
func fetchBoth(t *testing.T, ctx context.Context, backends []Backend, path string, check func(name string, metrics []types.Metric, errs []error)) {
	t.Helper()
	request := types.NewRenderRequest([]string{path}, 1, 4)
	metrics, _, errs := Renders(ctx, backends, request, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	check("ordinary", metrics, errs)
	replies := RendersGrouped(ctx, [][]Backend{backends, backends}, []types.RenderRequest{request, types.NewRenderRequest([]string{path + ".2"}, 1, 4)},
		cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	for _, reply := range replies {
		check("grouped", reply.Metrics, reply.Errors)
	}
}

func TestConnectRetryRefusedThenUp(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		retries, recoveries := fastConnectRetry(t, 2*time.Second)
		// The store refuses its first three connections, then serves data.
		b, _ := retryStore("cx", 3, refusedDial, nil)
		start := time.Now()
		request := types.NewRenderRequest([]string{"m"}, 1, 4)
		replies := []GroupedRenderReply{{}}
		if grouped {
			replies = RendersGrouped(context.Background(), [][]Backend{{b}, {b}},
				[]types.RenderRequest{request, types.NewRenderRequest([]string{"n"}, 1, 4)}, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
		} else {
			replies[0].Metrics, _, replies[0].Errors = Renders(context.Background(), []Backend{b}, request, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
		}
		for _, reply := range replies {
			if len(reply.Errors) != 0 || len(reply.Metrics) != 1 || reply.Metrics[0].Values[1] != 42 {
				t.Fatalf("grouped %v: %+v, want full data", grouped, reply)
			}
		}
		if counterValue(retries) == 0 || counterValue(recoveries) == 0 || time.Since(start) > time.Second {
			t.Fatalf("grouped %v: retries %v recoveries %v after %v", grouped, counterValue(retries), counterValue(recoveries), time.Since(start))
		}
	}
}

func TestConnectRetryWindowExhausted(t *testing.T) {
	const window = 200 * time.Millisecond
	retries, recoveries := fastConnectRetry(t, window)
	b, _ := retryStore("cx", 1<<30, refusedDial, nil)
	start := time.Now()
	metrics, _, errs := Renders(context.Background(), []Backend{b}, types.NewRenderRequest([]string{"m"}, 1, 4),
		cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	elapsed := time.Since(start)
	if len(metrics) != 0 || len(errs) != 1 || !errors.Is(errs[0], syscall.ECONNREFUSED) {
		t.Fatalf("metrics %+v errs %v, want the refusal", metrics, errs)
	}
	var u unreachableError
	if errors.As(errs[0], &u) {
		t.Fatal("internal retry marker leaked")
	}
	if elapsed < window-20*time.Millisecond || elapsed > window+time.Second {
		t.Fatalf("gave up after %v, want about %v", elapsed, window)
	}
	if counterValue(retries) == 0 || counterValue(recoveries) != 0 {
		t.Fatalf("retries %v recoveries %v", counterValue(retries), counterValue(recoveries))
	}
}

func TestConnectRetryRespectsDeadline(t *testing.T) {
	fastConnectRetry(t, 10*time.Second)
	b, _ := retryStore("cx", 1<<30, refusedDial, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _, errs := Renders(ctx, []Backend{b}, types.NewRenderRequest([]string{"m"}, 1, 4), cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	if len(errs) != 1 || !errors.Is(errs[0], syscall.ECONNREFUSED) {
		t.Fatalf("errs %v, want the refusal", errs)
	}
	if ctx.Err() != nil {
		t.Fatal("retries ran past the request deadline")
	}
}

func TestConnectRetrySkipsAnsweredGroup(t *testing.T) {
	for _, peer := range []error{nil, types.ErrMetricsNotFound} {
		retries, _ := fastConnectRetry(t, 2*time.Second)
		refused, calls := retryStore("cy", 1<<30, refusedDial, nil)
		answering, _ := retryStore("cy", 0, nil, peer)
		start := time.Now()
		fetchBoth(t, context.Background(), []Backend{refused, answering}, "m", func(name string, metrics []types.Metric, errs []error) {
			wantErrs := 1
			if peer != nil {
				wantErrs = 2
			}
			if len(errs) != wantErrs || (peer == nil) != (len(metrics) == 1) {
				t.Fatalf("%s peer %v: metrics %+v errs %v, want group recovery", name, peer, metrics, errs)
			}
			for _, err := range errs {
				if !errors.Is(err, types.ErrMetricsNotFound) {
					t.Fatalf("%s peer %v: unrecovered %v", name, peer, errs)
				}
			}
		})
		if calls.Load() == 0 || counterValue(retries) != 0 || time.Since(start) > time.Second {
			t.Fatalf("peer %v: answered group was retried: retries %v after %v", peer, counterValue(retries), time.Since(start))
		}
	}
}

// A store whose whole replica group is unreachable is not retried once another
// DC's group of the same cluster returned data: the render returns at the
// healthy groups' speed with their full data, and the refusal stays a plain
// error that the required-fetch rule recovers from the other group's data.
func TestConnectRetryCancelledByOtherGroup(t *testing.T) {
	retries, _, cancels := fastConnectRetryCancels(t, 5*time.Second)
	refused, _ := retryStoreIn("dc1", "cy", 1<<30, refusedDial, nil)
	lhr, _ := retryStoreIn("dc3", "cy", 0, nil, nil)
	fra, _ := retryStoreIn("dc2", "cy", 0, nil, nil)
	start := time.Now()
	fetchBoth(t, context.Background(), []Backend{refused, lhr, fra}, "m", func(name string, metrics []types.Metric, errs []error) {
		if len(metrics) != 1 || metrics[0].Values[1] != 42 {
			t.Fatalf("%s: metrics %+v, want the other groups' data", name, metrics)
		}
		if len(errs) != 1 || !errors.Is(errs[0], syscall.ECONNREFUSED) {
			t.Fatalf("%s: errs %v, want the refusal", name, errs)
		}
		var u unreachableError
		if errors.As(errs[0], &u) {
			t.Fatalf("%s: internal retry marker leaked", name)
		}
	})
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("waited %v for a store other groups had already covered", elapsed)
	}
	// One ordinary and two grouped fetches.
	if counterValue(retries) != 0 || counterValue(cancels) != 3 {
		t.Fatalf("retries %v cancels %v, want 0/3", counterValue(retries), counterValue(cancels))
	}
}

// Only an answer from a replica group of the same cluster makes a retry
// redundant.
func TestConnectRetryNotCancelledWithoutReplicaAnswer(t *testing.T) {
	const window = 150 * time.Millisecond
	for _, tc := range []struct {
		name          string
		dc, cluster   string
		answer        error
		ungroupedPeer bool
	}{
		{name: "other_cluster_data", dc: "dc3", cluster: "carbon"},
		{name: "other_cluster_not_found", dc: "dc3", cluster: "carbon", answer: types.ErrMetricsNotFound},
		{name: "other_group_server_error", dc: "dc3", cluster: "cy", answer: bnet.ErrHTTPCode(http.StatusServiceUnavailable)},
		{name: "ungrouped", ungroupedPeer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retries, _, cancels := fastConnectRetryCancels(t, window)
			cluster := "cy"
			if tc.ungroupedPeer {
				cluster = ""
			}
			refused, _ := retryStoreIn("dc1", cluster, 1<<30, refusedDial, nil)
			peer, _ := retryStoreIn(tc.dc, tc.cluster, 0, nil, tc.answer)
			start := time.Now()
			Renders(context.Background(), []Backend{refused, peer}, types.NewRenderRequest([]string{"m"}, 1, 4),
				cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
			if elapsed := time.Since(start); elapsed < window-20*time.Millisecond || counterValue(retries) == 0 || counterValue(cancels) != 0 {
				t.Fatalf("gave up after %v: retries %v cancels %v, want the full window", elapsed, counterValue(retries), counterValue(cancels))
			}
		})
	}
}

// The fe-112 canary: the refused store is the only member of one of its
// cluster's replica groups, the other groups of that cluster answer not-found,
// and the series lives in another cluster. Replicas of one cluster hold the
// same names, so their not-found already tells what the refused store would
// say: the fetch is given up at once instead of after the whole retry window.
// The refusal stays a plain connect error, so the required-fetch and replica-group rules decide the render as
// without retries: another cluster's data completes it, and with no data
// anywhere the fetch fails as a required fetch did before.
func TestConnectRetryCancelledByOtherGroupsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dataCluster bool // a group of another cluster holds the series
		lhrRefused  bool // a second group of the refused cluster is unreachable too
		wantCancels float64
	}{
		{name: "series_in_other_cluster", dataCluster: true, wantCancels: 3},
		{name: "series_nowhere", wantCancels: 3},
		{name: "two_groups_refused_one_not_found", dataCluster: true, lhrRefused: true, wantCancels: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retries, _, cancels := fastConnectRetryCancels(t, 5*time.Second)
			refused, _ := retryStoreIn("dc1", "carbon", 1<<30, refusedDial, nil)
			lhrRefusals := int32(0)
			if tc.lhrRefused {
				lhrRefusals = 1 << 30
			}
			lhr, _ := retryStoreIn("dc3", "carbon", lhrRefusals, refusedDial, types.ErrMetricsNotFound)
			fra, _ := retryStoreIn("dc2", "carbon", 0, nil, types.ErrMetricsNotFound)
			backends := []Backend{refused, lhr, fra}
			if tc.dataCluster {
				cy, _ := retryStoreIn("dc2", "cy", 0, nil, nil)
				backends = append(backends, cy)
			}
			start := time.Now()
			fetchBoth(t, context.Background(), backends, "m", func(name string, metrics []types.Metric, errs []error) {
				if (len(metrics) == 1) != tc.dataCluster {
					t.Fatalf("%s: metrics %+v, want data %v", name, metrics, tc.dataCluster)
				}
				refusals, notFound := 0, 0
				for _, err := range errs {
					var u unreachableError
					switch {
					case errors.As(err, &u):
						t.Fatalf("%s: internal retry marker leaked: %v", name, errs)
					case errors.Is(err, syscall.ECONNREFUSED):
						refusals++
					case errors.Is(err, types.ErrMetricsNotFound):
						notFound++
					}
				}
				wantRefusals := 1
				if tc.lhrRefused {
					wantRefusals = 2
				}
				if refusals != wantRefusals || notFound != 3-wantRefusals {
					t.Fatalf("%s: errs %v, want %d refusals as plain errors and the not-founds", name, errs, wantRefusals)
				}
			})
			if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
				t.Fatalf("waited %v although the refused cluster's other groups answered not-found", elapsed)
			}
			if counterValue(retries) != 0 || counterValue(cancels) != tc.wantCancels {
				t.Fatalf("retries %v cancels %v, want 0/%v", counterValue(retries), counterValue(cancels), tc.wantCancels)
			}
		})
	}
}

// Every group of the cluster refuses, then one comes back and answers
// not-found: the others' retries stop there.
func TestConnectRetryAllGroupsRefusedThenNotFound(t *testing.T) {
	retries, recoveries, cancels := fastConnectRetryCancels(t, 2*time.Second)
	ams, _ := retryStoreIn("dc1", "carbon", 1<<30, refusedDial, nil)
	lhr, _ := retryStoreIn("dc3", "carbon", 1<<30, refusedDial, nil)
	fra, _ := retryStoreIn("dc2", "carbon", 3, refusedDial, types.ErrMetricsNotFound)
	start := time.Now()
	metrics, _, errs := Renders(context.Background(), []Backend{ams, lhr, fra}, types.NewRenderRequest([]string{"m"}, 1, 4),
		cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %v after a group answered not-found", elapsed)
	}
	if len(metrics) != 0 || len(errs) != 3 {
		t.Fatalf("metrics %+v errs %v", metrics, errs)
	}
	if counterValue(retries) < 9 || counterValue(recoveries) != 1 || counterValue(cancels) != 2 {
		t.Fatalf("retries %v recoveries %v cancels %v", counterValue(retries), counterValue(recoveries), counterValue(cancels))
	}
}

// When every group was unreachable and one recovers, the others' repeats are
// not awaited, even if a just-restarted store accepts but answers slowly.
func TestConnectRetryStopsWaitingOnceAGroupRecovers(t *testing.T) {
	_, recoveries, cancels := fastConnectRetryCancels(t, 5*time.Second)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	slow, _ := retryStoreFunc("dc1", "cy", 1, refusedDial, func(context.Context, types.RenderRequest) ([]types.Metric, error) {
		<-release
		return nil, refusedDial
	})
	lhr, _ := retryStoreIn("dc3", "cy", 1, refusedDial, nil)
	start := time.Now()
	metrics, _, errs := Renders(context.Background(), []Backend{slow, lhr}, types.NewRenderRequest([]string{"m"}, 1, 4),
		cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("waited %v on a slow repeat after another group recovered", elapsed)
	}
	if len(metrics) != 1 || len(errs) != 1 || !errors.Is(errs[0], syscall.ECONNREFUSED) {
		t.Fatalf("metrics %+v errs %v, want lhr data and the ams refusal", metrics, errs)
	}
	var u unreachableError
	if errors.As(errs[0], &u) {
		t.Fatal("internal retry marker leaked")
	}
	if counterValue(recoveries) != 1 || counterValue(cancels) != 1 {
		t.Fatalf("recoveries %v cancels %v, want 1/1", counterValue(recoveries), counterValue(cancels))
	}
}

// All replica groups refuse briefly: retries continue until one is back.
func TestConnectRetryAllGroupsRefusedThenUp(t *testing.T) {
	retries, recoveries, cancels := fastConnectRetryCancels(t, 2*time.Second)
	ams, _ := retryStoreIn("dc1", "cy", 1<<30, refusedDial, nil)
	lhr, _ := retryStoreIn("dc3", "cy", 3, refusedDial, nil)
	metrics, _, errs := Renders(context.Background(), []Backend{ams, lhr}, types.NewRenderRequest([]string{"m"}, 1, 4),
		cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	if len(metrics) != 1 || metrics[0].Values[1] != 42 || len(errs) != 1 || !errors.Is(errs[0], syscall.ECONNREFUSED) {
		t.Fatalf("metrics %+v errs %v, want lhr data after its restart", metrics, errs)
	}
	if counterValue(retries) < 6 || counterValue(recoveries) != 1 || counterValue(cancels) != 1 {
		t.Fatalf("retries %v recoveries %v cancels %v", counterValue(retries), counterValue(recoveries), counterValue(cancels))
	}
}

func TestConnectRetryKeepsFailuresAfterAccept(t *testing.T) {
	for _, failure := range []error{
		bnet.ErrHTTPCode(http.StatusServiceUnavailable),
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, // body read, no *url.Error
		&url.Error{Op: "Get", URL: "http://store/render/", Err: context.DeadlineExceeded},
	} {
		retries, _ := fastConnectRetry(t, 2*time.Second)
		b, calls := retryStore("cx", 1, failure, nil)
		_, _, errs := Renders(context.Background(), []Backend{b}, types.NewRenderRequest([]string{"m"}, 1, 4),
			cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
		if len(errs) != 1 || errs[0] != failure || calls.Load() != 1 || counterValue(retries) != 0 {
			t.Fatalf("%v: errs %v calls %d retries %v, want one final failure", failure, errs, calls.Load(), counterValue(retries))
		}
	}
}

// Real transports: a closed port and a peer that drops the connection before
// any response are connect failures; failures after a response started are not.
func TestConnectFailureClassification(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusedAddr := closed.Addr().String()
	closed.Close()

	serve := func(handle func(net.Conn)) string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go func() {
					defer c.Close()
					buf := make([]byte, 4096)
					_, _ = c.Read(buf)
					handle(c)
				}()
			}
		}()
		return l.Addr().String()
	}
	dropped := serve(func(c net.Conn) {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0) // RST instead of FIN
		}
	})
	truncated := serve(func(c net.Conn) {
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 1000\r\n\r\npartial")
	})
	failed := serve(func(c net.Conn) {
		_, _ = io.WriteString(c, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
	})

	for _, transport := range []http.RoundTripper{
		&http.Transport{DisableCompression: true},
		bnet.NewSyncTransport(&http.Transport{DisableCompression: true}),
	} {
		for _, tc := range []struct {
			addr string
			want bool
		}{{refusedAddr, true}, {dropped, true}, {truncated, false}, {failed, false}} {
			nb, err := bnet.New(bnet.Config{Address: tc.addr, Cluster: "c", Client: &http.Client{Transport: transport}, Timeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			_, err = nb.Render(context.Background(), types.NewRenderRequest([]string{"m"}, 1, 4))
			if err == nil || connectFailure(err) != tc.want {
				t.Errorf("%T %s: %v classified %v, want %v", transport, tc.addr, err, err != nil && connectFailure(err), tc.want)
			}
		}
	}
}
