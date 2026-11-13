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
	cluster string
}

func (b retryTestBackend) BackendInfo() (string, string, string) { return b.cluster, b.cluster, "dc1" }

// retryStore fails its first refusals calls with failure, then answers.
func retryStore(cluster string, refusals int32, failure error, answer error) (Backend, *atomic.Int32) {
	calls := new(atomic.Int32)
	b := coalesceBackend(func(_ context.Context, r types.RenderRequest) ([]types.Metric, error) {
		if calls.Add(1) <= refusals {
			return nil, failure
		}
		if answer != nil {
			return nil, answer
		}
		return coalesceMetrics(r.Targets), nil
	}, 4)
	b.BackendImpl = retryTestBackend{coalesceTestBackend: b.BackendImpl.(coalesceTestBackend), cluster: cluster}
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
	retries = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_retries"})
	recoveries = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_recoveries"})
	SetConnectRetry(ConnectRetry{Window: window, InitialDelay: 10 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		Retries: retries, Recoveries: recoveries})
	t.Cleanup(func() { connectRetry.Store(nil) })
	return retries, recoveries
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
