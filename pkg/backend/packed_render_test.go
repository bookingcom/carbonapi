package backend

import (
	"bytes"
	"compress/gzip"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	bnet "github.com/bookingcom/carbonapi/pkg/backend/net"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

func TestPackedHTTPReplicaPipeline(t *testing.T) {
	t.Setenv("CARBONAPI_STREAM_RENDER", "1")
	t.Setenv("CARBONAPI_NATIVE_GZIP", "1")
	var ordinary, packed []Backend
	for replica := 0; replica < 3; replica++ {
		message := pb.MultiFetchResponse{}
		for _, name := range []string{"a", "b", "c"} {
			m := &pb.FetchResponse{Name: name, StartTime: 1000, StopTime: 87400, StepTime: 60, Values: make([]float64, 1440), IsAbsent: make([]bool, 1440)}
			for i := range m.IsAbsent {
				m.IsAbsent[i] = i%7 == 0
			}
			m.Values[1] = math.Float64frombits(0x7ff8000000000123)
			m.Values[2] = 42
			if replica == 0 {
				m.Values[2] = 99
			}
			m.Values[3] = math.Copysign(0, -1)
			m.Values[4] = 7
			m.IsAbsent[4] = replica == 0
			message.Metrics = append(message.Metrics, m)
		}
		body, err := message.MarshalVT()
		if err != nil {
			t.Fatal(err)
		}
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		if _, err = writer.Write(body); err != nil {
			t.Fatal(err)
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
		wire := compressed.Bytes()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.Header().Set("Content-Encoding", "gzip")
			w.Write(wire)
		}))
		t.Cleanup(server.Close)
		for _, enabled := range []bool{false, true} {
			flag := "0"
			if enabled {
				flag = "1"
			}
			t.Setenv("CARBONAPI_PACK_BACKEND_POINTS", flag)
			transport := &http.Transport{MaxIdleConnsPerHost: 8}
			t.Cleanup(transport.CloseIdleConnections)
			netBackend, err := bnet.New(bnet.Config{Address: server.URL, Client: &http.Client{Transport: transport}})
			if err != nil {
				t.Fatal(err)
			}
			b := coalesceBackend(nil, 16)
			b.BackendImpl = netBackend
			b.Proc()
			if enabled {
				packed = append(packed, b)
			} else {
				ordinary = append(ordinary, b)
			}
		}
	}
	c := cfg.RenderReplicaMismatchConfig{RenderReplicaMatchMode: cfg.ReplicaMatchModeMajority}
	request := types.NewRenderRequest([]string{"a", "b", "c"}, 1000, 87400)
	for iteration := 0; iteration < 4; iteration++ {
		want, ws, we := Renders(context.Background(), ordinary, request, c, zap.NewNop())
		got, gs, ge := Renders(context.Background(), packed, request, c, zap.NewNop())
		if len(we) != 0 || len(ge) != 0 || gs != ws {
			t.Fatalf("stats/errors differ: %+v %+v %v %v", gs, ws, ge, we)
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Name < got[j].Name })
		sort.Slice(want, func(i, j int) bool { return want[i].Name < want[j].Name })
		if len(got) != len(want) {
			t.Fatal("series count differs")
		}
		for i := range got {
			if len(got[i].Values) != len(want[i].Values) {
				t.Fatal("point count differs")
			}
			for j, v := range got[i].Values {
				if math.Float64bits(v) != math.Float64bits(want[i].Values[j]) {
					t.Fatalf("bits differ at %d/%d", i, j)
				}
			}
			got[i].Values, want[i].Values = nil, nil
			if !reflect.DeepEqual(got[i], want[i]) {
				t.Fatal("metadata/absence differs")
			}
		}
	}
}
