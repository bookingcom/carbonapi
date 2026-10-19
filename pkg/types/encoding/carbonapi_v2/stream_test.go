package carbonapi_v2

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
)

type fragmentedReader struct {
	data  []byte
	chunk int
}

func (r *fragmentedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data[:min(len(r.data), r.chunk)])
	r.data = r.data[n:]
	return n, nil
}
func equalStreamMetrics(t testing.TB, a, b []types.Metric) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("metrics: %d != %d", len(a), len(b))
	}
	for i := range a {
		av, bv := a[i].Values, b[i].Values
		if len(av) != len(bv) {
			t.Fatal("point count differs")
		}
		for j, v := range av {
			if math.Float64bits(v) != math.Float64bits(bv[j]) {
				t.Fatalf("float bits differ at metric %d point %d", i, j)
			}
		}
		a[i].Values, b[i].Values = nil, nil
		if !reflect.DeepEqual(a[i], b[i]) {
			t.Fatalf("metric metadata/absence differs: %+v != %+v", a[i], b[i])
		}
		a[i].Values, b[i].Values = av, bv
	}
}
func streamFixture(seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	msg := pb.MultiFetchResponse{}
	for i := 0; i < 1+int(seed%6); i++ {
		m := &pb.FetchResponse{Name: "a.b", StartTime: -300, StopTime: 900, StepTime: 60}
		for j := 0; j < int(seed%70); j++ {
			m.Values = append(m.Values, math.Float64frombits(rng.Uint64()))
			m.IsAbsent = append(m.IsAbsent, rng.Intn(4) == 0)
		}
		msg.Metrics = append(msg.Metrics, m)
	}
	b, err := msg.MarshalVT()
	if err != nil {
		panic(err)
	}
	return b
}
func TestStreamDecoderDifferential(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		body := streamFixture(seed)
		for _, chunk := range []int{1, 3, 31, 32768} {
			want, err := RenderDecoder(body)
			if err != nil {
				t.Fatal(err)
			}
			got, err := RenderStreamDecoder(&fragmentedReader{data: body, chunk: chunk})
			if err != nil {
				t.Fatal(err)
			}
			equalStreamMetrics(t, got, want)
			// Returned points and names remain valid after another decode reuses
			// the reader buffer and the caller overwrites its input bytes.
			copyBody := bytes.Clone(body)
			got, err = RenderStreamDecoder(bytes.NewReader(copyBody))
			if err != nil {
				t.Fatal(err)
			}
			clear(copyBody)
			_, err = RenderStreamDecoder(bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			want, err = RenderDecoder(body)
			if err != nil {
				t.Fatal(err)
			}
			equalStreamMetrics(t, got, want)
		}
	}
}
func TestStreamDecoderWireVariants(t *testing.T) {
	var metric []byte
	metric = protowire.AppendTag(metric, 1, protowire.BytesType)
	metric = protowire.AppendString(metric, "original")
	metric = protowire.AppendTag(metric, 1, protowire.BytesType)
	metric = protowire.AppendString(metric, "last.name")
	for _, v := range []uint64{math.Float64bits(-0.5), math.Float64bits(math.Inf(1))} {
		metric = protowire.AppendTag(metric, 5, protowire.Fixed64Type)
		metric = protowire.AppendFixed64(metric, v)
	}
	metric = protowire.AppendTag(metric, 5, protowire.BytesType)
	metric = protowire.AppendBytes(metric, binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.NaN())))
	metric = protowire.AppendTag(metric, 6, protowire.VarintType)
	metric = protowire.AppendVarint(metric, 19)
	metric = protowire.AppendTag(metric, 6, protowire.BytesType)
	metric = protowire.AppendBytes(metric, []byte{0x80, 0, 0xff, 1, 2, 0})
	metric = protowire.AppendTag(metric, 30, protowire.StartGroupType)
	metric = protowire.AppendTag(metric, 31, protowire.VarintType)
	metric = protowire.AppendVarint(metric, 10)
	metric = protowire.AppendTag(metric, 30, protowire.EndGroupType)
	body := protowire.AppendTag(nil, 1, protowire.BytesType)
	body = protowire.AppendBytes(body, metric)
	body = protowire.AppendTag(body, 40, protowire.Fixed32Type)
	body = protowire.AppendFixed32(body, 123)
	want, err := RenderDecoder(body)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RenderStreamDecoder(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	equalStreamMetrics(t, got, want)
}
func TestStreamDecoderTruncation(t *testing.T) {
	body := streamFixture(17)
	for i := 0; i < len(body); i++ {
		want, a := RenderDecoder(body[:i])
		got, b := RenderStreamDecoder(bytes.NewReader(body[:i]))
		if (a == nil) != (b == nil) {
			t.Fatalf("truncation %d: reference=%v streamed=%v", i, a, b)
		}
		if a == nil {
			equalStreamMetrics(t, got, want)
		}
	}
}
func FuzzStreamDecoder(f *testing.F) {
	for _, seed := range []int64{0, 1, 3, 19, 37} {
		f.Add(streamFixture(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<20 {
			return
		}
		want, a := RenderDecoder(body)
		got, b := RenderStreamDecoder(bytes.NewReader(body))
		if b == nil && a != nil {
			t.Fatalf("stream accepted input rejected by reference: %v", a)
		}
		if a == nil && b == nil {
			equalStreamMetrics(t, got, want)
		}
		packed, packedErr := RenderStreamDecoderPacked(bytes.NewReader(body))
		if packedErr == nil && a != nil {
			t.Fatalf("packed stream accepted input rejected by reference: %v", a)
		}
		if a == nil && packedErr == nil {
			equalStreamMetrics(t, unpackStream(packed), want)
		}
	})
}
func BenchmarkStreamDecoder(b *testing.B) {
	msg := pb.MultiFetchResponse{}
	for i := 0; i < 100; i++ {
		msg.Metrics = append(msg.Metrics, &pb.FetchResponse{Name: "realistic.metric.name", Values: make([]float64, 10000), IsAbsent: make([]bool, 10000)})
	}
	body, err := msg.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}
	for _, kind := range []string{"buffered", "streamed"} {
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				if kind == "buffered" {
					_, err = RenderDecoder(body)
				} else {
					_, err = RenderStreamDecoder(bytes.NewReader(body))
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPackedStreamDecoderReplicaBoundary(t *testing.T) {
	for _, n := range []int{128, 129, 1440, 1441} {
		m := types.Metric{Name: "a", Values: make([]float64, n), IsAbsent: make([]bool, n), StartTime: 1, StopTime: 9999, StepTime: 60}
		for i := range m.Values {
			m.IsAbsent[i] = i%3 == 0
			if i%67 == 0 {
				m.Values[i] = math.Float64frombits(uint64(i) | 0x7ff8000000000000)
			}
		}
		body, err := RenderEncoder([]types.Metric{m})
		if err != nil {
			t.Fatal(err)
		}
		want, err := RenderDecoder(body)
		if err != nil {
			t.Fatal(err)
		}
		for _, chunk := range []int{1, 31, 32768} {
			pending, err := RenderStreamDecoderPacked(&fragmentedReader{data: body, chunk: chunk})
			if err != nil {
				t.Fatal(err)
			}
			if len(pending[0].Values) != 0 {
				t.Fatal("sparse response was not packed")
			}
			got, _ := types.MergeMetrics([][]types.Metric{pending}, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
			equalStreamMetrics(t, got, want)
		}
	}
}
