package carbonapi_v2

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestPackedStreamDensityAndSplitFields(t *testing.T) {
	rng := rand.New(rand.NewSource(1818))
	for _, kind := range []string{"dense", "dense-prefix", "dense-suffix", "sparse"} {
		values := make([]float64, 6000)
		for i := range values {
			if kind == "dense" || kind == "dense-prefix" && i < 1024 || kind == "dense-suffix" && i > 1024 || kind == "sparse" && i%997 == 0 {
				values[i] = math.Float64frombits(rng.Uint64())
			}
		}
		var metric []byte
		for i := 0; i < len(values); {
			count := min(1+rng.Intn(150), len(values)-i)
			wire := make([]byte, count*8)
			for j := 0; j < count; j++ {
				binary.LittleEndian.PutUint64(wire[j*8:], math.Float64bits(values[i+j]))
			}
			metric = protowire.AppendTag(metric, 5, protowire.BytesType)
			metric = protowire.AppendBytes(metric, wire)
			i += count
		}
		absent := make([]byte, len(values))
		for i := range absent {
			if i%3 == 0 {
				absent[i] = 1
			}
		}
		metric = protowire.AppendTag(metric, 6, protowire.BytesType)
		metric = protowire.AppendBytes(metric, absent)
		body := protowire.AppendTag(nil, 1, protowire.BytesType)
		body = protowire.AppendBytes(body, metric)
		want, err := RenderDecoder(body)
		if err != nil {
			t.Fatal(err)
		}
		for _, chunk := range []int{1, 31, 32768} {
			got, err := RenderStreamDecoderPacked(&fragmentedReader{data: body, chunk: chunk})
			if err != nil {
				t.Fatal(kind, err)
			}
			equalStreamMetrics(t, unpackStream(got), want)
		}
	}
}

func TestPackedStreamMixedMetricOwnership(t *testing.T) {
	rng := rand.New(rand.NewSource(20261022))
	msg := &pb.MultiFetchResponse{}
	// Alternate packed and dense storage, and exercise both buffer growth
	// and shorter successors. Retained dense arrays must remain independent.
	for i := 0; i < 60; i++ {
		n := []int{6000, 6000, 256, 128, 127, 0}[i%6]
		m := &pb.FetchResponse{Name: string(rune('a' + i)), Values: make([]float64, n), IsAbsent: make([]bool, n)}
		for j := range m.Values {
			m.IsAbsent[j] = (i+j)%3 == 0
			if i%6 == 1 || j%997 == 0 {
				m.Values[j] = math.Float64frombits(rng.Uint64())
			}
		}
		msg.Metrics = append(msg.Metrics, m)
	}
	body, err := msg.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	want, err := RenderDecoder(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []int{1, 31, 32768} {
		got, err := RenderStreamDecoderPacked(&fragmentedReader{data: body, chunk: chunk})
		if err != nil {
			t.Fatal(err)
		}
		// Empty repeated fields retain the reference decoder's nil slices.
		for i := range want {
			if want[i].IsAbsent == nil && got[i].IsAbsent != nil {
				t.Fatalf("metric %d: empty absence field became non-nil", i)
			}
		}
		equalStreamMetrics(t, unpackStream(got), want)
	}
}

func BenchmarkSparseStream(b *testing.B) {
	msg := &pb.MultiFetchResponse{}
	for i := 0; i < 100; i++ {
		m := &pb.FetchResponse{Values: make([]float64, 10000), IsAbsent: make([]bool, 10000)}
		for j := i; j < len(m.Values); j += 997 {
			m.Values[j] = 17
		}
		msg.Metrics = append(msg.Metrics, m)
	}
	body, _ := msg.MarshalVT()
	for _, packed := range []bool{false, true} {
		b.Run(map[bool]string{false: "dense", true: "packed"}[packed], func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ms, err := renderStreamDecoder(bytes.NewReader(body), packed)
				if err != nil || len(ms) != 100 {
					b.Fatal(err)
				}
			}
		})
	}
}
