package carbonapi_v2

import (
	"bytes"
	"math"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/types"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
)

func unpackStream(ms []types.Metric) []types.Metric {
	result, _ := types.MergeMetrics([][]types.Metric{ms}, cfg.RenderReplicaMismatchConfig{}, zap.NewNop())
	return result
}

func TestZeroStreamDifferential(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 4095, 4096, 4097, 10000} {
		for _, special := range []int{-1, 0, n / 2, n - 1} {
			for _, value := range []float64{0, math.Copysign(0, -1), 17, math.Float64frombits(0x7ff8000000000123)} {
				m := &pb.FetchResponse{Name: "zero.metric", Values: make([]float64, n), IsAbsent: make([]bool, n)}
				for i := range m.IsAbsent {
					m.IsAbsent[i] = i%7 == 0
				}
				if special >= 0 && special < n {
					m.Values[special] = value
				}
				body, err := (&pb.MultiFetchResponse{Metrics: []*pb.FetchResponse{m}}).MarshalVT()
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
					equalStreamMetrics(t, unpackStream(got), want)
				}
			}
		}
	}
}

func TestZeroStreamRepeatedFields(t *testing.T) {
	for _, nonzero := range []int{-1, 0, 129} {
		var metric []byte
		for i := 0; i < 260; i++ {
			// Mix packed and unpacked fields with absence and unknown fields.
			value := uint64(0)
			if i == nonzero {
				value = math.Float64bits(-3)
			}
			metric = protowire.AppendTag(metric, 5, protowire.Fixed64Type)
			metric = protowire.AppendFixed64(metric, value)
			metric = protowire.AppendTag(metric, 6, protowire.VarintType)
			metric = protowire.AppendVarint(metric, uint64(i%2))
			metric = protowire.AppendTag(metric, 5, protowire.BytesType)
			metric = protowire.AppendBytes(metric, make([]byte, 8))
		}
		body := protowire.AppendTag(nil, 1, protowire.BytesType)
		body = protowire.AppendBytes(body, metric)
		want, err := RenderDecoder(body)
		if err != nil {
			t.Fatal(err)
		}
		got, err := RenderStreamDecoderPacked(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		equalStreamMetrics(t, unpackStream(got), want)
		// Every truncated field must retain the reference rejection behavior.
		for i := 0; i < len(body); i++ {
			_, a := RenderDecoder(body[:i])
			_, b := RenderStreamDecoderPacked(bytes.NewReader(body[:i]))
			if (a == nil) != (b == nil) {
				t.Fatalf("cut %d: reference=%v packed=%v", i, a, b)
			}
		}
	}
}

func BenchmarkZeroStream(b *testing.B) {
	msg := &pb.MultiFetchResponse{}
	for i := 0; i < 100; i++ {
		msg.Metrics = append(msg.Metrics, &pb.FetchResponse{Values: make([]float64, 10000), IsAbsent: make([]bool, 10000)})
	}
	body, _ := msg.MarshalVT()
	for _, packed := range []bool{false, true} {
		b.Run(map[bool]string{false: "dense", true: "packed"}[packed], func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ms, err := renderStreamDecoder(bytes.NewReader(body), packed)
				if err != nil || len(ms) != 100 {
					b.Fatalf("decode: %v", err)
				}
			}
		})
	}
}
