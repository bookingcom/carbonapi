package net

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/types"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

// payload builds a store-like MultiFetchResponse: random-walk gauges with
// runs of repeated values and some gaps, like real minutely series.
func payload(metrics, points int) []byte {
	rnd := rand.New(rand.NewSource(int64(metrics*7919 + points)))
	resp := pb.MultiFetchResponse{}
	for m := 0; m < metrics; m++ {
		vals := make([]float64, points)
		absent := make([]bool, points)
		v := float64(rnd.Intn(1000))
		for i := range vals {
			switch r := rnd.Intn(10); {
			case r == 0:
				absent[i] = true
				vals[i] = math.NaN()
				continue
			case r < 6:
			default:
				v += float64(rnd.Intn(21) - 10)
			}
			vals[i] = v
		}
		resp.Metrics = append(resp.Metrics, &pb.FetchResponse{
			Name:      fmt.Sprintf("aggregations.minutely.service%d.by_host.host-%03d.latency.p99", m%13, m),
			StartTime: 1791560000,
			StopTime:  1791560000 + int32(points*60),
			StepTime:  60,
			Values:    vals,
			IsAbsent:  absent,
		})
	}
	b, err := resp.MarshalVT()
	if err != nil {
		panic(err)
	}
	return b
}

// store emulates go-carbon behind NYTimes/gziphandler (min size 1400).
func store(body []byte) *httptest.Server {
	var gz bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gz, gzip.DefaultCompression)
	zw.Write(body)
	zw.Close()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		if len(body) >= 1400 && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Write(gz.Bytes())
			return
		}
		w.Write(body)
	}))
}

func BenchmarkNetRender(b *testing.B) {
	for _, c := range []struct{ metrics, points int }{
		{1, 10},    // 10 min of minutely: below the gzip threshold
		{1, 360},   // 6h
		{1, 1440},  // 1d
		{20, 1440}, // small glob
		{200, 360}, // larger glob
	} {
		body := payload(c.metrics, c.points)
		srv := store(body)
		be, err := New(Config{Address: srv.URL, Client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}})
		if err != nil {
			b.Fatal(err)
		}
		req := types.NewRenderRequest([]string{"aggregations.minutely.service*.by_host.*.latency.p99"}, 1791560000, 1791646400)
		b.Run(fmt.Sprintf("metrics=%d/points=%d/bytes=%d", c.metrics, c.points, len(body)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ms, err := be.Render(context.Background(), req)
				if err != nil || len(ms) != c.metrics {
					b.Fatal(err, len(ms))
				}
			}
		})
		srv.Close()
	}
}
