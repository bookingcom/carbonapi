package net

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
)

// A large gzip response trickling in at a cross-DC-ish rate: streaming
// decompression overlaps with the transfer, two-phase decompression does not.
func BenchmarkNetRenderSlowLargeGzip(b *testing.B) {
	body := payload(300, 8000) // ~22 MB raw
	var gz bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gz, gzip.DefaultCompression)
	zw.Write(body)
	zw.Close()
	z := gz.Bytes()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "gzip")
		const chunk = 32 << 10
		for off := 0; off < len(z); off += chunk {
			end := off + chunk
			if end > len(z) {
				end = len(z)
			}
			w.Write(z[off:end])
			w.(http.Flusher).Flush()
			time.Sleep(time.Millisecond)
		}
	}))
	defer srv.Close()
	be, _ := New(Config{Address: srv.URL, Client: &http.Client{Transport: &http.Transport{}}})
	req := types.NewRenderRequest([]string{"x.*"}, 0, 1)
	b.ReportMetric(float64(len(body))/1e6, "rawMB")
	b.ReportMetric(float64(len(z))/1e6, "gzMB")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := be.Render(context.Background(), req); err != nil {
			b.Fatal(err)
		}
	}
}
