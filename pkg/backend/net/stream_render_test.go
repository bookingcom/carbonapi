package net

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/bookingcom/carbonapi/pkg/types"
)

type gzipFragments struct {
	data  []byte
	chunk int
}

func (r *gzipFragments) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data[:min(r.chunk, len(r.data))])
	r.data = r.data[n:]
	return n, nil
}
func gzipData(t testing.TB, body []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestNativeInflaterCompressionVariants(t *testing.T) {
	noise := make([]byte, 65537)
	_, _ = rand.New(rand.NewSource(42)).Read(noise)
	for _, level := range []int{gzip.NoCompression, gzip.BestSpeed, gzip.BestCompression, gzip.HuffmanOnly} {
		for _, body := range [][]byte{noise, bytes.Repeat([]byte("repeated backend points\x00\xff"), 4096)} {
			var encoded bytes.Buffer
			w, err := gzip.NewWriterLevel(&encoded, level)
			if err != nil {
				t.Fatal(err)
			}
			w.Name, w.Comment, w.Extra = "backend", "optional gzip header", []byte{0, 1, 255}
			if _, err := w.Write(body); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			for _, chunk := range []int{1, 37, 32768} {
				base, err := gzip.NewReader(bytes.NewReader(encoded.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				want, err := io.ReadAll(base)
				if err != nil {
					t.Fatal(err)
				}
				r, err := newNativeInflater(&gzipFragments{data: encoded.Bytes(), chunk: chunk})
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("level=%d chunk=%d: %v; bytes=%d/%d", level, chunk, err, len(got), len(want))
				}
			}
		}
	}
}

func TestNativeInflaterDifferential(t *testing.T) {
	for _, size := range []int{0, 1, 31, 4096, 65536, 1 << 20} {
		body := bytes.Repeat([]byte("some repeated protobuf-like bytes\x00\x01\xff"), (size+38)/39)
		if len(body) > size {
			body = body[:size]
		}
		gz := gzipData(t, body)
		// Headers, optional fields, CRCs and concatenated streams are owned by
		// the gzip implementation. The oracle is the actual standard reader.
		for _, chunk := range []int{1, 37, 32768} {
			for _, encoded := range [][]byte{gz, append(bytes.Clone(gz), gz...)} {
				base, err := gzip.NewReader(&gzipFragments{data: encoded, chunk: chunk})
				if err != nil {
					t.Fatal(err)
				}
				want, a := io.ReadAll(base)
				r, err := newNativeInflater(&gzipFragments{data: encoded, chunk: chunk})
				if err != nil {
					t.Fatal(err)
				}
				got, b := io.ReadAll(r)
				r.Close()
				if (a == nil) != (b == nil) || !bytes.Equal(got, want) {
					t.Fatalf("size=%d chunk=%d: reference=%v native=%v bytes=%d/%d", size, chunk, a, b, len(want), len(got))
				}
				if err := r.Reset(bytes.NewReader(gz)); err != nil {
					t.Fatal(err)
				}
				got, err = io.ReadAll(r)
				r.Close()
				if err != nil || !bytes.Equal(got, body) {
					t.Fatalf("reset: %v", err)
				}
			}
		}
	}
}
func TestNativeInflaterRejectsCorruption(t *testing.T) {
	gz := gzipData(t, bytes.Repeat([]byte("long response with a CRC"), 100))
	cases := [][]byte{{}, gz[:len(gz)-1], gz[:10], append(bytes.Clone(gz), 42)}
	corrupt := bytes.Clone(gz)
	corrupt[len(corrupt)-5] ^= 16
	cases = append(cases, corrupt)
	for _, body := range cases {
		base, a := gzip.NewReader(bytes.NewReader(body))
		if a == nil {
			_, a = io.ReadAll(base)
		}
		r, b := newNativeInflater(bytes.NewReader(body))
		if b == nil {
			_, b = io.ReadAll(r)
		}
		if a == nil || b == nil {
			t.Fatalf("corrupt gzip accepted: reference=%v native=%v", a, b)
		}
	}
}
func equalStreamResults(t *testing.T, a, b []types.Metric) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatal("series count differs")
	}
	for i := range a {
		av, bv := a[i].Values, b[i].Values
		if len(av) != len(bv) {
			t.Fatal("point count differs")
		}
		for j, v := range av {
			if math.Float64bits(v) != math.Float64bits(bv[j]) {
				t.Fatal("float bits differ")
			}
		}
		a[i].Values, b[i].Values = nil, nil
		if !reflect.DeepEqual(a[i], b[i]) {
			t.Fatal("metadata/absence differs")
		}
		a[i].Values, b[i].Values = av, bv
	}
}
func TestStreamingRenderMatchesBuffered(t *testing.T) {
	for _, shape := range [][2]int{{1, 10}, {1, 10000}, {100, 10000}} {
		body := payload(shape[0], shape[1])
		srv := store(body)
		base, err := New(Config{Address: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		base.streamRender = false // Keep the buffered decoder as the reference.
		request := types.NewRenderRequest([]string{"fixture.*"}, 0, 1)
		want, err := base.Render(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		defaults, err := New(Config{Address: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		got, err := defaults.Render(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		equalStreamResults(t, got, want)
		for _, native := range []bool{false, true} {
			be, err := New(Config{Address: srv.URL})
			if err != nil {
				t.Fatal(err)
			}
			be.streamRender, be.nativeGzip = true, native
			got, err := be.Render(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			equalStreamResults(t, got, want)
		}
		srv.Close()
	}
}
func TestStreamingRenderValidatesGzipTrailer(t *testing.T) {
	body := gzipData(t, payload(1, 1000))
	body[len(body)-5] ^= 16
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(body)
	}))
	defer srv.Close()
	for _, native := range []bool{false, true} {
		be, err := New(Config{Address: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		be.streamRender, be.nativeGzip = true, native
		_, err = be.Render(context.Background(), types.NewRenderRequest([]string{"x"}, 0, 1))
		if err == nil {
			t.Fatal("corrupt gzip trailer accepted")
		}
	}
}
func BenchmarkRenderDecodeStrategies(b *testing.B) {
	for _, shape := range [][2]int{{1, 360}, {1, 10000}, {100, 10000}} {
		body := payload(shape[0], shape[1])
		srv := store(body)
		for _, kind := range []string{"buffered", "stream-go", "stream-native"} {
			b.Run(fmt.Sprintf("%s/metrics=%d/points=%d", kind, shape[0], shape[1]), func(b *testing.B) {
				be, err := New(Config{Address: srv.URL, Client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}})
				if err != nil {
					b.Fatal(err)
				}
				be.streamRender = kind != "buffered"
				be.nativeGzip = kind == "stream-native"
				request := types.NewRenderRequest([]string{"x.*"}, 0, 1)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := be.Render(context.Background(), request); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		srv.Close()
	}
}
