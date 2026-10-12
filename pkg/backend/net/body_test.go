package net

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// The encoder must produce byte-for-byte what url.Values.Encode produced.
func TestRenderEncoderMatchesValuesEncode(t *testing.T) {
	legacy := func(from, until int32, targets []string) string {
		vals := url.Values{
			"target": targets,
			"format": fmtProto,
			"from":   []string{strconv.Itoa(int(from))},
			"until":  []string{strconv.Itoa(int(until))},
		}
		return vals.Encode()
	}

	rnd := rand.New(rand.NewSource(1))
	alphabet := []rune("abcXYZ019._-*?{},[]%&=+ /\\'\"#~é😀")
	randTarget := func() string {
		n := rnd.Intn(40)
		var sb strings.Builder
		for i := 0; i < n; i++ {
			sb.WriteRune(alphabet[rnd.Intn(len(alphabet))])
		}
		return sb.String()
	}

	cases := [][]string{
		nil,
		{""},
		{"a.b.c"},
		{"aggregations.minutely.{a,b}.*.sum", "general.x[1-3].y?"},
	}
	for i := 0; i < 2000; i++ {
		ts := make([]string, rnd.Intn(4))
		for j := range ts {
			ts[j] = randTarget()
		}
		cases = append(cases, ts)
	}

	for _, ts := range cases {
		from := int32(rnd.Uint32())
		until := int32(rnd.Uint32())
		want := legacy(from, until, ts)
		got := carbonapiV2RenderEncoder(&url.URL{}, from, until, ts).RawQuery
		if got != want {
			t.Fatalf("targets %q from %d until %d:\n got %s\nwant %s", ts, from, until, got, want)
		}
	}
}

func gzipped(t *testing.T, b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// Bodies come back intact whether or not the backend compresses them, and a
// pooled buffer or gzip reader never leaks bytes from one response into the
// next.
func TestBodyGzipAndPlain(t *testing.T) {
	bodies := [][]byte{
		bytes.Repeat([]byte("0123456789"), 5000),
		[]byte("short"),
		{},
		bytes.Repeat([]byte{0, 1, 2, 3}, 70000),
		[]byte("tiny again"),
	}

	for _, compress := range []bool{true, false} {
		t.Run(fmt.Sprintf("gzip=%v", compress), func(t *testing.T) {
			var i int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
					t.Errorf("Accept-Encoding = %q, want gzip", got)
				}
				body := bodies[i%len(bodies)]
				i++
				if compress {
					w.Header().Set("Content-Encoding", "gzip")
					body = gzipped(t, body)
				}
				w.Write(body)
			}))
			defer server.Close()

			b, err := New(Config{Address: server.URL, Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 3; round++ {
				for _, want := range bodies {
					_, got, err := b.call(context.Background(), b.url("/render"), "render")
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got.data, want) {
						t.Fatalf("round %d: body mismatch: got %d bytes want %d", round, len(got.data), len(want))
					}
					got.release()
				}
			}
		})
	}
}

func TestBodyCorruptGzip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Write([]byte("definitely not gzip"))
	}))
	defer server.Close()

	b, err := New(Config{Address: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.call(context.Background(), b.url("/render"), "render"); err == nil {
		t.Fatal("expected an error for a corrupt gzip body")
	}
}

func TestNoGzipWhenTransportDisablesCompression(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "" {
			t.Errorf("Accept-Encoding = %q, want none", got)
		}
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	b, err := New(Config{Address: server.URL, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := b.call(context.Background(), b.url("/render"), "render")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.data) != "ok" {
		t.Fatalf("got %q", got.data)
	}
}

func TestGzipSizeHint(t *testing.T) {
	body := bytes.Repeat([]byte("abc"), 100000)
	z := gzipped(t, body)
	if got := gzipSizeHint(z); got != len(body) {
		t.Fatalf("hint %d, want %d", got, len(body))
	}
	if got := gzipSizeHint(z[:10]); got != 0 {
		t.Fatalf("short input: hint %d, want 0", got)
	}
	bogus := append([]byte(nil), z...)
	copy(bogus[len(bogus)-4:], []byte{0xff, 0xff, 0xff, 0x7f}) // ~2 GiB claimed
	if got := gzipSizeHint(bogus); got != 0 {
		t.Fatalf("implausible trailer: hint %d, want 0", got)
	}
}

// A multi-member stream only records the last member's size in its trailer;
// the hint is then too small and the buffer must still grow to fit.
func TestBodyMultiMemberGzip(t *testing.T) {
	first := bytes.Repeat([]byte("first member "), 20000)
	second := []byte("second")
	z := append(gzipped(t, first), gzipped(t, second)...)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(z)
	}))
	defer server.Close()

	b, err := New(Config{Address: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := b.call(context.Background(), b.url("/render"), "render")
	if err != nil {
		t.Fatal(err)
	}
	if want := append(append([]byte(nil), first...), second...); !bytes.Equal(got.data, want) {
		t.Fatalf("got %d bytes, want %d", len(got.data), len(want))
	}
}

// Bodies whose compressed size crosses the two-phase limit are inflated while
// streaming; both paths must return identical bytes, also with pooled readers
// alternating between them.
func TestBodyGzipAcrossTwoPhaseLimit(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	random := func(n int) []byte {
		b := make([]byte, n)
		rnd.Read(b) // incompressible, so the compressed size is ~n
		return b
	}
	bodies := [][]byte{
		random(twoPhaseGzipMax - 1000),
		random(twoPhaseGzipMax + 1),
		append(random(300000), bytes.Repeat([]byte("tail"), 50000)...),
		[]byte("small again"),
	}
	multi := append(gzipped(t, bodies[1]), gzipped(t, bodies[0])...)

	var i int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		if i == len(bodies) {
			w.Write(multi)
		} else {
			w.Write(gzipped(t, bodies[i]))
		}
		i++
	}))
	defer server.Close()

	b, err := New(Config{Address: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	wants := append(append([][]byte{}, bodies...), append(append([]byte(nil), bodies[1]...), bodies[0]...))
	for k, want := range wants {
		_, got, err := b.call(context.Background(), b.url("/render"), "render")
		if err != nil {
			t.Fatalf("body %d: %v", k, err)
		}
		if !bytes.Equal(got.data, want) {
			t.Fatalf("body %d: got %d bytes, want %d", k, len(got.data), len(want))
		}
		got.release()
	}
}
