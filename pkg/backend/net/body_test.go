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
					if !bytes.Equal(bodyBytes(got), want) {
						t.Fatalf("round %d: body mismatch: got %d bytes want %d", round, len(bodyBytes(got)), len(want))
					}
					putBody(got)
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
	if string(bodyBytes(got)) != "ok" {
		t.Fatalf("got %q", bodyBytes(got))
	}
}
