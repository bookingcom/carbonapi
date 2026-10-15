package util

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/handlers"
)

// The pooled handler must be indistinguishable from gorilla's: same status,
// headers and body bytes, also when writers are reused across responses.
func TestCompressHandlerMatchesGorilla(t *testing.T) {
	rnd := rand.New(rand.NewSource(5))
	big := make([]byte, 300000)
	for i := range big {
		big[i] = "0123456789,.[]null"[rnd.Intn(18)]
	}

	inner := map[string]http.HandlerFunc{
		"small": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`[{"target":"a","datapoints":[[1,2]]}]`))
		},
		"big-json": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(big)
		},
		"status-and-length": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "5")
			w.WriteHeader(http.StatusTeapot)
			w.Write([]byte("hello"))
		},
		"chunks-and-flush": func(w http.ResponseWriter, r *http.Request) {
			for i := 0; i < 5; i++ {
				w.Write(big[i*1000 : (i+1)*1000])
				w.(http.Flusher).Flush()
			}
		},
		"readfrom": func(w http.ResponseWriter, r *http.Request) {
			io.Copy(w, bytes.NewReader(big[:70000]))
		},
		"empty": func(w http.ResponseWriter, r *http.Request) {},
	}
	encodings := []string{"gzip", "deflate", "gzip, deflate", "deflate, gzip", " br , gzip", "br", "", "identity"}

	for round := 0; round < 3; round++ {
		for name, h := range inner {
			for _, enc := range encodings {
				for _, upgrade := range []string{"", "websocket"} {
					want := serve(handlers.CompressHandler(h), enc, upgrade)
					got := serve(CompressHandler(h), enc, upgrade)
					where := fmt.Sprintf("round %d handler %s Accept-Encoding %q Upgrade %q", round, name, enc, upgrade)
					if got.Code != want.Code {
						t.Fatalf("%s: status %d, want %d", where, got.Code, want.Code)
					}
					if !reflect.DeepEqual(got.Header(), want.Header()) {
						t.Fatalf("%s: headers\n got %v\nwant %v", where, got.Header(), want.Header())
					}
					if !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
						t.Fatalf("%s: body differs (%d vs %d bytes)", where, got.Body.Len(), want.Body.Len())
					}
				}
			}
		}
	}
}

func serve(h http.Handler, acceptEncoding, upgrade string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/render", strings.NewReader(""))
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	if upgrade != "" {
		req.Header.Set("Upgrade", upgrade)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
