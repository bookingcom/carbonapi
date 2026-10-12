package util

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/felixge/httpsnoop"
)

// CompressHandler behaves exactly like gorilla/handlers.CompressHandler (same
// encoding negotiation, headers and compressed bytes), but reuses gzip and
// flate writers instead of allocating a new one, several hundred KB, for every
// response.
func CompressHandler(h http.Handler) http.Handler {
	const (
		acceptEncoding = "Accept-Encoding"
		gzipEncoding   = "gzip"
		flateEncoding  = "deflate"
	)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// detect what encoding to use
		var encoding string
		for _, curEnc := range strings.Split(r.Header.Get(acceptEncoding), ",") {
			curEnc = strings.TrimSpace(curEnc)
			if curEnc == gzipEncoding || curEnc == flateEncoding {
				encoding = curEnc
				break
			}
		}

		// always add Accept-Encoding to Vary to prevent intermediate caches corruption
		w.Header().Add("Vary", acceptEncoding)

		if encoding == "" || r.Header.Get("Upgrade") != "" {
			h.ServeHTTP(w, r)
			return
		}

		var encWriter io.WriteCloser
		if encoding == gzipEncoding {
			gw := getGzipWriter(w)
			defer putGzipWriter(gw)
			encWriter = gw
		} else {
			fw := getFlateWriter(w)
			defer putFlateWriter(fw)
			encWriter = fw
		}
		defer encWriter.Close()

		w.Header().Set("Content-Encoding", encoding)
		r.Header.Del(acceptEncoding)

		cw := &compressResponseWriter{
			w:          w,
			compressor: encWriter,
		}

		w = httpsnoop.Wrap(w, httpsnoop.Hooks{
			Write: func(httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return cw.Write
			},
			WriteHeader: func(httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return cw.WriteHeader
			},
			Flush: func(httpsnoop.FlushFunc) httpsnoop.FlushFunc {
				return cw.Flush
			},
			ReadFrom: func(rff httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
				return cw.ReadFrom
			},
		})

		h.ServeHTTP(w, r)
	})
}

var (
	gzipWriterPool  sync.Pool // *gzip.Writer
	flateWriterPool sync.Pool // *flate.Writer
)

// Reset gives a writer exactly the state gzip.NewWriterLevel / flate.NewWriter
// would, so pooled writers produce the same bytes as fresh ones.

func getGzipWriter(w io.Writer) *gzip.Writer {
	if gw, ok := gzipWriterPool.Get().(*gzip.Writer); ok {
		gw.Reset(w)
		return gw
	}
	gw, _ := gzip.NewWriterLevel(w, gzip.DefaultCompression)
	return gw
}

func putGzipWriter(gw *gzip.Writer) {
	gw.Reset(io.Discard) // drop the reference to the response
	gzipWriterPool.Put(gw)
}

func getFlateWriter(w io.Writer) *flate.Writer {
	if fw, ok := flateWriterPool.Get().(*flate.Writer); ok {
		fw.Reset(w)
		return fw
	}
	fw, _ := flate.NewWriter(w, gzip.DefaultCompression)
	return fw
}

func putFlateWriter(fw *flate.Writer) {
	fw.Reset(io.Discard)
	flateWriterPool.Put(fw)
}

type compressResponseWriter struct {
	compressor io.Writer
	w          http.ResponseWriter
}

func (cw *compressResponseWriter) WriteHeader(c int) {
	cw.w.Header().Del("Content-Length")
	cw.w.WriteHeader(c)
}

func (cw *compressResponseWriter) Write(b []byte) (int, error) {
	h := cw.w.Header()
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", http.DetectContentType(b))
	}
	h.Del("Content-Length")

	return cw.compressor.Write(b)
}

func (cw *compressResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(cw.compressor, r)
}

type flusher interface {
	Flush() error
}

func (cw *compressResponseWriter) Flush() {
	// Flush compressed data if compressor supports it.
	if f, ok := cw.compressor.(flusher); ok {
		_ = f.Flush()
	}
	// Flush HTTP response.
	if f, ok := cw.w.(http.Flusher); ok {
		f.Flush()
	}
}
