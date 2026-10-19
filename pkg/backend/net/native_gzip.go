//go:build nativegzip && cgo

package net

/*
#cgo LDFLAGS: -lz
#include <stdlib.h>
#include <zlib.h>

typedef struct { unsigned int used, produced; int code; } carbonapi_inflate_result;
static z_stream *carbonapi_inflate_new(void) {
    z_stream *s = (z_stream *)calloc(1, sizeof(z_stream));
    if (s && inflateInit2(s, 15 + 16) != Z_OK) { free(s); return NULL; }
    return s;
}
static int carbonapi_inflate_reset(z_stream *s) { return inflateReset2(s, 15 + 16); }
static void carbonapi_inflate_free(z_stream *s) { if (s) { inflateEnd(s); free(s); } }
static carbonapi_inflate_result carbonapi_inflate_step(z_stream *s, unsigned char *in,
        unsigned int input_size, unsigned char *out, unsigned int output_size) {
    s->next_in = in; s->avail_in = input_size;
    s->next_out = out; s->avail_out = output_size;
    int code = inflate(s, Z_NO_FLUSH);
    carbonapi_inflate_result result = {input_size - s->avail_in, output_size - s->avail_out, code};
    // Never retain Go input/output pointers after returning across the cgo boundary.
    s->next_in = NULL; s->next_out = NULL; s->avail_in = 0; s->avail_out = 0;
    return result;
}
*/
import "C"

import (
	"compress/gzip"
	"fmt"
	"io"
	"runtime"
	"strings"
	"unsafe"
)

type nativeGzipReader struct {
	state                 *C.z_stream
	src                   io.Reader
	input                 []byte
	pos, end              int
	sourceErr, errorState error
	memberEnded           bool
}

func newNativeInflater(src io.Reader) (renderInflater, error) {
	r := &nativeGzipReader{state: C.carbonapi_inflate_new(), input: make([]byte, 32<<10)}
	if r.state == nil {
		return nil, fmt.Errorf("zlib: cannot allocate inflater")
	}
	runtime.SetFinalizer(r, func(r *nativeGzipReader) { C.carbonapi_inflate_free(r.state) })
	if err := r.Reset(src); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *nativeGzipReader) Reset(src io.Reader) error {
	code := C.carbonapi_inflate_reset(r.state)
	runtime.KeepAlive(r)
	if code != C.Z_OK {
		return fmt.Errorf("zlib: reset failed (%d)", int(code))
	}
	r.src = src
	r.pos = 0
	r.end = 0
	r.sourceErr = nil
	r.errorState = nil
	r.memberEnded = false
	return nil
}
func (r *nativeGzipReader) Close() error { r.src = nil; r.errorState = io.ErrClosedPipe; return nil }
func (r *nativeGzipReader) fill() error {
	for attempt := 0; attempt < 100; attempt++ {
		n, err := r.src.Read(r.input)
		r.pos, r.end, r.sourceErr = 0, n, err
		if n != 0 || err != nil {
			return err
		}
	}
	r.sourceErr = io.ErrNoProgress
	return r.sourceErr
}
func (r *nativeGzipReader) fail(err error, n int) (int, error) { r.errorState = err; return n, err }
func (r *nativeGzipReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.errorState != nil {
		return 0, r.errorState
	}
	for {
		if r.memberEnded {
			if r.pos == r.end && r.sourceErr == nil {
				_ = r.fill() // Buffered input is consumed before sourceErr is reported.
			}
			if r.pos == r.end {
				if r.sourceErr == io.EOF {
					return r.fail(io.EOF, 0)
				}
				return r.fail(r.sourceErr, 0)
			}
			if C.carbonapi_inflate_reset(r.state) != C.Z_OK {
				return r.fail(fmt.Errorf("zlib: member reset failed"), 0)
			}
			r.memberEnded = false
		}
		inputSize := r.end - r.pos
		input := r.input[r.pos:r.end]
		result := C.carbonapi_inflate_step(r.state, (*C.uchar)(unsafe.Pointer(unsafe.SliceData(input))), C.uint(inputSize), (*C.uchar)(unsafe.Pointer(unsafe.SliceData(p))), C.uint(min(uint64(len(p)), uint64(^uint32(0)))))
		runtime.KeepAlive(r)
		r.pos += int(result.used)
		n := int(result.produced)
		switch result.code {
		case C.Z_STREAM_END:
			r.memberEnded = true
		case C.Z_OK, C.Z_BUF_ERROR:
		default:
			message := C.GoString(r.state.msg)
			if strings.Contains(message, "incorrect data check") || strings.Contains(message, "incorrect length check") {
				return r.fail(gzip.ErrChecksum, n)
			}
			return r.fail(fmt.Errorf("zlib: %s (%d)", message, int(result.code)), n)
		}
		if n > 0 {
			return n, nil
		}
		if r.memberEnded {
			continue
		}
		if r.pos == r.end {
			if r.sourceErr != nil {
				// Try pending output once with no further input before deciding
				// that EOF truncated the current member.
				if inputSize > 0 {
					continue
				}
				if r.sourceErr == io.EOF {
					return r.fail(io.ErrUnexpectedEOF, 0)
				}
				return r.fail(r.sourceErr, 0)
			}
			_ = r.fill() // Buffered input is consumed before sourceErr is reported.
		} else if result.used == 0 {
			return r.fail(fmt.Errorf("zlib: inflater made no progress"), 0)
		}
	}
}
