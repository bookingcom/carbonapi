package net

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/bookingcom/carbonapi/pkg/types/encoding/carbonapi_v2"
	"github.com/pkg/errors"
)

type renderInflater interface {
	io.ReadCloser
	Reset(io.Reader) error
}

var nativeInflaters sync.Pool

func acquireRenderInflater(src io.Reader, native bool) (renderInflater, error) {
	if native {
		if r, ok := nativeInflaters.Get().(renderInflater); ok {
			if err := r.Reset(src); err != nil {
				return nil, err
			}
			return r, nil
		}
		return newNativeInflater(src)
	}
	if r, ok := gzipPool.Get().(*gzip.Reader); ok {
		if err := r.Reset(src); err != nil {
			return nil, err
		}
		return r, nil
	}
	return gzip.NewReader(src)
}
func releaseRenderInflater(r renderInflater, native bool) {
	r.Close()
	if native {
		nativeInflaters.Put(r)
	} else {
		gzipPool.Put(r)
	}
}

type trackedRenderReader struct {
	io.Reader
	readError error
}

func (r *trackedRenderReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && err != io.EOF {
		r.readError = err
	}
	return n, err
}

// RenderPacked is used only by the backend replica fan-in. Public Render keeps
// its materialized-slice contract for standalone transport consumers.
func (b NetBackend) RenderPacked(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	if !b.streamRender || !b.packRender {
		return b.Render(ctx, request)
	}
	u := carbonapiV2RenderEncoder(b.url("/render/"), request.From, request.Until, request.Targets)
	return b.renderStreamPoints(ctx, u, true)
}

func (b NetBackend) renderStream(ctx context.Context, u *url.URL) ([]types.Metric, error) {
	return b.renderStreamPoints(ctx, u, false)
}

func (b NetBackend) renderStreamPoints(ctx context.Context, u *url.URL, packed bool) ([]types.Metric, error) {
	ctx, cancel := b.setTimeout(ctx)
	defer cancel()
	req, err := b.request(ctx, u)
	if err != nil {
		return nil, err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		if b.responsesCount != nil {
			b.responsesCount.WithLabelValues("http_client_error", "render").Inc()
		}
		return nil, err
	}
	defer resp.Body.Close()
	count := func(status string) {
		if b.responsesCount != nil {
			b.responsesCount.WithLabelValues(status, "render").Inc()
		}
	}
	contentType := resp.Header.Get("Content-Type")
	if resp.StatusCode != http.StatusOK || (contentType != "application/x-protobuf" && contentType != "application/protobuf" && contentType != "application/octet-stream") {
		body, err := readBody(resp)
		defer body.release()
		if err != nil {
			count("http_body_error")
			return nil, err
		}
		count(strconv.Itoa(resp.StatusCode))
		if resp.StatusCode == http.StatusNotFound {
			return nil, types.ErrMetricsNotFound
		}
		if resp.StatusCode != http.StatusOK {
			return nil, ErrHTTPCode(resp.StatusCode)
		}
		if contentType == "application/text" {
			return nil, errors.Errorf("Unexpected application/text response:\n%s", string(body.data))
		}
		return nil, errors.Errorf("Unknown content type '%s'", contentType)
	}
	var source io.Reader = resp.Body
	var inflater renderInflater
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		inflater, err = acquireRenderInflater(source, b.nativeGzip)
		if err != nil {
			count("http_body_error")
			return nil, err
		}
		source = inflater
	}
	tracked := &trackedRenderReader{Reader: source}
	decode := carbonapi_v2.RenderStreamDecoder
	if packed {
		decode = carbonapi_v2.RenderStreamDecoderPacked
	}
	metrics, decodeErr := decode(tracked)
	// Validate the complete HTTP/gzip stream even when protobuf decoding stops
	// early, retaining CRC, truncation and connection reuse behavior.
	if decodeErr != nil {
		_, _ = io.Copy(io.Discard, tracked)
	}
	if tracked.readError != nil {
		count("http_body_error")
		return nil, tracked.readError
	}
	if inflater != nil {
		releaseRenderInflater(inflater, b.nativeGzip)
	}
	count("200")
	if decodeErr != nil {
		return nil, errors.Wrap(decodeErr, "Unmarshal failed")
	}
	if len(metrics) == 0 {
		return nil, types.ErrMetricsNotFound
	}
	b.cache.setAll(len(metrics), func(i int) string { return metrics[i].Name })
	return metrics, nil
}
