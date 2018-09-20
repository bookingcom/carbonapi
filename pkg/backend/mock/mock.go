package mock

import (
	"context"
	"io"
	"net/url"

	"go.uber.org/zap"
)

type Backend struct {
	call func(context.Context, *url.URL, io.Reader) ([]byte, error)
	url  func(string) *url.URL
}

var noLog *zap.Logger = zap.New(nil)

func (b Backend) Call(ctx context.Context, u *url.URL, body io.Reader) ([]byte, error) {
	return b.call(ctx, u, body)
}

func (b Backend) URL(path string) *url.URL {
	return b.url(path)
}

func (b Backend) Logger() *zap.Logger {
	return noLog
}

func New(call func(context.Context, *url.URL, io.Reader) ([]byte, error), url func(string) *url.URL) Backend {
	return Backend{
		call: call,
		url:  url,
	}
}
