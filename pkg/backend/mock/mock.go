/*
Package mock defines a mock backend for testing.

Example use:

	b := New(Config{
		Info: func(context.Context, string) ([]types.Info, error) {
			return nil, errors.New("Oh no")
		}
	})
	got, err := b.Info(ctx, "foo")
*/
package mock

import (
	"context"

	"github.com/bookingcom/carbonapi/pkg/types"

	"go.uber.org/zap"
)

// Backend is a mock backend.
type Backend struct {
	find     func(context.Context, types.FindRequest) (types.Matches, error)
	info     func(context.Context, types.InfoRequest) ([]types.Info, error)
	render   func(context.Context, types.RenderRequest) ([]types.Metric, error)
	contains func([]string) bool
}

// Config configures a mock Backend. Define ad-hoc functions to return
// expected values depending on input. If a function is not defined,
// default to one that returns an empty response and nil error.
// A mock backend contains all targets by default.
type Config struct {
	Find     func(context.Context, types.FindRequest) (types.Matches, error)
	Info     func(context.Context, types.InfoRequest) ([]types.Info, error)
	Render   func(context.Context, types.RenderRequest) ([]types.Metric, error)
	Contains func([]string) bool
}

var (
	noLog      *zap.Logger                                                        = zap.New(nil)
	noFind     func(context.Context, types.FindRequest) (types.Matches, error)    = func(context.Context, types.FindRequest) (types.Matches, error) { return types.Matches{}, nil }
	noInfo     func(context.Context, types.InfoRequest) ([]types.Info, error)     = func(context.Context, types.InfoRequest) ([]types.Info, error) { return nil, nil }
	noRender   func(context.Context, types.RenderRequest) ([]types.Metric, error) = func(context.Context, types.RenderRequest) ([]types.Metric, error) { return nil, nil }
	noContains func([]string) bool                                                = func([]string) bool { return true }
)

func (b Backend) Find(ctx context.Context, request types.FindRequest) (types.Matches, error) {
	return b.find(ctx, request)
}

func (b Backend) Info(ctx context.Context, request types.InfoRequest) ([]types.Info, error) {
	return b.info(ctx, request)
}

func (b Backend) Render(ctx context.Context, request types.RenderRequest) ([]types.Metric, error) {
	return b.render(ctx, request)
}

// Logger returns a no-op logger.
func (b Backend) Logger() *zap.Logger {
	return noLog
}

// Probe is a no-op.
func (b Backend) Probe() {}

// New creates a new mock backend.
func New(cfg Config) Backend {
	b := Backend{}

	if cfg.Find != nil {
		b.find = cfg.Find
	} else {
		b.find = noFind
	}

	if cfg.Info != nil {
		b.info = cfg.Info
	} else {
		b.info = noInfo
	}

	if cfg.Render != nil {
		b.render = cfg.Render
	} else {
		b.render = noRender
	}

	if cfg.Contains != nil {
		b.contains = cfg.Contains
	} else {
		b.contains = noContains
	}

	return b
}

func (b Backend) Contains(targets []string) bool {
	return b.contains(targets)
}
