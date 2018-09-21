package backend

import (
	"context"

	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TODO(gmagnusson): ^ Remove IsAbsent: IsAbsent[i] => Values[i] == NaN
// Doing math on NaN is expensive, but assuming that all functions will treat a
// default value of 0 intelligently is wrong (see multiplication). Thus math
// needs an if IsAbsent[i] check anyway, which is also expensive if we're
// worrying about those levels of performance in the first place.

// Renders makes Render calls to multiple backends.
func Renders(ctx context.Context, backends []Backend, from int32, until int32, targets []string) ([]types.Metric, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Metric, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		go func(b Backend) {
			msg, err := b.Render(ctx, from, until, targets)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([][]types.Metric, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if err := checkErrs(ctx, errs, len(backends), backends[0].Logger()); err != nil {
		return nil, err
	}

	return types.MergeMetrics(msgs), nil
}

// Infos makes Info calls to multiple backends.
func Infos(ctx context.Context, backends []Backend, metric string) ([]types.Info, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Info, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		go func(b Backend) {
			msg, err := b.Info(ctx, metric)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([][]types.Info, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if err := checkErrs(ctx, errs, len(backends), backends[0].Logger()); err != nil {
		return nil, err
	}

	return types.MergeInfos(msgs), nil
}

// Finds makes Find calls to multiple backends.
func Finds(ctx context.Context, backends []Backend, query string) ([]types.Match, error) {
	if len(backends) == 0 {
		return nil, nil
	}

	msgCh := make(chan []types.Match, len(backends))
	errCh := make(chan error, len(backends))
	for _, backend := range backends {
		go func(b Backend) {
			msg, err := b.Find(ctx, query)
			if err != nil {
				errCh <- err
			} else {
				msgCh <- msg
			}
		}(backend)
	}

	msgs := make([][]types.Match, 0, len(backends))
	errs := make([]error, 0, len(backends))
	for i := 0; i < len(backends); i++ {
		select {
		case msg := <-msgCh:
			msgs = append(msgs, msg)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if err := checkErrs(ctx, errs, len(backends), backends[0].Logger()); err != nil {
		return nil, err
	}

	return types.MergeMatches(msgs), nil
}

func checkErrs(ctx context.Context, errs []error, limit int, logger *zap.Logger) error {
	if len(errs) == 0 {
		return nil
	}

	if len(errs) >= limit {
		return errors.WithMessage(combineErrors(errs), "All backend requests failed")
	}

	logger.Warn("Some requests failed",
		zap.String("uuid", util.GetUUID(ctx)),
		zap.Error(combineErrors(errs)),
	)

	return nil
}
