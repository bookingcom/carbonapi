package backend

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/go-graphite/carbonapi/pkg/types"
	"github.com/go-graphite/carbonapi/protobuf/carbonapi_v2"
	"github.com/go-graphite/carbonapi/util"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TODO(gmagnusson): ^ Remove IsAbsent: IsAbsent[i] => Values[i] == NaN
// Doing math on NaN is expensive, but assuming that all functions will treat a
// default value of 0 intelligently is wrong (see multiplication). Thus math
// needs an if IsAbsent[i] check anyway, which is also expensive if we're
// worrying about those levels of performance in the first place.

// Render fetches raw metrics from a backend.
func (b Backend) Render(ctx context.Context, from int32, until int32, targets []string) ([]types.Metric, error) {
	u := b.url("/render")
	u, body := carbonapiV2RenderEncoder(u, from, until, targets)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	ts, err := carbonapiV2RenderDecoder(resp.Body)

	return ts, err
}

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

	if err := checkErrs(ctx, errs, len(backends), backends[0].logger); err != nil {
		return nil, err
	}

	return types.MergeMetrics(msgs), nil
}

func carbonapiV2RenderEncoder(u *url.URL, from int32, until int32, targets []string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("from", fmt.Sprintf("%d", from))
	vals.Set("until", fmt.Sprintf("%d", until))
	for _, target := range targets {
		vals.Add("target", target)
	}
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2RenderDecoder(blob []byte) ([]types.Metric, error) {
	resp := &carbonapi_v2.Metrics{}
	if err := resp.Unmarshal(blob); err != nil {
		return nil, err
	}

	metrics := make([]types.Metric, len(resp.Metrics))
	for i, m := range resp.Metrics {
		metric := types.Metric{
			Name:      m.Name,
			StartTime: m.StartTime,
			StopTime:  m.StopTime,
			StepTime:  m.StepTime,
			Values:    m.Values,
			IsAbsent:  m.IsAbsent,
		}

		/*
			TODO(gmagnusson):
			for j, absent := range metric.IsAbsent {
				if absent {
					t.Values[i] = math.NaN
				}
			}
			and then remove Metric.IsAbsent
		*/

		metrics[i] = metric
	}

	return metrics, nil
}

// Info fetches metadata about a metric from a backend.
func (b Backend) Info(ctx context.Context, metric string) ([]types.Info, error) {
	u := b.url("/info")
	u, body := carbonapiV2InfoEncoder(u, metric)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	infos, err := carbonapiV2InfoDecoder(resp.Body)
	if err != nil {
		return nil, err
	}

	return infos, nil
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

	if err := checkErrs(ctx, errs, len(backends), backends[0].logger); err != nil {
		return nil, err
	}

	return types.MergeInfos(msgs), nil
}

func carbonapiV2InfoEncoder(u *url.URL, metric string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("target", metric)
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2InfoDecoder(blob []byte) ([]types.Info, error) {
	s := &carbonapi_v2.Infos{}
	if err := s.Unmarshal(blob); err != nil {
		return nil, err
	}

	infos := make([]types.Info, len(s.Infos))
	for i, sInfo := range s.Infos {
		info := types.Info{
			Name:              sInfo.Name,
			AggregationMethod: sInfo.AggregationMethod,
			MaxRetention:      sInfo.MaxRetention,
			XFilesFactor:      sInfo.XFilesFactor,
			Retentions:        make([]types.Retention, len(sInfo.Retentions)),
		}
		for j, inf := range sInfo.Retentions {
			info.Retentions[j] = types.Retention{
				SecondsPerPoint: inf.SecondsPerPoint,
				NumberOfPoints:  inf.NumberOfPoints,
			}
		}

		infos[i] = info
	}

	return infos, nil
}

// Find resolves globs and finds metrics in a backend.
func (b Backend) Find(ctx context.Context, query string) ([]types.Match, error) {
	u := b.url("/metrics/find")
	u, body := carbonapiV2FindEncoder(u, query)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	find, err := carbonapiV2FindDecoder(resp.Body)

	return find, err
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

	if err := checkErrs(ctx, errs, len(backends), backends[0].logger); err != nil {
		return nil, err
	}

	return types.MergeMatches(msgs), nil
}

func carbonapiV2FindEncoder(u *url.URL, query string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("query", query)
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2FindDecoder(blob []byte) ([]types.Match, error) {
	f := &carbonapi_v2.Matches{}

	if err := f.Unmarshal(blob); err != nil {
		return nil, err
	}

	matches := make([]types.Match, len(f.Matches))
	for i, match := range f.Matches {
		matches[i] = types.Match{
			Path:   match.Path,
			IsLeaf: match.IsLeaf,
		}
	}

	return matches, nil
}

func checkErrs(ctx context.Context, errs []error, limit int, logger *zap.Logger) error {
	if len(errs) == 0 {
		return nil
	}

	if len(errs) == limit {
		return errors.WithMessage(combineErrors(errs), "All backend requests failed")
	}

	logger.Warn("Some requests failed",
		zap.String("uuid", util.GetUUID(ctx)),
		zap.Error(combineErrors(errs)),
	)

	return nil
}
