package backend

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/go-graphite/carbonapi/protobuf/carbonapi_v2"
)

// Metric represents a part of a time series.
type Metric struct {
	Name      string
	StartTime int32
	StopTime  int32
	StepTime  int32
	Values    []float64
	IsAbsent  []bool
}

// TODO(gmagnusson): ^ Remove IsAbsent: IsAbsent[i] => Values[i] == NaN
// Doing math on NaN is expensive, but assuming that all functions will treat a
// default value of 0 intelligently is wrong (see multiplication). Thus math
// needs an if IsAbsent[i] check anyway, which is also expensive if we're
// worrying about those levels of performance in the first place.

// Render fetches raw metrics from a backend.
func (b Backend) Render(ctx context.Context, from int32, until int32, metrics []string) ([]Metric, error) {
	u := b.url("/render")
	u, body := carbonapiV2RenderEncoder(u, from, until, metrics)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	ts, err := carbonapiV2RenderDecoder(resp.Body)

	return ts, err
}

func carbonapiV2RenderEncoder(u *url.URL, from int32, until int32, metrics []string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("from", fmt.Sprintf("%d", from))
	vals.Set("until", fmt.Sprintf("%d", until))
	for _, name := range metrics {
		vals.Add("target", name)
	}
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2RenderDecoder(blob []byte) ([]Metric, error) {
	resp := &carbonapi_v2.Metrics{}
	if err := resp.Unmarshal(blob); err != nil {
		return nil, err
	}

	metrics := make([]Metric, len(resp.Metrics))
	for i, m := range resp.Metrics {
		metric := Metric{
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

	return nil, nil
}

// Info contains metadata about a metric in Graphite.
type Info struct {
	Name              string
	AggregationMethod string
	MaxRetention      int32
	XFilesFactor      float32
	Retentions        []Retention
}

// Retention is the Graphite retention schema for a metric archive.
type Retention struct {
	SecondsPerPoint int32
	NumberOfPoints  int32
}

// Info fetches metadata about a metric from a backend.
func (b Backend) Info(ctx context.Context, metric string) ([]Info, error) {
	// TODO(gmagnusson): Needs to return []Info or map[string]Info
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

func carbonapiV2InfoEncoder(u *url.URL, metric string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("target", metric)
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2InfoDecoder(blob []byte) ([]Info, error) {
	s := &carbonapi_v2.Infos{}
	if err := s.Unmarshal(blob); err != nil {
		return nil, err
	}

	infos := make([]Info, len(s.Infos))
	for i, sInfo := range s.Infos {
		info := Info{
			Name:              sInfo.Name,
			AggregationMethod: sInfo.AggregationMethod,
			MaxRetention:      sInfo.MaxRetention,
			XFilesFactor:      sInfo.XFilesFactor,
			Retentions:        make([]Retention, len(sInfo.Retentions)),
		}
		for j, inf := range sInfo.Retentions {
			info.Retentions[j] = Retention{
				SecondsPerPoint: inf.SecondsPerPoint,
				NumberOfPoints:  inf.NumberOfPoints,
			}
		}

		infos[i] = info
	}

	return infos, nil
}

// Match describes a glob match from a Graphite store.
type Match struct {
	Path   string
	IsLeaf bool
}

// Find resolves globs and finds metrics in a backend.
func (b Backend) Find(ctx context.Context, query string) ([]Match, error) {
	u := b.url("/metrics/find")
	u, body := carbonapiV2FindEncoder(u, query)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return nil, err
	}

	find, err := carbonapiV2FindDecoder(resp.Body)

	return find, err
}

func carbonapiV2FindEncoder(u *url.URL, query string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("query", query)
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2FindDecoder(blob []byte) ([]Match, error) {
	f := &carbonapi_v2.Matches{}

	if err := f.Unmarshal(blob); err != nil {
		return nil, err
	}

	matches := make([]Match, len(f.Matches))
	for i, match := range f.Matches {
		matches[i] = Match{
			Path:   match.Path,
			IsLeaf: match.IsLeaf,
		}
	}

	return matches, nil
}
