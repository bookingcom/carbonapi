package backend

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/go-graphite/protocol/carbonapi_v2_pb"
)

type Timeseries struct {
	Name      string
	StartTime int32
	StopTime  int32
	StepTime  int32
	Values    []float64
	IsAbsent  []bool
}

// TODO(gmagnusson): ^ Remove IsAbsent

// Render fetches raw metrics from a backend.
func (b Backend) Render(ctx context.Context, from int32, until int32, metrics []string) ([]Timeseries, error) {
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

func carbonapiV2RenderDecoder(blob []byte) ([]Timeseries, error) {
	mfr := &carbonapi_v2_pb.MultiFetchResponse{}
	if err := mfr.Unmarshal(blob); err != nil {
		return nil, err
	}

	ts := make([]Timeseries, len(mfr.Metrics))
	for i, metric := range mfr.Metrics {
		t := Timeseries{
			Name:      metric.Name,
			StartTime: metric.StartTime,
			StopTime:  metric.StopTime,
			StepTime:  metric.StepTime,
			Values:    metric.Values,
			IsAbsent:  metric.IsAbsent,
		}

		/*
			TODO(gmagnusson):
			for j, absent := range metric.IsAbsent {
				if absent {
					t.Values[i] = math.NaN
				}
			}
			and then remove Timeseries.IsAbsent
		*/

		ts[i] = t
	}

	return nil, nil
}

type Info struct {
	Name              string
	AggregationMethod string
	MaxRetention      int32
	XFilesFactor      float32
	Retentions        []Retention
}

type Retention struct {
	SecondsPerPoint int32
	NumberOfPoints  int32
}

// Info fetches detailed information about a metric from a backend.
func (b Backend) Info(ctx context.Context, metric string) (Info, error) {
	// TODO(gmagnusson): Needs to return []Info or map[string]Info
	u := b.url("/info")
	u, body := carbonapiV2InfoEncoder(u, metric)

	resp, err := b.Call(ctx, u, body)
	if err != nil {
		return Info{}, err
	}

	info, err := carbonapiV2InfoDecoder(resp.Body)

	return info, err
}

func carbonapiV2InfoEncoder(u *url.URL, metric string) (*url.URL, io.Reader) {
	vals := url.Values{}
	vals.Set("target", metric)
	u.RawQuery = vals.Encode()

	return u, nil
}

func carbonapiV2InfoDecoder(blob []byte) (Info, error) {
	s := &carbonapi_v2_pb.ServerInfoResponse{}
	if err := s.Unmarshal(blob); err != nil {
		return Info{}, err
	}

	info := Info{
		Name:              s.Info.Name,
		AggregationMethod: s.Info.AggregationMethod,
		MaxRetention:      s.Info.MaxRetention,
		XFilesFactor:      s.Info.XFilesFactor,
		Retentions:        make([]Retention, len(s.Info.Retentions)),
	}
	for i, inf := range s.Info.Retentions {
		info.Retentions[i] = Retention{
			SecondsPerPoint: inf.SecondsPerPoint,
			NumberOfPoints:  inf.NumberOfPoints,
		}
	}

	return info, nil
}

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
	f := &carbonapi_v2_pb.GlobResponse{}

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
