package carbonapi_v2

import (
	"github.com/go-graphite/carbonapi/pkg/types"
)

var Response = response{
	Find: find{
		Marshal:   marshalMatches,
		Unmarshal: unmarshalMatches,
	},
	Info: info{
		Marshal:   marshalInfos,
		Unmarshal: unmarshalInfos,
	},
	Render: render{
		Marshal:   marshalMetrics,
		Unmarshal: unmarshalMetrics,
	},
}

type response struct {
	Find   find
	Info   info
	Render render
}

type find struct {
	Marshal   func([]types.Match) ([]byte, error)
	Unmarshal func([]byte) ([]types.Match, error)
}

type info struct {
	Marshal   func([]types.Info) ([]byte, error)
	Unmarshal func([]byte) ([]types.Info, error)
}

type render struct {
	Marshal   func([]types.Metric) ([]byte, error)
	Unmarshal func([]byte) ([]types.Metric, error)
}

func marshalMatches(matches []types.Match) ([]byte, error) {
	out := Matches{
		Matches: make([]Match, len(matches)),
	}

	for i, match := range matches {
		out.Matches[i] = Match{
			Path:   match.Path,
			IsLeaf: match.IsLeaf,
		}
	}

	return out.Marshal()
}

func unmarshalMatches(blob []byte) ([]types.Match, error) {
	f := Matches{}

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

func marshalInfos(infos []types.Info) ([]byte, error) {
	out := Infos{
		Hosts: make([]string, 0, len(infos)),
		Infos: make([]Info, 0, len(infos)),
	}

	for i, sInfo := range infos {
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

		out.Hosts[i] = sInfo.Host
		out.Infos[i] = info
	}

	return out.Marshal()
}

func unmarshalInfos(blob []byte) ([]types.Info, error) {
	s := Infos{}
	if err := s.Unmarshal(blob); err != nil {
		return nil, err
	}

	infos := make([]types.Info, len(s.Infos))
	for i, sInfo := range s.Infos {
		info := types.Info{
			Host:              s.Hosts[i],
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

func marshalMetrics(metrics []types.Metric) ([]byte, error) {
	out := Metrics{
		Metrics: make([]Metric, len(metrics)),
	}

	for i, m := range metrics {
		metric := Metric{
			Name:      m.Name,
			StartTime: m.StartTime,
			StopTime:  m.StopTime,
			StepTime:  m.StepTime,
			Values:    m.Values,
			IsAbsent:  m.IsAbsent,
		}

		out.Metrics[i] = metric
	}

	return out.Marshal()
}

func unmarshalMetrics(blob []byte) ([]types.Metric, error) {
	resp := Metrics{}
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
