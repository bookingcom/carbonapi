/*
Package carbonapi_v2 defines encoding and decoding methods for Find, Info and
Render responses.

It uses a modified version 2 of the carbonapi protocol buffer schema that is
compatible with the original one.
*/
package carbonapi_v2

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bookingcom/carbonapi/pkg/types"

	"github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func FindEncoder(matches types.Matches) ([]byte, error) {
	out := carbonapi_v2_pb.GlobResponse{
		Name:    matches.Name,
		Matches: make([]*carbonapi_v2_pb.GlobMatch, len(matches.Matches)),
	}

	for i, match := range matches.Matches {
		out.Matches[i] = &carbonapi_v2_pb.GlobMatch{
			Path:   match.Path,
			IsLeaf: match.IsLeaf,
		}
	}

	return out.MarshalVT()
}

func FindDecoder(blob []byte) (types.Matches, error) {
	f := carbonapi_v2_pb.GlobResponse{}
	if err := f.UnmarshalVT(blob); err != nil {
		return types.Matches{}, err
	}

	matches := types.Matches{
		Name:    f.Name,
		Matches: make([]types.Match, len(f.Matches)),
	}

	for i, match := range f.Matches {
		matches.Matches[i] = types.Match{
			Path:   match.Path,
			IsLeaf: match.IsLeaf,
		}
	}

	return matches, nil
}

func InfoEncoder(infos []types.Info) ([]byte, error) {
	out := carbonapi_v2_pb.ZipperInfoResponse{
		Responses: make([]*carbonapi_v2_pb.ServerInfoResponse, 0, len(infos)),
	}

	for _, sInfo := range infos {
		info := &carbonapi_v2_pb.ServerInfoResponse{
			Server: sInfo.Host,
			Info: &carbonapi_v2_pb.InfoResponse{
				Name:              sInfo.Name,
				AggregationMethod: sInfo.AggregationMethod,
				MaxRetention:      sInfo.MaxRetention,
				XFilesFactor:      sInfo.XFilesFactor,
				Retentions:        make([]*carbonapi_v2_pb.Retention, len(sInfo.Retentions)),
			},
		}
		for j, inf := range sInfo.Retentions {
			info.Info.Retentions[j] = &carbonapi_v2_pb.Retention{
				SecondsPerPoint: inf.SecondsPerPoint,
				NumberOfPoints:  inf.NumberOfPoints,
			}
		}

		out.Responses = append(out.Responses, info)
	}

	return out.MarshalVT()
}

func IsInfoResponse(blob []byte) (bool, error) {
	r := bytes.NewReader(blob)
	fieldToType := make(map[uint64]uint64)

	// Find protobuf field and wire types in message:
	// https://developers.google.com/protocol-buffers/docs/encoding
	for {
		i, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				break
			}

			return false, err
		}

		ptype := i & 3
		pfield := i >> 3

		fieldToType[pfield] = ptype

		if ptype == 2 {
			i, err := binary.ReadUvarint(r)
			if err != nil {
				// Corrupted message
				return false, err
			}

			_, err = r.Seek(int64(i), io.SeekCurrent)
			if err != nil {
				return false, err
			}
		}
	}

	if len(fieldToType) == 1 {
		// Presumably ZipperInfoResponse ¯\_(ツ)_/¯
		return false, nil
	} else if len(fieldToType) == 2 {
		// Presumably ServerInfoResponse
		// NOTE(gmagnusson): We should never get this, because we ship
		// ZipperInfoResponse around
		return false, nil
	}

	return true, nil
}

func MultiInfoDecoder(blob []byte) ([]types.Info, error) {
	s := carbonapi_v2_pb.ZipperInfoResponse{}
	if err := s.UnmarshalVT(blob); err != nil {
		return nil, err
	}

	infos := make([]types.Info, len(s.Responses))
	for i, sInfo := range s.Responses {
		info := types.Info{
			Host:              sInfo.Server,
			Name:              sInfo.Info.Name,
			AggregationMethod: sInfo.Info.AggregationMethod,
			MaxRetention:      sInfo.Info.MaxRetention,
			XFilesFactor:      sInfo.Info.XFilesFactor,
			Retentions:        make([]types.Retention, len(sInfo.Info.Retentions)),
		}
		for j, inf := range sInfo.Info.Retentions {
			info.Retentions[j] = types.Retention{
				SecondsPerPoint: inf.SecondsPerPoint,
				NumberOfPoints:  inf.NumberOfPoints,
			}
		}

		infos[i] = info
	}

	return infos, nil
}

func SingleInfoDecoder(blob []byte, host string) ([]types.Info, error) {
	s := carbonapi_v2_pb.InfoResponse{}
	if err := s.UnmarshalVT(blob); err != nil {
		return nil, err
	}

	info := types.Info{
		Host:              host,
		Name:              s.Name,
		AggregationMethod: s.AggregationMethod,
		MaxRetention:      s.MaxRetention,
		XFilesFactor:      s.XFilesFactor,
		Retentions:        make([]types.Retention, len(s.Retentions)),
	}
	for j, inf := range s.Retentions {
		info.Retentions[j] = types.Retention{
			SecondsPerPoint: inf.SecondsPerPoint,
			NumberOfPoints:  inf.NumberOfPoints,
		}
	}

	return []types.Info{info}, nil
}

func RenderEncoder(metrics []types.Metric) ([]byte, error) {
	out := carbonapi_v2_pb.MultiFetchResponse{
		Metrics: make([]*carbonapi_v2_pb.FetchResponse, len(metrics)),
	}

	for i, m := range metrics {
		metric := &carbonapi_v2_pb.FetchResponse{
			Name:      m.Name,
			StartTime: m.StartTime,
			StopTime:  m.StopTime,
			StepTime:  m.StepTime,
			Values:    m.Values,
			IsAbsent:  m.IsAbsent,
		}

		out.Metrics[i] = metric
	}

	return out.MarshalVT()
}

func RenderDecoder(blob []byte) ([]types.Metric, error) {
	resp := carbonapi_v2_pb.MultiFetchResponse{}
	if err := resp.UnmarshalVT(blob); err != nil {
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
