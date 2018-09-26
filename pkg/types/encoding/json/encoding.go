/*
Package json defines encoding and decoding methods for Find, Info and Render
responses.
*/
package json

import (
	"encoding/json"
	"math"
	"strconv"

	"github.com/go-graphite/carbonapi/pkg/types"
)

func FindEncoder(matches []types.Match) ([]byte, error) {
	return json.Marshal(matches)
}

func FindDecoder(blob []byte) ([]types.Match, error) {
	matches := make([]types.Match, 0)
	err := json.Unmarshal(blob, &matches)

	return matches, err
}

type jsonInfo struct {
	Name              string
	AggregationMethod string
	MaxRetention      int32
	Retentions        []jsonRet
}

type jsonRet struct {
	SecondsPerPoint int32
	NumberOfPoints  int32
}

func InfoEncoder(infos []types.Info) ([]byte, error) {
	jsonInfos := make(map[string]jsonInfo)

	for _, info := range infos {
		jInfo := jsonInfo{
			Name:              info.Name,
			AggregationMethod: info.AggregationMethod,
			MaxRetention:      info.MaxRetention,
			Retentions:        make([]jsonRet, 0, len(info.Retentions)),
		}

		for _, ret := range info.Retentions {
			jInfo.Retentions = append(jInfo.Retentions, jsonRet{
				SecondsPerPoint: ret.SecondsPerPoint,
				NumberOfPoints:  ret.NumberOfPoints,
			})
		}

		jsonInfos[info.Host] = jInfo
	}

	return json.Marshal(jsonInfos)
}

func InfoDecoder(blob []byte) ([]types.Info, error) {
	jsonInfos := make(map[string]jsonInfo)
	if err := json.Unmarshal(blob, &jsonInfos); err != nil {
		return nil, err
	}

	infos := make([]types.Info, 0, len(jsonInfos))
	for host, info := range jsonInfos {
		inf := types.Info{
			Host:              host,
			Name:              info.Name,
			AggregationMethod: info.AggregationMethod,
			MaxRetention:      info.MaxRetention,
			Retentions:        make([]types.Retention, 0, len(info.Retentions)),
		}

		for _, ret := range info.Retentions {
			inf.Retentions = append(inf.Retentions, types.Retention{
				SecondsPerPoint: ret.SecondsPerPoint,
				NumberOfPoints:  ret.NumberOfPoints,
			})
		}

		infos = append(infos, inf)
	}

	return infos, nil
}

type jsonMetric struct {
	Name       string
	Datapoints [][]string
}

func RenderEncoder(metrics []types.Metric) ([]byte, error) {
	jms := make([]jsonMetric, 0, len(metrics))

	for _, metric := range metrics {
		t := metric.StartTime

		jm := jsonMetric{
			Name:       metric.Name,
			Datapoints: make([][]string, len(metric.Values)),
		}

		for i := range metric.Values {
			epoch := strconv.FormatInt(int64(t), 10)
			var val string
			if metric.IsAbsent[i] || math.IsInf(metric.Values[i], 0) || math.IsNaN(metric.Values[i]) {
				val = "null"
			} else {
				val = strconv.FormatFloat(metric.Values[i], 'f', -1, 64)
			}
			jm.Datapoints[i] = []string{val, epoch}
		}

		jms = append(jms, jm)
		t += metric.StepTime
	}

	return json.Marshal(jms)
}

func RenderDecoder(blob []byte) ([]types.Metric, error) {
	jms := make([]jsonMetric, 0)
	if err := json.Unmarshal(blob, &jms); err != nil {
		return nil, err
	}

	metrics := make([]types.Metric, 0, len(jms))
	for _, jm := range jms {
		metric := types.Metric{
			Name:     jm.Name,
			Values:   make([]float64, len(jm.Datapoints)),
			IsAbsent: make([]bool, len(jm.Datapoints)),
		}

		for i, pair := range jm.Datapoints {
			if i == 0 {
				epoch, err := strconv.ParseInt(pair[1], 10, 32)
				if err != nil {
					return metrics, err
				}
				metric.StartTime = int32(epoch)
			} else if i == len(jm.Datapoints)-1 {
				epoch, err := strconv.ParseInt(pair[1], 10, 32)
				if err != nil {
					return metrics, err
				}
				metric.StopTime = int32(epoch)
			}

			if pair[0] == "null" {
				metric.IsAbsent[i] = true
			} else {
				value, err := strconv.ParseFloat(pair[0], 64)
				if err != nil {
					return metrics, err
				}
				metric.Values[i] = value
			}
		}

		if len(metric.Values) > 0 {
			metric.StepTime = (metric.StopTime - metric.StartTime) / int32(len(metric.Values))
		}

		metrics = append(metrics, metric)
	}

	return metrics, nil
}
