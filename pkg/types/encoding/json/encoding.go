/*
Package json defines encoding and decoding methods for Find, Info and Render
responses.
*/
package json

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/go-graphite/carbonapi/pkg/types"
)

type jsonMatch struct {
	AllowChildren int            `json:"allowChildren"`
	Context       map[string]int `json:"context"`
	Expandable    int            `json:"expandable"`
	ID            string         `json:"id"`
	Leaf          int            `json:"leaf"`
	Text          string         `json:"text"`
}

func FindEncoder(matches types.Matches) ([]byte, error) {
	jms := make([]jsonMatch, 0, len(matches.Matches))

	var basepath string
	if i := strings.LastIndex(matches.Name, "."); i != -1 {
		basepath = matches.Name[:i+1]
	}

	for _, m := range matches.Matches {
		name := m.Path
		if i := strings.LastIndex(name, "."); i != -1 {
			name = name[i+1:]
		}

		jm := jsonMatch{
			Text: name,
			ID:   basepath + name,
		}

		if m.IsLeaf {
			jm.Leaf = 1
		} else {
			jm.AllowChildren = 1
		}

		if !m.IsLeaf || strings.ContainsRune(jm.ID, '*') {
			jm.Expandable = 1
		}

		// jm.Context not set on purpose; seems to always be empty map?

		jms = append(jms, jm)
	}

	return json.Marshal(jms)
}

/*
NOTE(gmagnusson): Not implemented because I'm not sure we can decode a JSON
blob in such a way that the roundtrip 'matches -> decode(encode(matches))' is
the identity map, or that the iteration at least stabilizes.

func FindDecoder(blob []byte) ([]types.Match, error) { }
*/

type jsonInfo struct {
	Name              string    `json:"name"`
	AggregationMethod string    `json:"aggregationMethod"`
	MaxRetention      int32     `json:"maxRetention"`
	Retentions        []jsonRet `json:"retentions"`
}

type jsonRet struct {
	SecondsPerPoint int32 `json:"secondsPerPoint"`
	NumberOfPoints  int32 `json:"numberOfPoints"`
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
	Name       string     `json:"name"`
	Datapoints [][]string `json:"datapoints"`
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
