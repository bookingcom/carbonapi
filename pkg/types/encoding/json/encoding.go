/*
Package json defines encoding and decoding methods for Find, Info and Render
responses.
*/
package json

import (
	"encoding/json"
	"math"
	"sort"
	"strings"

	"github.com/bookingcom/carbonapi/pkg/types"

	"github.com/pkg/errors"
)

type jsonMatch struct {
	AllowChildren int            `json:"allowChildren"`
	Context       map[string]int `json:"context"`
	Expandable    int            `json:"expandable"`
	ID            string         `json:"id"`
	Leaf          int            `json:"leaf"`
	Text          string         `json:"text"`
}

// FindEncoder converts matches to JSON data
func FindEncoder(matches types.Matches) ([]byte, error) {
	jms := matchesToJSONMatches(matches)

	return json.Marshal(jms)
}

func matchesToJSONMatches(matches types.Matches) []jsonMatch {
	// values are stored in the map first to remove duplicates by ID
	ms := make(map[string]jsonMatch)

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
			Text:    name,
			ID:      basepath + name,
			Context: make(map[string]int),
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

		ms[jm.ID] = jm
	}

	jms := make([]jsonMatch, 0, len(ms))
	for _, jm := range ms {
		jms = append(jms, jm)
	}

	sort.Slice(jms, func(i, j int) bool {
		return jms[i].Text < jms[j].Text
	})

	return jms
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
	XFilesFactor      float32   `json:"xFilesFactor"`
	Retentions        []jsonRet `json:"retentions"`
}

type jsonRet struct {
	SecondsPerPoint int32 `json:"secondsPerPoint"`
	NumberOfPoints  int32 `json:"numberOfPoints"`
}

// InfoEncoder converts acquired info data to JSON string
func InfoEncoder(infos []types.Info) ([]byte, error) {
	jsonInfos := make(map[string]jsonInfo)

	for _, info := range infos {
		jInfo := jsonInfo{
			Name:              info.Name,
			AggregationMethod: info.AggregationMethod,
			MaxRetention:      info.MaxRetention,
			XFilesFactor:      info.XFilesFactor,
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

// InfoDecoder converts JSON string to metrics info
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
			XFilesFactor:      info.XFilesFactor,
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
	Name       string          `json:"name"`
	Datapoints [][]interface{} `json:"datapoints"`
}

// RenderEncoder converts metrics data to JSON format
func RenderEncoder(metrics []types.Metric) ([]byte, error) {
	jms := make([]jsonMetric, 0, len(metrics))

	for _, metric := range metrics {
		t := metric.StartTime

		jm := jsonMetric{
			Name:       metric.Name,
			Datapoints: make([][]interface{}, len(metric.Values)),
		}

		for i := range metric.Values {
			data := make([]interface{}, 2)

			if metric.IsAbsent[i] || math.IsInf(metric.Values[i], 0) || math.IsNaN(metric.Values[i]) {
				data[0] = nil
			} else {
				data[0] = metric.Values[i]
			}

			data[1] = t
			jm.Datapoints[i] = data

			t += metric.StepTime
		}

		jms = append(jms, jm)
	}

	return json.Marshal(jms)
}

// RenderDecoder converts JSON string to metrics data
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
				epoch, ok := pair[1].(int32)
				if !ok {
					return metrics, errors.Errorf("Expected integer epoch, got '%v'", pair[1])
				}
				metric.StartTime = epoch
			} else if i == len(jm.Datapoints)-1 {
				epoch, ok := pair[1].(int32)
				if !ok {
					return metrics, errors.Errorf("Expected integer epoch, got '%v'", pair[1])
				}
				metric.StopTime = epoch
			}

			str, ok := pair[0].(string)
			if ok {
				if str == "null" {
					metric.IsAbsent[i] = true
					continue
				} else {
					return metrics, errors.Errorf("Invalid string value '%s' in JSON", str)
				}
			}

			value, ok := pair[0].(float64)
			if !ok {
				return metrics, errors.Errorf("Expected float64 in metric, got '%v'", pair[0])
			}
			metric.Values[i] = value
		}

		if len(metric.Values) > 0 {
			metric.StepTime = (metric.StopTime - metric.StartTime) / int32(len(metric.Values))
		}

		metrics = append(metrics, metric)
	}

	return metrics, nil
}
