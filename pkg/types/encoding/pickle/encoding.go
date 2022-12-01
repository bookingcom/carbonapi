// Package pickle defines encoding methods for Find and Render responses.
// The package does not define decoding methods or methods for handling Info
// responses:
//   - The /info endpoint is a carbonapi invention. It's unlikely that any Python
//     stack will know about it.
//   - For now, we do not expect to talk to Python stores. Patches to let us do that
//     are welcome.
package pickle

import (
	"bytes"
	"time"

	"github.com/bookingcom/carbonapi/pkg/intervalset"
	"github.com/bookingcom/carbonapi/pkg/types"

	pickle "github.com/lomik/og-rek"
)

// FindEncoderV0_9 encodes a Find response in a format that graphite-web 0.9.x
// can understand.
func FindEncoderV0_9(matches types.Matches) ([]byte, error) {
	// Used to live in cmd/carbonapi/main.go
	var result []map[string]interface{}
	for _, m := range matches.Matches {
		mm := map[string]interface{}{
			"metric_path": m.Path,
			"isLeaf":      m.IsLeaf,
		}
		result = append(result, mm)
	}

	var buf bytes.Buffer
	penc := pickle.NewEncoder(&buf)
	err := penc.Encode(result)

	return buf.Bytes(), err
}

// FindEncoderV1_0 encodes a Find response in a format that graphite-web 0.1
// can understand.
func FindEncoderV1_0(matches types.Matches) ([]byte, error) {
	// Used to live in cmd/carbonapi/main.go
	now := int32(time.Now().Unix() + 60)
	interval := &intervalset.IntervalSet{Start: 0, End: now}

	var result []map[string]interface{}
	for _, m := range matches.Matches {
		mm := map[string]interface{}{
			"is_leaf":   m.IsLeaf,
			"path":      m.Path,
			"intervals": interval,
		}
		result = append(result, mm)
	}

	var buf bytes.Buffer
	penc := pickle.NewEncoder(&buf)
	err := penc.Encode(result)

	return buf.Bytes(), err
}

/* TODO(gmagnusson)
func FindDecoder(blob []byte) ([]types.Match, error) {
}
*/

// RenderEncoder encodes a Render response in a format graphite-web can understand.
func RenderEncoder(metrics []types.Metric) ([]byte, error) {
	// NOTE(gmagnusson): 100% copy-pasted from expr/types/types.go
	var p []map[string]interface{}

	for _, metric := range metrics {
		values := make([]interface{}, len(metric.Values))
		for i, v := range metric.Values {
			if metric.IsAbsent[i] {
				values[i] = pickle.None{}
			} else {
				values[i] = v
			}

		}
		p = append(p, map[string]interface{}{
			"name":   metric.Name,
			"start":  metric.StartTime,
			"end":    metric.StopTime,
			"step":   metric.StepTime,
			"values": values,
		})
	}

	var buf bytes.Buffer
	penc := pickle.NewEncoder(&buf)
	err := penc.Encode(p)

	return buf.Bytes(), err
}

/* TODO(gmagnusson)
func RenderDecoder(blob []byte) ([]types.Metric, error) {
}
*/

/*
NOT TODO(gmagnusson)
func InfoEncoder(infos []types.Info) ([]byte, error) {
}

func InfoDecoder(blob []byte) ([]types.Info, error) {
}
*/
