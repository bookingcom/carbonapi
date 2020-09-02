package types

import (
	"math"
	"testing"
	"time"

	"github.com/bookingcom/carbonapi/pkg/types"
	"github.com/google/go-cmp/cmp"
)

func TestMarshalCSVInUTC(t *testing.T) {
	results := []*MetricData{
		&MetricData{
			Metric: types.Metric{
				Name:      "foo",
				StartTime: 0,
				StopTime:  1,
				StepTime:  1,
				Values:    []float64{2},
				IsAbsent:  []bool{false},
			},
		},
	}

	blob := MarshalCSV(results, time.UTC)
	got := string(blob)

	exp := "\"foo\",1970-01-01 00:00:00,2\n"

	if got != exp {
		t.Errorf("Expected '%s', got '%s'", exp, got)
	}
}

func TestMarshalCSVNotInUTC(t *testing.T) {
	results := []*MetricData{
		&MetricData{
			Metric: types.Metric{
				Name:      "foo",
				StartTime: 0,
				StopTime:  1,
				StepTime:  1,
				Values:    []float64{2},
				IsAbsent:  []bool{false},
			},
		},
	}

	tz := time.FixedZone("UTC+1", int(time.Hour/time.Second))
	blob := MarshalCSV(results, tz)
	got := string(blob)

	exp := "\"foo\",1970-01-01 01:00:00,2\n"

	if got != exp {
		t.Errorf("Expected '%s', got '%s'", exp, got)
	}
}

func TestConsolidate(t *testing.T) {
	tests := []struct {
		name           string
		input          *MetricData
		valuesPerPoint int
		expected       *MetricData
		expectedEnd    int32
		aggregation    func([]float64, []bool) (float64, bool)
	}{
		{"no consolidation",
			MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, 0),
			1,
			MakeMetricData("metric1", []float64{1, math.NaN(), math.NaN(), 3, 4, 12}, 1, 0),
			6,
			nil,
		},
		{"valuesPerPoint_2_none_values",
			MakeMetricData("metric1", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}, 1, 0),
			2,
			MakeMetricData("metric1", []float64{math.NaN(), math.NaN(), math.NaN()}, 2, 0),
			5,
			nil,
		},
		{"valuesPerPoint_2_some_none_values",
			MakeMetricData("metric1", []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1, 2, 3, 4}, 1, 0),
			2,
			MakeMetricData("metric1", []float64{math.NaN(), math.NaN(), 1, 2.5, 4}, 2, 0),
			9,
			nil,
		},
		{"valuesPerPoint_2_average",
			MakeMetricData("metric1", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1, 0),
			2,
			MakeMetricData("metric1", []float64{0.5, 2.5, 4.5, 6.5, 8.5, 10}, 2, 0),
			11,
			nil,
		},
		{"valuesPerPoint_2_max",
			MakeMetricData("metric1", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1, 0),
			2,
			MakeMetricData("metric1", []float64{1, 3, 5, 7, 9, 10}, 2, 0),
			11,
			AggMax,
		},
		{"valuesPerPoint_2_min",
			MakeMetricData("metric1", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1, 0),
			2,
			MakeMetricData("metric1", []float64{0, 2, 4, 6, 8, 10}, 2, 0),
			11,
			AggMin,
		},
	}

	for _, test := range tests {
		if test.aggregation != nil {
			test.input.AggregateFunction = test.aggregation
		}
		got := test.input.Consolidate(test.valuesPerPoint)
		if diff := cmp.Diff(test.expected.Values, got.Values); diff != "" {
			t.Errorf("Consolidation Values for %s (-want +got):\n%s", test.name, diff)
		}
		if diff := cmp.Diff(test.expected.IsAbsent, got.IsAbsent); diff != "" {
			t.Errorf("Consolidation IsAbsent for %s (-want +got):\n%s", test.name, diff)
		}
		if test.valuesPerPoint != got.ValuesPerPoint {
			t.Errorf("Consolidation ValuesPerPoint for %s. Want: %d. Got: %d", test.name, test.valuesPerPoint, got.ValuesPerPoint)
		}
		if test.expectedEnd != got.StopTime {
			t.Errorf("Consolidation StopTime for %s. Want: %d. Got: %d", test.name, test.expectedEnd, got.StopTime)
		}
		if test.expected.StepTime != got.StepTime {
			t.Errorf("Consolidation StepTime for %s. Want: %d. Got: %d", test.name, test.expected.StepTime, got.StepTime)
		}
		if test.expected.StartTime != got.StartTime {
			t.Errorf("Consolidation StartTime for %s. Want: %d. Got: %d", test.name, test.expected.StartTime, got.StartTime)
		}

	}
}
