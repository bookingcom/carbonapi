package helper

import (
	"math"
	"testing"
)

func TestExtractTags(t *testing.T) {
	tests := []struct {
		name     string
		metric   string
		expected map[string]string
	}{
		{
			name:   "tagged metric",
			metric: "cpu.usage_idle;cpu=cpu-total;host=test",
			expected: map[string]string{
				"name": "cpu.usage_idle",
				"cpu":  "cpu-total",
				"host": "test",
			},
		},
		{
			name:   "no tags in metric",
			metric: "cpu.usage_idle",
			expected: map[string]string{
				"name": "cpu.usage_idle",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ExtractTags(tt.metric)
			if len(actual) != len(tt.expected) {
				t.Fatalf("amount of tags doesn't match: got %v, expected %v", actual, tt.expected)
			}
			for tag, value := range actual {
				vExpected, ok := tt.expected[tag]
				if !ok {
					t.Fatalf("tag %v not found in %+v", value, actual)
				} else if vExpected != value {
					t.Errorf("unexpected tag-value, got %v, expected %v", value, vExpected)
				}
			}
		})
	}
}

func TestSummarizeValues(t *testing.T) {
	epsilon := math.Nextafter(1, 2) - 1
	tests := []struct {
		name     string
		function string
		values   []float64
		expected float64
	}{
		{
			name:     "no values",
			function: "sum",
			values:   []float64{},
			expected: math.NaN(),
		},
		{
			name:     "sum",
			function: "sum",
			values:   []float64{1, 2, 3},
			expected: 6,
		},
		{
			name:     "sum alias",
			function: "total",
			values:   []float64{1, 2, 3},
			expected: 6,
		},
		{
			name:     "avg",
			function: "avg",
			values:   []float64{1, 2, 3, 4},
			expected: 2.5,
		},
		{
			name:     "max",
			function: "max",
			values:   []float64{1, 2, 3, 4},
			expected: 4,
		},
		{
			name:     "min",
			function: "min",
			values:   []float64{1, 2, 3, 4},
			expected: 1,
		},
		{
			name:     "last",
			function: "last",
			values:   []float64{1, 2, 3, 4},
			expected: 4,
		},
		{
			name:     "range",
			function: "range",
			values:   []float64{1, 2, 3, 4},
			expected: 3,
		},
		{
			name:     "median",
			function: "median",
			values:   []float64{1, 2, 3, 10, 11},
			expected: 3,
		},
		{
			name:     "multiply",
			function: "multiply",
			values:   []float64{1, 2, 3, 4},
			expected: 24,
		},
		{
			name:     "diff",
			function: "diff",
			values:   []float64{1, 2, 3, 4},
			expected: -8,
		},
		{
			name:     "count",
			function: "count",
			values:   []float64{1, 2, 3, 4},
			expected: 4,
		},
		//{
		//name:     "stddev",
		//function: "stddev",
		//values:   []float64{1, 2, 3, 4},
		//expected: 1.118033988749895,
		//},
		{
			name:     "p50 (fallback)",
			function: "p50",
			values:   []float64{1, 2, 3, 10, 11},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := SummarizeValues(tt.function, tt.values)
			if math.Abs(actual-tt.expected) > epsilon {
				t.Errorf("actual %v, expected %v", actual, tt.expected)
			}
		})
	}

}
