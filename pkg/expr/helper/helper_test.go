package helper

import (
	"math"
	"testing"
)

func TestPercentile(t *testing.T) {
	input := []struct {
		name        string
		data        []float64
		percent     float64
		interpolate bool
		expected    float64
		absent      bool
	}{
		{"simple", []float64{1, 2, 3}, 50, false, 2, false},
		{"80", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 80, false, 9, false},
	}

	for _, test := range input {
		got, absent := Percentile(test.data, test.percent, test.interpolate)
		if got != test.expected {
			t.Errorf("Expected: %f. Got: %f. Test: %+v", test.expected, got, test)
		}
		if absent != test.absent {
			t.Errorf("Expected absent: %t. Got: %t. Test: %+v", test.absent, absent, test)
		}
	}
}

func TestSummarizeValues(t *testing.T) {
	tests := []struct {
		name     string
		function string
		values   []float64
		absent   []bool
		expected float64
		isAbsent bool
		wantErr  bool
	}{
		{
			name:     "sum with all values",
			function: "sum",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, false, false, false, false},
			expected: 15,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "total with all values",
			function: "total",
			values:   []float64{10, 20, 30},
			absent:   []bool{false, false, false},
			expected: 60,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "avg with all values",
			function: "avg",
			values:   []float64{2, 4, 6, 8},
			absent:   []bool{false, false, false, false},
			expected: 5,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "average with some absent",
			function: "average",
			values:   []float64{10, 20, 30, 40},
			absent:   []bool{false, true, false, false},
			expected: 26.666666666666668,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "max with all values",
			function: "max",
			values:   []float64{1, 5, 3, 9, 2},
			absent:   []bool{false, false, false, false, false},
			expected: 9,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "min with all values",
			function: "min",
			values:   []float64{5, 2, 8, 1, 9},
			absent:   []bool{false, false, false, false, false},
			expected: 1,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "min with some absent",
			function: "min",
			values:   []float64{5, 2, 8, 1, 9},
			absent:   []bool{false, true, false, false, false},
			expected: 1,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "last with all values",
			function: "last",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, false, false, false, false},
			expected: 5,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "last with trailing absent",
			function: "last",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, false, false, false, true},
			expected: 4,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "count with all values",
			function: "count",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, false, false, false, false},
			expected: 5,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "count with some absent",
			function: "count",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, true, false, true, false},
			expected: 3,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "median with odd count",
			function: "median",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, false, false, false, false},
			expected: 3,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "median with even count",
			function: "median",
			values:   []float64{1, 2, 3, 4},
			absent:   []bool{false, false, false, false},
			expected: 2.5,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "stddev with simple values",
			function: "stddev",
			values:   []float64{2, 4, 4, 4, 5, 5, 7, 9},
			absent:   []bool{false, false, false, false, false, false, false, false},
			expected: 2,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "stddev with all same values",
			function: "stddev",
			values:   []float64{5, 5, 5, 5, 5},
			absent:   []bool{false, false, false, false, false},
			expected: 0,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "stddev with some absent",
			function: "stddev",
			values:   []float64{2, 4, 4, 4, 5, 5, 7, 9},
			absent:   []bool{false, true, false, false, false, false, false, false},
			expected: 2.099562636671296,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "empty values",
			function: "sum",
			values:   []float64{},
			absent:   []bool{},
			expected: 0,
			isAbsent: true,
			wantErr:  false,
		},
		{
			name:     "percentile p50",
			function: "p50",
			values:   []float64{1, 2, 3, 4, 5},
			absent:   []bool{false, false, false, false, false},
			expected: 3,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "percentile p95",
			function: "p95",
			values:   []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			absent:   []bool{false, false, false, false, false, false, false, false, false, false},
			expected: 9.55,
			isAbsent: false,
			wantErr:  false,
		},
		{
			name:     "unsupported function",
			function: "invalid",
			values:   []float64{1, 2, 3},
			absent:   []bool{false, false, false},
			expected: 0,
			isAbsent: true,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isAbsent, err := SummarizeValues(tt.function, tt.values, tt.absent)

			if (err != nil) != tt.wantErr {
				t.Errorf("SummarizeValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if isAbsent != tt.isAbsent {
				t.Errorf("SummarizeValues() isAbsent = %v, want %v", isAbsent, tt.isAbsent)
			}

			if !tt.isAbsent && !almostEqual(got, tt.expected) {
				t.Errorf("SummarizeValues() got = %v, want %v", got, tt.expected)
			}
		})
	}
}

// almostEqual checks if two float64 values are approximately equal
func almostEqual(a, b float64) bool {
	const epsilon = 1e-9
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}
	if math.IsInf(a, -1) && math.IsInf(b, -1) {
		return true
	}
	return math.Abs(a-b) < epsilon
}
