package helper

import (
	"testing"
)

func TestPercentile(t *testing.T) {
	input := []struct {
		name        string
		data        []float64
		percent     float64
		interpolate bool
		expected    float64
	}{
		{"simple", []float64{1, 2, 3}, 50, false, 2},
		{"80", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 80, false, 9},
	}

	for _, test := range input {
		got := Percentile(test.data, test.percent, test.interpolate)
		if got != test.expected {
			t.Errorf("Expected: %f. Got: %f. Test: %+v", test.expected, got, test)
		}
	}
}
