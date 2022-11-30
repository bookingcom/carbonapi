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
