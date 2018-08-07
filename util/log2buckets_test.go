package util

import (
	"testing"
)

func TestBucket(t *testing.T) {
	type Test struct {
		Input    int64
		Buckets  int
		Expected int
	}

	tests := []Test{
		Test{
			Input:    0,
			Buckets:  1,
			Expected: 0,
		},
		Test{
			Input:    1000,
			Buckets:  1,
			Expected: 1,
		},
		Test{
			Input:    0,
			Buckets:  4,
			Expected: 0,
		},
		Test{
			Input:    49,
			Buckets:  4,
			Expected: 0,
		},
		Test{
			Input:    50,
			Buckets:  4,
			Expected: 1,
		},
		Test{
			Input:    99,
			Buckets:  4,
			Expected: 1,
		},
		Test{
			Input:    100,
			Buckets:  4,
			Expected: 2,
		},
		Test{
			Input:    199,
			Buckets:  4,
			Expected: 2,
		},
		Test{
			Input:    200,
			Buckets:  4,
			Expected: 3,
		},
		Test{
			Input:    399,
			Buckets:  4,
			Expected: 3,
		},
		Test{
			Input:    400,
			Buckets:  4,
			Expected: 4,
		},
		Test{
			Input:    800,
			Buckets:  4,
			Expected: 4,
		},
	}

	for _, test := range tests {
		got := Bucket(test.Input, test.Buckets)
		if got != test.Expected {
			t.Fatalf("Input: %v\nGot: %d", test, got)
		}
	}
}

func TestBounds(t *testing.T) {
	type Test struct {
		Input int
		Lower int
		Upper int
	}

	tests := []Test{
		Test{
			Input: 0,
			Lower: 0,
			Upper: 50,
		},
		Test{
			Input: 1,
			Lower: 50,
			Upper: 100,
		},
		Test{
			Input: 2,
			Lower: 100,
			Upper: 200,
		},
	}

	for _, test := range tests {
		lower, upper := Bounds(test.Input)
		if lower != test.Lower || upper != test.Upper {
			t.Fatalf("Input: %v\nGot: [%d, %d]", test, lower, upper)
		}
	}
}
