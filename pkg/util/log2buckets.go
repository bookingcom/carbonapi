package util

const (
	StepSize = 50
)

// Bucket finds the number of the bucket that 'n' falls into out of 'buckets'
// buckets.
func Bucket(n int64, buckets int) int {
	var bucket int

	for bucket = 0; bucket < buckets; bucket++ {
		upper := int64(StepSize * (1 << uint(bucket)))
		if upper > n {
			break
		}
	}

	return bucket
}

// Bounds returns the lower and upper bounds of bucket number 'bucket'.
func Bounds(bucket int) (lower, upper int) {
	if bucket == 0 {
		lower = 0
	} else {
		lower = StepSize * (1 << (uint(bucket) - 1))
	}

	upper = StepSize * (1 << uint(bucket))

	return lower, upper
}
