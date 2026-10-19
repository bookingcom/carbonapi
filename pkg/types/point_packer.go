package types

import (
	"math"
	"math/bits"
)

// PointPacker builds temporary replica storage without a dense value vector.
// AppendBits preserves every IEEE bit, including absent values and signed zero.
// The builder is consumed once by Finish or DenseValues.
type PointPacker struct {
	values  []uint64
	n, mask int
}

func (p *PointPacker) AppendBits(value uint64) {
	if p.n&63 == 0 {
		p.mask = len(p.values)
		p.values = append(p.values, 0)
	}
	if value != 0 {
		p.values[p.mask] |= uint64(1) << (uint(p.n) & 63)
		p.values = append(p.values, value)
	}
	p.n++
}

func (p *PointPacker) AppendZeros(n int) {
	if remainder := p.n & 63; remainder != 0 {
		count := min(n, 64-remainder)
		p.n += count
		n -= count
	}
	if n == 0 {
		return
	}
	p.values = append(p.values, make([]uint64, (n+63)/64)...)
	p.mask = len(p.values) - 1
	p.n += n
}

// DensePreferred bounds temporary overhead on dense input: after a small
// sample the decoder resumes its ordinary direct-to-array path.
func (p *PointPacker) DensePreferred() bool {
	return p.n >= 512 && len(p.values)-(p.n+63)/64 > p.n/2
}

func (p *PointPacker) DenseValues() []float64 {
	if p.n == 0 {
		return nil
	}
	values := make([]float64, p.n)
	offset := 0
	for start := 0; start < p.n; start += 64 {
		mask := p.values[offset]
		offset++
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			values[start+bit] = math.Float64frombits(p.values[offset])
			offset++
			mask &= mask - 1
		}
	}
	return values
}

// Finish retains packed storage only when it saves at least 25% and matches
// the absence vector. Unequal vectors keep the ordinary decoder's behavior.
func (p *PointPacker) Finish(m *Metric) {
	blocks := (p.n + 63) / 64
	if p.n < 128 || len(m.IsAbsent) != p.n || cap(p.values)*8+blocks*8+64 >= p.n*9*3/4 {
		m.Values = p.DenseValues()
		return
	}
	absent := make([]uint64, blocks)
	for i, value := range m.IsAbsent {
		if value {
			absent[i/64] |= uint64(1) << (uint(i) & 63)
		}
	}
	m.packed = &packedPoints{values: p.values, absent: absent, n: p.n}
	m.Values, m.IsAbsent = nil, nil
}
