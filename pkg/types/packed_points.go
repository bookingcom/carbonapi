package types

import (
	"math"
	"math/bits"
)

// packedPoints is temporary transport storage. Each block stores a bitmap of
// nonzero IEEE 754 bit patterns followed by those patterns; absence uses a
// separate bitmap. It preserves negative zero, NaN payloads and absent values.
// Expression evaluation always receives ordinary, independently owned slices.
type packedPoints struct {
	values, absent []uint64
	n              int
}

// PackPoints compacts an owned backend reply while it waits for replica merge.
// Dense and short series keep their existing representation.
func (m *Metric) PackPoints() {
	n := len(m.Values)
	if n < 128 || len(m.IsAbsent) != n || m.packed != nil {
		return
	}
	nonzero := 0
	for _, v := range m.Values {
		if math.Float64bits(v) != 0 {
			nonzero++
		}
	}
	blocks := (n + 63) / 64
	if (nonzero+2*blocks)*8+64 >= n*9*3/4 {
		return
	}
	p := &packedPoints{values: make([]uint64, 0, nonzero+blocks), absent: make([]uint64, blocks), n: n}
	for start := 0; start < n; start += 64 {
		index := len(p.values)
		p.values = append(p.values, 0)
		var mask, absent uint64
		for i, v := range m.Values[start:min(start+64, n)] {
			if value := math.Float64bits(v); value != 0 {
				mask |= uint64(1) << i
				p.values = append(p.values, value)
			}
			if m.IsAbsent[start+i] {
				absent |= uint64(1) << i
			}
		}
		p.values[index] = mask
		p.absent[start/64] = absent
	}
	m.packed, m.Values, m.IsAbsent = p, nil, nil
}

func (m *Metric) materializePoints() {
	p := m.packed
	if p == nil {
		return
	}
	m.Values, m.IsAbsent = make([]float64, p.n), make([]bool, p.n)
	offset := 0
	for start := 0; start < p.n; start += 64 {
		mask := p.values[offset]
		offset++
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			m.Values[start+bit] = math.Float64frombits(p.values[offset])
			offset++
			mask &= mask - 1
		}
		absent := p.absent[start/64]
		for absent != 0 {
			bit := bits.TrailingZeros64(absent)
			m.IsAbsent[start+bit] = true
			absent &= absent - 1
		}
	}
	m.packed = nil
}

// packedPointCursor follows monotone point indices, including skipped blocks
// when a complete preferred replica makes later replicas unnecessary.
// Losing replicas are read in place instead of expanding dense temporary arrays.
type packedPointCursor struct{ block, offset, next int }

func (m Metric) pointCount() int {
	if m.packed != nil {
		return m.packed.n
	}
	return len(m.Values)
}

// lastPresentTime returns the timestamp of the newest present point, or
// math.MinInt64 when every point is absent.
func (m Metric) lastPresentTime() int64 {
	last := -1
	if p := m.packed; p != nil {
		for block := len(p.absent) - 1; block >= 0 && last < 0; block-- {
			present := ^p.absent[block]
			if valid := p.n - block*64; valid < 64 {
				present &= uint64(1)<<uint(valid) - 1
			}
			if present != 0 {
				last = block*64 + 63 - bits.LeadingZeros64(present)
			}
		}
	} else {
		for i := len(m.IsAbsent) - 1; i >= 0; i-- {
			if !m.IsAbsent[i] {
				last = i
				break
			}
		}
	}
	if last < 0 {
		return math.MinInt64
	}
	return int64(m.StartTime) + int64(last)*int64(m.StepTime)
}

// presentCount returns the number of present points.
func (m Metric) presentCount() int {
	if p := m.packed; p != nil {
		absent := 0
		for _, block := range p.absent {
			absent += bits.OnesCount64(block)
		}
		return p.n - absent
	}
	present := 0
	for _, absent := range m.IsAbsent {
		if !absent {
			present++
		}
	}
	return present
}

func (c *packedPointCursor) advanceToBlock(p *packedPoints, block int) {
	for c.block < block {
		c.offset = c.next
		c.next += 1 + bits.OnesCount64(p.values[c.offset])
		c.block++
	}
}

func (c *packedPointCursor) pointAt(p *packedPoints, i int) (float64, bool) {
	block := i >> 6
	bit := uint64(1) << (uint(i) & 63)
	if p.absent[block]&bit != 0 {
		return 0, true
	}
	mask := p.values[c.offset]
	if mask&bit == 0 {
		return 0, false
	}
	offset := c.offset + 1 + bits.OnesCount64(mask&(bit-1))
	return math.Float64frombits(p.values[offset]), false
}
