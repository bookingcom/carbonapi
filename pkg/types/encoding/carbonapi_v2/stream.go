package carbonapi_v2

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/bookingcom/carbonapi/pkg/types"
)

var streamReaders = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, 32<<10) }}
var zeroPointBytes [32 << 10]byte
var nativeLittleEndian = func() bool { var b [2]byte; binary.NativeEndian.PutUint16(b[:], 1); return b[0] == 1 }()

// RenderStreamDecoder consumes a MultiFetchResponse without retaining its
// decompressed wire representation. Point storage belongs to the returned
// metrics; the small reader buffer can be reused immediately.
func RenderStreamDecoder(src io.Reader) ([]types.Metric, error) {
	return renderStreamDecoder(src, false)
}

// RenderStreamDecoderPacked keeps sparse point vectors compact until replica merge.
func RenderStreamDecoderPacked(src io.Reader) ([]types.Metric, error) {
	return renderStreamDecoder(src, true)
}

func renderStreamDecoder(src io.Reader, packed bool) ([]types.Metric, error) {
	r := streamReaders.Get().(*bufio.Reader)
	r.Reset(src)
	defer func() { r.Reset(nil); streamReaders.Put(r) }()
	s := protoStream{r: r, left: math.MaxInt64}
	metrics := make([]types.Metric, 0)
	var absentScratch []bool
	for {
		tag, err := s.varint()
		if err == io.EOF {
			return metrics, nil
		}
		if err != nil {
			return nil, err
		}
		if err = validTag(tag); err != nil {
			return nil, err
		}
		if tag>>3 != 1 {
			if err = s.skip(tag, 0); err != nil {
				return nil, err
			}
			continue
		}
		if tag&7 != 2 {
			return nil, fmt.Errorf("proto: wrong wire type %d for Metrics", tag&7)
		}
		n, err := s.length()
		if err != nil {
			return nil, err
		}
		child := protoStream{r: r, left: n, absentScratch: absentScratch}
		m, err := child.metric(packed)
		if err != nil {
			return nil, err
		}
		s.left -= n
		if packed {
			m.PackPoints()
			// Packed metrics own a bitmap, so their temporary bool storage can
			// serve the next metric. Dense metrics retain it and must detach.
			absentScratch = child.absentScratch
			if m.IsAbsent != nil || cap(absentScratch) > maxStreamPointChunk {
				absentScratch = nil
			}
		}
		metrics = append(metrics, m)
	}
}

type protoStream struct {
	r             *bufio.Reader
	left          int64
	absentScratch []bool
}

func validTag(tag uint64) error {
	if tag>>3 == 0 || tag>>3 > 1<<29-1 || tag&7 > 5 {
		return fmt.Errorf("proto: invalid tag %d", tag)
	}
	return nil
}
func (s *protoStream) varint() (uint64, error) {
	var value uint64
	for i := 0; i < 10; i++ {
		if s.left == 0 {
			if i == 0 {
				return 0, io.EOF
			}
			return 0, io.ErrUnexpectedEOF
		}
		b, err := s.r.ReadByte()
		if err != nil {
			if err == io.EOF && i != 0 {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		s.left--
		if i == 9 && b > 1 {
			return 0, fmt.Errorf("proto: varint overflow")
		}
		value |= uint64(b&127) << uint(i*7)
		if b < 128 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("proto: varint overflow")
}
func (s *protoStream) length() (int64, error) {
	n, err := s.varint()
	if err != nil {
		return 0, err
	}
	if n > uint64(s.left) || n > uint64(math.MaxInt) {
		return 0, io.ErrUnexpectedEOF
	}
	return int64(n), nil
}
func (s *protoStream) read(dst []byte) error {
	if int64(len(dst)) > s.left {
		return io.ErrUnexpectedEOF
	}
	s.left -= int64(len(dst))
	_, err := io.ReadFull(s.r, dst)
	return err
}
func (s *protoStream) discard(n int64) error {
	if n > s.left {
		return io.ErrUnexpectedEOF
	}
	s.left -= n
	_, err := io.CopyN(io.Discard, s.r, n)
	return err
}
func (s *protoStream) skip(tag uint64, depth int) error {
	if depth > 100 {
		return fmt.Errorf("proto: group nesting exceeds 100")
	}
	switch tag & 7 {
	case 0:
		_, err := s.varint()
		return err
	case 1:
		return s.discard(8)
	case 2:
		n, err := s.length()
		if err != nil {
			return err
		}
		return s.discard(n)
	case 3:
		for {
			next, err := s.varint()
			if err != nil {
				return err
			}
			if err = validTag(next); err != nil {
				return err
			}
			if next&7 == 4 {
				if next>>3 != tag>>3 {
					return fmt.Errorf("proto: mismatched end group")
				}
				return nil
			}
			if err = s.skip(next, depth+1); err != nil {
				return err
			}
		}
	case 4:
		return fmt.Errorf("proto: unexpected end group")
	case 5:
		return s.discard(4)
	}
	return fmt.Errorf("proto: invalid wire type")
}

// A corrupt length must not reserve a giant point array before its bytes have
// arrived. Large legitimate fields are read in bounded increments instead.
const maxStreamPointChunk = 1 << 20

func (s *protoStream) floats(dst []float64, n int64) ([]float64, error) {
	if n%8 != 0 {
		return nil, fmt.Errorf("proto: packed double length is not divisible by 8")
	}
	for n > 0 {
		count := int(min(n/8, maxStreamPointChunk))
		start := len(dst)
		dst = append(dst, make([]float64, count)...)
		points := dst[start:]
		// Float64 contains no Go pointers. The reader writes only into this
		// newly owned array, never into borrowed protobuf input storage.
		wire := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(points))), len(points)*8)
		if err := s.read(wire); err != nil {
			return nil, err
		}
		if !nativeLittleEndian {
			for i, v := range points {
				points[i] = math.Float64frombits(bits.ReverseBytes64(math.Float64bits(v)))
			}
		}
		n -= int64(count * 8)
	}
	return dst, nil
}

// Decode sparse wire values directly into packed replica storage. Entire zero
// chunks use a vectorized comparison; only nonzero blocks inspect point bits.
func (s *protoStream) packedFloats(p *types.PointPacker, n int64) ([]float64, bool, error) {
	if n%8 != 0 {
		return nil, false, fmt.Errorf("proto: packed double length is not divisible by 8")
	}
	if n > s.left {
		return nil, false, io.ErrUnexpectedEOF
	}
	for n > 0 {
		count := int(min(n, int64(len(zeroPointBytes))))
		wire, err := s.r.Peek(count)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, false, err
		}
		consumed := count
		dense := false
		if bytes.Equal(wire, zeroPointBytes[:count]) {
			p.AppendZeros(count / 8)
		} else {
			for start := 0; start < count; start += 512 {
				end := min(start+512, count)
				block := wire[start:end]
				if bytes.Equal(block, zeroPointBytes[:len(block)]) {
					p.AppendZeros(len(block) / 8)
				} else {
					for i := 0; i < len(block); i += 8 {
						p.AppendBits(binary.LittleEndian.Uint64(block[i : i+8]))
					}
				}
				if p.DensePreferred() {
					consumed = end
					dense = true
					break
				}
			}
		}
		if _, err = s.r.Discard(consumed); err != nil {
			return nil, false, err
		}
		s.left -= int64(consumed)
		n -= int64(consumed)
		if dense {
			values, err := s.floats(p.DenseValues(), n)
			return values, true, err
		}
	}
	return nil, false, nil
}

func (s *protoStream) bools(dst []bool, n int64) ([]bool, error) {
	// Canonical packed bools use one byte per element. Read directly into
	// owned storage and normalize the representation before any bool is read.
	// Noncanonical multi-byte varints use the general reader below.
	if n > maxStreamPointChunk {
		end := s.left - n
		for s.left > end {
			v, err := s.varint()
			if err != nil {
				return nil, err
			}
			if s.left < end {
				return nil, io.ErrUnexpectedEOF
			}
			dst = append(dst, v != 0)
		}
		return dst, nil
	}
	start := len(dst)
	if int(n) <= cap(dst)-start {
		// read overwrites every byte before any bool is inspected.
		dst = dst[:start+int(n)]
	} else {
		dst = append(dst, make([]bool, int(n))...)
	}
	if n == 0 {
		return dst, nil
	}
	wire := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst[start:]))), int(n))
	if err := s.read(wire); err != nil {
		return nil, err
	}
	out := start
	for i := 0; i < len(wire); {
		v := uint64(wire[i])
		used := 1
		if wire[i] >= 128 {
			v, used = binary.Uvarint(wire[i:])
			if used <= 0 {
				return nil, io.ErrUnexpectedEOF
			}
		}
		dst[out] = v != 0
		out++
		i += used
	}
	return dst[:out], nil
}
func (s *protoStream) metric(packed bool) (types.Metric, error) {
	var m types.Metric
	if packed {
		m.IsAbsent = s.absentScratch[:0]
	}
	var points types.PointPacker
	packing := packed
	for s.left > 0 {
		tag, err := s.varint()
		if err != nil {
			return m, err
		}
		if err = validTag(tag); err != nil {
			return m, err
		}
		field, wire := tag>>3, tag&7
		switch field {
		case 1:
			if wire != 2 {
				return m, fmt.Errorf("proto: wrong wire type for Name")
			}
			n, err := s.length()
			if err != nil {
				return m, err
			}
			// io.ReadAll here is confined to an unusual oversized metric name,
			// never to the decompressed response or a point field.
			var name []byte
			if n > maxStreamPointChunk {
				name, err = io.ReadAll(io.LimitReader(s.r, n))
				s.left -= int64(len(name))
				if err == nil && int64(len(name)) != n {
					err = io.ErrUnexpectedEOF
				}
			} else {
				name = make([]byte, int(n))
				err = s.read(name)
			}
			if err != nil {
				return m, err
			}
			m.Name = string(name)
		case 2, 3, 4:
			if wire != 0 {
				return m, fmt.Errorf("proto: wrong wire type for timestamp")
			}
			v, err := s.varint()
			if err != nil {
				return m, err
			}
			switch field {
			case 2:
				m.StartTime = int32(v)
			case 3:
				m.StopTime = int32(v)
			case 4:
				m.StepTime = int32(v)
			}
		case 5:
			n := int64(8)
			if wire == 2 {
				n, err = s.length()
			} else if wire != 1 {
				return m, fmt.Errorf("proto: wrong wire type for Values")
			}
			if err != nil {
				return m, err
			}
			if packing {
				var dense bool
				m.Values, dense, err = s.packedFloats(&points, n)
				packing = !dense
			} else {
				m.Values, err = s.floats(m.Values, n)
			}
			if err != nil {
				return m, err
			}
		case 6:
			switch wire {
			case 0:
				v, err := s.varint()
				if err != nil {
					return m, err
				}
				m.IsAbsent = append(m.IsAbsent, v != 0)
			case 2:
				n, err := s.length()
				if err != nil {
					return m, err
				}
				m.IsAbsent, err = s.bools(m.IsAbsent, n)
				if err != nil {
					return m, err
				}
			default:
				return m, fmt.Errorf("proto: wrong wire type for IsAbsent")
			}
		default:
			if err = s.skip(tag, 0); err != nil {
				return m, err
			}
		}
	}
	if len(m.IsAbsent) == 0 {
		m.IsAbsent = nil
	}
	s.absentScratch = m.IsAbsent
	if packing {
		points.Finish(&m)
	}
	return m, nil
}
