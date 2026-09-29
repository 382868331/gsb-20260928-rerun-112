package gateway

import (
	"fmt"
	"math"
)

// Field describes one protocol-side field and its relationship to engineering
// values. Scale/offset use the field's native unit: raw = (engineering/scale)+offset.
type Field struct {
	DataType DataType `json:"dataType"`
	Endian   Endian   `json:"endian"`
	Unit     string   `json:"unit"`
	Scale    float64  `json:"scale"`
	Offset   float64  `json:"offset"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
}

func (f Field) validate() error {
	switch f.DataType {
	case Uint16, Int16, Float32:
	default:
		return fmt.Errorf("unsupported data type %q", f.DataType)
	}
	switch f.Endian {
	case BigEndian, LittleEndian:
	default:
		return fmt.Errorf("unsupported endianness %q", f.Endian)
	}
	if !finiteFloat(f.Scale) || f.Scale == 0 {
		return fmt.Errorf("field scale must be finite and non-zero")
	}
	if !finiteFloat(f.Offset) {
		return fmt.Errorf("field offset must be finite")
	}
	if f.Min != nil && f.Max != nil && *f.Min > *f.Max {
		return fmt.Errorf("field min %v is greater than max %v", *f.Min, *f.Max)
	}
	return nil
}

func (f Field) checkEngineering(v float64) error {
	if f.Min != nil && v < *f.Min {
		return fmt.Errorf("value %g below field minimum %g %s", v, *f.Min, f.Unit)
	}
	if f.Max != nil && v > *f.Max {
		return fmt.Errorf("value %g above field maximum %g %s", v, *f.Max, f.Unit)
	}
	return nil
}

// Encode converts an engineering value into protocol bytes. It rejects values
// that are non-finite, out of range, fractional when the target is integral,
// or not exactly representable as float32 when that is the wire type.
func (f Field) Encode(v Value) ([]byte, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	converted, err := convertUnit(v, f.Unit)
	if err != nil {
		return nil, err
	}
	n := converted.Number
	if !finiteFloat(n) {
		return nil, fmt.Errorf("cannot encode non-finite value")
	}
	if err := f.checkEngineering(n); err != nil {
		return nil, err
	}
	raw := n/f.Scale + f.Offset
	if !finiteFloat(raw) {
		return nil, fmt.Errorf("non-finite raw value after scaling")
	}
	switch f.DataType {
	case Uint16:
		x, err := encodeInteger(raw, 0, math.MaxUint16)
		if err != nil {
			return nil, err
		}
		b := []byte{byte(x >> 8), byte(x)}
		if f.Endian == LittleEndian {
			b[0], b[1] = b[1], b[0]
		}
		return b, nil
	case Int16:
		x, err := encodeInteger(raw, math.MinInt16, math.MaxInt16)
		if err != nil {
			return nil, err
		}
		b := []byte{byte(uint16(x) >> 8), byte(x)}
		if f.Endian == LittleEndian {
			b[0], b[1] = b[1], b[0]
		}
		return b, nil
	case Float32:
		if !finiteFloat(raw) {
			return nil, fmt.Errorf("non-finite value cannot be encoded as float32")
		}
		if fv := float32(raw); !finiteFloat(float64(fv)) {
			return nil, fmt.Errorf("value %g overflows float32", raw)
		}
		b := make([]byte, 4)
		u := math.Float32bits(float32(raw))
		if f.Endian == BigEndian {
			b[0], b[1], b[2], b[3] = byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
		} else {
			b[0], b[1], b[2], b[3] = byte(u), byte(u>>8), byte(u>>16), byte(u>>24)
		}
		return b, nil
	}
	return nil, fmt.Errorf("unsupported data type %q", f.DataType)
}

// Decode converts protocol bytes into a normalized engineering value.
func (f Field) Decode(b []byte) (Value, error) {
	if err := f.validate(); err != nil {
		return Value{}, err
	}
	var raw float64
	switch f.DataType {
	case Uint16:
		if len(b) != 2 {
			return Value{}, fmt.Errorf("uint16 needs 2 bytes, got %d", len(b))
		}
		if f.Endian == LittleEndian {
			b = []byte{b[1], b[0]}
		}
		raw = float64(uint16(b[0])<<8 | uint16(b[1]))
	case Int16:
		if len(b) != 2 {
			return Value{}, fmt.Errorf("int16 needs 2 bytes, got %d", len(b))
		}
		if f.Endian == LittleEndian {
			b = []byte{b[1], b[0]}
		}
		raw = float64(int16(uint16(b[0])<<8 | uint16(b[1])))
	case Float32:
		if len(b) != 4 {
			return Value{}, fmt.Errorf("float32 needs 4 bytes, got %d", len(b))
		}
		if f.Endian == LittleEndian {
			b = []byte{b[3], b[2], b[1], b[0]}
		}
		u := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
		raw = float64(math.Float32frombits(u))
	default:
		return Value{}, fmt.Errorf("unsupported data type %q", f.DataType)
	}
	if !finiteFloat(raw) {
		return Value{}, fmt.Errorf("decoded non-finite protocol value")
	}
	n := (raw - f.Offset) * f.Scale
	if !finiteFloat(n) {
		return Value{}, fmt.Errorf("non-finite engineering value after scaling")
	}
	if err := f.checkEngineering(n); err != nil {
		return Value{}, err
	}
	return Value{Number: n, Unit: f.Unit}, nil
}

func encodeInteger(raw float64, min, max int64) (int64, error) {
	if math.Trunc(raw) != raw {
		return 0, fmt.Errorf("fractional value %g is not representable by integer field", raw)
	}
	if raw < float64(min) || raw > float64(max) {
		return 0, fmt.Errorf("integer raw value %g outside [%d,%d]", raw, min, max)
	}
	return int64(raw), nil
}
