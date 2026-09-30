package gateway

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

var (
	ErrOutOfRange   = errors.New("value is outside the representable range")
	ErrLossyInteger = errors.New("value requires prohibited integer truncation")
)

type Endianness string

const (
	BigEndian    Endianness = "big-endian"
	LittleEndian Endianness = "little-endian"
)

type EndpointSpec struct {
	Protocol string
	Tag      string
	DataType string
	Unit     string
	Scale    float64
	Offset   float64
	MinRaw   float64
	MaxRaw   float64
	Endian   Endianness
}

type FieldMapping struct {
	Name  string
	Alpha EndpointSpec
	Beta  EndpointSpec
}

func (m FieldMapping) Validate() error {
	if m.Name == "" || m.Alpha.Tag == "" || m.Beta.Tag == "" {
		return errors.New("mapping and endpoint tags are required")
	}
	if m.Alpha.Scale == 0 || m.Beta.Scale == 0 {
		return errors.New("scale must not be zero")
	}
	if !compatibleUnits(m.Alpha.Unit, m.Beta.Unit) {
		return fmt.Errorf("incompatible units %q and %q", m.Alpha.Unit, m.Beta.Unit)
	}
	if err := validateSpec(m.Alpha); err != nil {
		return err
	}
	return validateSpec(m.Beta)
}

func validateSpec(s EndpointSpec) error {
	switch s.DataType {
	case "int16", "uint16", "int32":
	default:
		return fmt.Errorf("unsupported data type %q", s.DataType)
	}
	if s.MinRaw > s.MaxRaw {
		return ErrOutOfRange
	}
	return nil
}

func (m FieldMapping) AlphaToBeta(raw float64) (float64, error) {
	if err := m.Alpha.ValidateRaw(raw); err != nil {
		return 0, err
	}
	canonical, err := fromEndpoint(m.Alpha, raw)
	if err != nil {
		return 0, err
	}
	return toEndpoint(m.Beta, canonical)
}

func (m FieldMapping) BetaToAlpha(raw float64) (int64, error) {
	if err := m.Beta.ValidateRaw(raw); err != nil {
		return 0, err
	}
	canonical, err := fromEndpoint(m.Beta, raw)
	if err != nil {
		return 0, err
	}
	out, err := toEndpoint(m.Alpha, canonical)
	if err != nil {
		return 0, err
	}
	return int64Value(out, m.Alpha)
}

func (s EndpointSpec) ValidateRaw(value float64) error {
	if math.IsNaN(value) {
		return ErrNaN
	}
	if math.IsInf(value, 0) || value < s.MinRaw || value > s.MaxRaw {
		return fmt.Errorf("%w: %s %g not in [%g,%g]", ErrOutOfRange, s.Protocol, value, s.MinRaw, s.MaxRaw)
	}
	return nil
}

func fromEndpoint(s EndpointSpec, raw float64) (float64, error) {
	if err := s.ValidateRaw(raw); err != nil {
		return 0, err
	}
	engineering := raw*s.Scale + s.Offset
	return toCanonical(s.Unit, engineering)
}

func toEndpoint(s EndpointSpec, canonical float64) (float64, error) {
	if math.IsNaN(canonical) || math.IsInf(canonical, 0) {
		return 0, ErrNaN
	}
	engineering, err := fromCanonical(s.Unit, canonical)
	if err != nil {
		return 0, err
	}
	raw := (engineering - s.Offset) / s.Scale
	if err := s.ValidateRaw(raw); err != nil {
		return 0, err
	}
	if isIntegerType(s.DataType) && math.Abs(raw-math.Round(raw)) > 1e-9 {
		return 0, fmt.Errorf("%w: %s value %g is not exactly representable", ErrLossyInteger, s.Protocol, raw)
	}
	return math.Round(raw), nil
}

func isIntegerType(t string) bool { return strings.HasPrefix(t, "int") || strings.HasPrefix(t, "uint") }

func int64Value(raw float64, spec EndpointSpec) (int64, error) {
	r := math.Round(raw)
	switch spec.DataType {
	case "uint16":
		if r < 0 || r > math.MaxUint16 {
			return 0, ErrOutOfRange
		}
	case "int16":
		if r < math.MinInt16 || r > math.MaxInt16 {
			return 0, ErrOutOfRange
		}
	case "int32":
		if r < math.MinInt32 || r > math.MaxInt32 {
			return 0, ErrOutOfRange
		}
	}
	return int64(r), nil
}

func DefaultMappings() []FieldMapping {
	return []FieldMapping{
		{
			Name:  "temperature",
			Alpha: EndpointSpec{Protocol: AlphaProtocol, Tag: "register:40001", DataType: "int16", Unit: "degC", Scale: 0.1, MinRaw: -1000, MaxRaw: 5000, Endian: BigEndian},
			Beta:  EndpointSpec{Protocol: BetaProtocol, Tag: "T1", DataType: "int16", Unit: "degC", Scale: 0.01, MinRaw: -10000, MaxRaw: 50000, Endian: LittleEndian},
		},
		{
			Name:  "pressure",
			Alpha: EndpointSpec{Protocol: AlphaProtocol, Tag: "register:40002", DataType: "uint16", Unit: "kPa", Scale: 1, MinRaw: 0, MaxRaw: 10000, Endian: BigEndian},
			Beta:  EndpointSpec{Protocol: BetaProtocol, Tag: "P1", DataType: "int16", Unit: "psi", Scale: 0.01, MinRaw: 0, MaxRaw: 14504, Endian: LittleEndian},
		},
		{
			Name:  "setpoint",
			Alpha: EndpointSpec{Protocol: AlphaProtocol, Tag: "register:40003", DataType: "int16", Unit: "degC", Scale: 0.1, MinRaw: -1000, MaxRaw: 5000, Endian: BigEndian},
			Beta:  EndpointSpec{Protocol: BetaProtocol, Tag: "S1", DataType: "int16", Unit: "degC", Scale: 0.01, MinRaw: -10000, MaxRaw: 50000, Endian: LittleEndian},
		},
	}
}
