package gateway

import "math"

// Direction is the logical operation requested by the upper system.
type Direction string

const (
	Read  Direction = "read"
	Write Direction = "write"
)

// DataType is the limited set supported by the first-round adapters.
type DataType string

const (
	Uint16  DataType = "uint16"
	Int16   DataType = "int16"
	Float32 DataType = "float32"
)

// Endian describes wire byte order for multi-byte protocol fields.
type Endian string

const (
	BigEndian    Endian = "big"
	LittleEndian Endian = "little"
)

// Value is an engineering value plus its physical unit.
// All in-memory calculations use float64; protocol encoders independently
// verify that the value can be represented by the destination field.
type Value struct {
	Number float64 `json:"number"`
	Unit   string  `json:"unit"`
}

func finiteFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
