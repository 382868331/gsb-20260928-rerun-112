package gateway

import (
	"math"
	"testing"
)

func TestIntegerEncodingRejectsFractionalAndOverflow(t *testing.T) {
	f := Field{DataType: Uint16, Endian: BigEndian, Unit: "C", Scale: 1}
	for _, n := range []float64{-1, 65536, 1.5} {
		if _, err := f.Encode(Value{Number: n, Unit: "C"}); err == nil {
			t.Fatalf("encoded unrepresentable %g", n)
		}
	}
	b, err := f.Encode(Value{Number: 258, Unit: "C"})
	if err != nil || string(b) != "\x01\x02" {
		t.Fatalf("big endian encode = % x, %v", b, err)
	}
}

func TestPressureUnitConversion(t *testing.T) {
	f := Field{DataType: Float32, Endian: LittleEndian, Unit: "psi", Scale: 1}
	b, err := f.Encode(Value{Number: 6.894757293168, Unit: "kPa"})
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.Decode(b)
	if err != nil || math.Abs(v.Number-1) > 1e-6 || v.Unit != "psi" {
		t.Fatalf("decode = %+v, %v", v, err)
	}
}
