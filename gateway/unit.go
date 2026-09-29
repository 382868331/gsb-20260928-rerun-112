package gateway

import "fmt"

// Unit conversion is explicit. Unknown or incompatible units fail instead of
// being treated as identity conversion.
var linearUnits = map[string]struct {
	group, canonical string
	factor, offset   float64
}{
	"C":   {group: "temperature", canonical: "C", factor: 1, offset: 0},
	"F":   {group: "temperature", canonical: "C", factor: 1.0 / 1.8, offset: -32.0 / 1.8},
	"kPa": {group: "pressure", canonical: "kPa", factor: 1, offset: 0},
	"psi": {group: "pressure", canonical: "kPa", factor: 6.894757293168, offset: 0},
}

// convertUnit converts between compatible linear engineering units.
func convertUnit(v Value, targetUnit string) (Value, error) {
	if v.Unit == targetUnit {
		return v, nil
	}
	from, ok1 := linearUnits[v.Unit]
	to, ok2 := linearUnits[targetUnit]
	if !ok1 || !ok2 {
		return Value{}, fmt.Errorf("unknown unit pair: %q -> %q", v.Unit, targetUnit)
	}
	if from.group != to.group {
		return Value{}, fmt.Errorf("incompatible units: %q (%s) -> %q (%s)", v.Unit, from.group, targetUnit, to.group)
	}
	canonical := v.Number*from.factor + from.offset
	if !finiteFloat(canonical) {
		return Value{}, fmt.Errorf("non-finite value after unit conversion")
	}
	out := (canonical - to.offset) / to.factor
	if !finiteFloat(out) {
		return Value{}, fmt.Errorf("non-finite value after unit conversion")
	}
	return Value{Number: out, Unit: targetUnit}, nil
}
