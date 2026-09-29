package gateway

func compatibleUnits(a, b string) bool {
	switch a {
	case "degC", "degF", "K":
		return b == "degC" || b == "degF" || b == "K"
	case "kPa", "psi", "bar":
		return b == "kPa" || b == "psi" || b == "bar"
	default:
		return a == b
	}
}

func toCanonical(unit string, value float64) (float64, error) {
	switch unit {
	case "degF":
		return (value - 32) * 5 / 9, nil
	case "K":
		return value - 273.15, nil
	case "psi":
		return value * 6.894757, nil
	case "bar":
		return value * 100, nil
	default:
		return value, nil
	}
}

func fromCanonical(unit string, canonical float64) (float64, error) {
	switch unit {
	case "degF":
		return canonical*9/5 + 32, nil
	case "K":
		return canonical + 273.15, nil
	case "psi":
		return canonical / 6.894757, nil
	case "bar":
		return canonical / 100, nil
	default:
		return canonical, nil
	}
}
