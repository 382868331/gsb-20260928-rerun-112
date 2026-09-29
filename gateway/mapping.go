package gateway

import "fmt"

type FieldRef struct {
	Adapter string `json:"adapter"`
	Address string `json:"address"`
	Field   Field  `json:"field"`
}

type Point struct {
	Name string   `json:"name"`
	From FieldRef `json:"from"`
	To   FieldRef `json:"to"`
}

type Mapping struct {
	points map[string]Point
}

func NewMapping(points []Point) (*Mapping, error) {
	m := &Mapping{points: make(map[string]Point, len(points))}
	for _, p := range points {
		if p.Name == "" {
			return nil, fmt.Errorf("point name is required")
		}
		if p.From.Adapter == "" || p.To.Adapter == "" {
			return nil, fmt.Errorf("point %q requires source and destination adapters", p.Name)
		}
		if err := p.From.Field.validate(); err != nil {
			return nil, fmt.Errorf("point %q source: %w", p.Name, err)
		}
		if err := p.To.Field.validate(); err != nil {
			return nil, fmt.Errorf("point %q destination: %w", p.Name, err)
		}
		if _, exists := m.points[p.Name]; exists {
			return nil, fmt.Errorf("duplicate point %q", p.Name)
		}
		m.points[p.Name] = p
	}
	return m, nil
}

func (m *Mapping) Point(name string) (Point, error) {
	p, ok := m.points[name]
	if !ok {
		return Point{}, fmt.Errorf("unknown point %q", name)
	}
	return p, nil
}

func (m *Mapping) Points() []Point {
	out := make([]Point, 0, len(m.points))
	for _, p := range m.points {
		out = append(out, p)
	}
	return out
}
