package gateway

import (
	"context"
	"fmt"
)

type Gateway struct {
	mapping  *Mapping
	tracker  *Tracker
	adapters map[string]Adapter
}

func New(mapping *Mapping, tracker *Tracker, adapters ...Adapter) *Gateway {
	g := &Gateway{mapping: mapping, tracker: tracker, adapters: make(map[string]Adapter)}
	for _, a := range adapters {
		g.adapters[a.Name()] = a
	}
	return g
}

func (g *Gateway) Operation(id string) (Trace, bool) { return g.tracker.Get(id) }
func (g *Gateway) Operations() []Trace               { return g.tracker.List() }

func (g *Gateway) Read(ctx context.Context, pointName string) (*Trace, error) {
	p, err := g.mapping.Point(pointName)
	if err != nil {
		return nil, err
	}
	from, _, err := g.pair(p)
	if err != nil {
		return nil, err
	}
	t := NewTrace(Read, pointName, nil)
	g.tracker.Add(t)
	t.Event(StageReceived, "read point %s", pointName)
	src, err := from.Read(ctx, p.From, t)
	if err != nil {
		t.Event(StageFailed, "source read failed: %v", err)
		t.Fail(err)
		return t, err
	}
	t.Event(StageDecode, "source value %g %s", src.Number, src.Unit)
	wireValue, err := p.To.Field.Encode(src)
	if err != nil {
		err = fmt.Errorf("value cannot be represented on destination: %w", err)
		t.Event(StageFailed, "%v", err)
		t.Fail(err)
		return t, err
	}
	t.Event(StageEncode, "destination wire bytes % x", wireValue)
	result, err := p.To.Field.Decode(wireValue)
	if err != nil {
		t.Event(StageFailed, "converted value verification failed: %v", err)
		t.Fail(err)
		return t, err
	}
	t.Event(StageCompleted, "converted value %g %s", result.Number, result.Unit)
	t.Succeed(&result)
	return t, nil
}

func (g *Gateway) Write(ctx context.Context, pointName string, v Value) (*Trace, error) {
	p, err := g.mapping.Point(pointName)
	if err != nil {
		return nil, err
	}
	_, to, err := g.pair(p)
	if err != nil {
		return nil, err
	}
	t := NewTrace(Write, pointName, &v)
	g.tracker.Add(t)
	t.Event(StageReceived, "write point %s value %g %s", pointName, v.Number, v.Unit)
	err = to.Write(ctx, p.To, v, t)
	if err != nil {
		if IsUncertain(err) {
			t.Event(StageUncertain, "write outcome unknown: %v", err)
			t.MarkUnknown(err)
		} else {
			t.Event(StageFailed, "write failed: %v", err)
			t.Fail(err)
		}
		return t, err
	}
	result := v
	t.Event(StageCompleted, "write acknowledged")
	t.Succeed(&result)
	return t, nil
}

func (g *Gateway) Close() error {
	var first error
	for _, a := range g.adapters {
		if err := a.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (g *Gateway) pair(p Point) (Adapter, Adapter, error) {
	from := g.adapters[p.From.Adapter]
	to := g.adapters[p.To.Adapter]
	if from == nil {
		return nil, nil, fmt.Errorf("adapter %q not registered", p.From.Adapter)
	}
	if to == nil {
		return nil, nil, fmt.Errorf("adapter %q not registered", p.To.Adapter)
	}
	return from, to, nil
}
