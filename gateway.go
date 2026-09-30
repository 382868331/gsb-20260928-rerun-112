package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	OpRead          = "read"
	OpWrite         = "write"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusUnknown   = "unknown"
	StatusPartial   = "partial"
)

var (
	ErrUnsupportedField = errors.New("unsupported field")
	ErrUnsupportedOp    = errors.New("unsupported operation")
	ErrResultUnknown    = errors.New("write result is unknown")
	ErrNaN              = errors.New("numeric value is NaN")
)

func isUnknownWriteError(err error) bool { return errors.Is(err, ErrResultUnknown) }

type Upstream interface {
	Name() string
	Read(context.Context, string) (float64, error)
	Write(context.Context, string, float64) error
	Close() error
}

type Trace struct {
	ID        string        `json:"id"`
	Channel   string        `json:"channel"`
	Operation string        `json:"operation"`
	Field     string        `json:"field"`
	StartedAt time.Time     `json:"started_at"`
	Duration  time.Duration `json:"duration"`
	Status    string        `json:"status"`
	From      string        `json:"from_protocol"`
	To        string        `json:"to_protocol"`
	InValue   *float64      `json:"in_value,omitempty"`
	Canonical *float64      `json:"canonical_value,omitempty"`
	OutValue  *float64      `json:"out_value,omitempty"`
	BackValue *float64      `json:"back_value,omitempty"`
	Attempts  int           `json:"attempts"`
	Error     string        `json:"error,omitempty"`
	Events    []TraceEvent  `json:"events"`
	Frames    []TraceFrame  `json:"frames"`
}

type TraceEvent struct {
	At      time.Time `json:"at"`
	Stage   string    `json:"stage"`
	Message string    `json:"message"`
}

type TraceFrame struct {
	Direction string `json:"direction"`
	Protocol  string `json:"protocol"`
	Hex       string `json:"hex"`
}

type TraceStore struct {
	mu      sync.RWMutex
	records []Trace
	byID    map[string]Trace
	batches *batchStore
}

func NewTraceStore() *TraceStore {
	return &TraceStore{byID: map[string]Trace{}, batches: newBatchStore()}
}

func (s *TraceStore) Add(t Trace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, t)
	s.byID[t.ID] = t
}

func (s *TraceStore) Get(id string) (Trace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.byID[id]
	return t, ok
}

func (s *TraceStore) List() []Trace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Trace, len(s.records))
	copy(out, s.records)
	return out
}

type Channel struct {
	Name     string
	Mappings map[string]FieldMapping
	Upstream Upstream
	Traces   *TraceStore
	Now      func() time.Time
	NewID    func() string
}

func NewChannel(name string, upstream Upstream, traces *TraceStore, mappings ...FieldMapping) (*Channel, error) {
	byName := map[string]FieldMapping{}
	for _, mapping := range mappings {
		if err := mapping.Validate(); err != nil {
			return nil, err
		}
		byName[mapping.Name] = mapping
		byName[mapping.Alpha.Tag] = mapping
	}
	if traces == nil {
		traces = NewTraceStore()
	}
	return &Channel{Name: name, Mappings: byName, Upstream: upstream, Traces: traces, Now: time.Now, NewID: newTraceID}, nil
}

func (c *Channel) ServeAlpha(ctx context.Context, listener net.Listener) error {
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return err
		}
		go func(conn net.Conn) {
			defer conn.Close()
			_ = c.HandleAlphaConn(ctx, conn)
		}(conn)
	}
}

func (c *Channel) HandleAlphaConn(ctx context.Context, conn net.Conn) error {
	req, raw, err := ReadAlphaRequest(conn)
	if err != nil {
		t := Trace{ID: c.NewID(), Channel: c.Name, StartedAt: c.Now(), From: AlphaProtocol, To: c.Upstream.Name(), Attempts: 1, Status: StatusFailed, Error: err.Error()}
		t.Frames = append(t.Frames, TraceFrame{Direction: "in", Protocol: AlphaProtocol, Hex: fmtHex(raw)})
		c.event(&t, "decode", err.Error())
		c.Traces.Add(t)
		return err
	}
	resp, trace := c.processAlphaRequest(ctx, req)
	trace.Frames = append(trace.Frames, TraceFrame{Direction: "in", Protocol: AlphaProtocol, Hex: fmtHex(raw)})
	out := resp.Encode()
	trace.Frames = append(trace.Frames, TraceFrame{Direction: "out", Protocol: AlphaProtocol, Hex: fmtHex(out)})
	c.Traces.Add(trace)
	_, err = conn.Write(out)
	return err
}

func (c *Channel) ProcessAlphaRequest(ctx context.Context, req AlphaRequest) (AlphaResponse, Trace) {
	resp, trace := c.processAlphaRequest(ctx, req)
	c.Traces.Add(trace)
	return resp, trace
}

func (c *Channel) processAlphaRequest(ctx context.Context, req AlphaRequest) (AlphaResponse, Trace) {
	t := Trace{ID: c.NewID(), Channel: c.Name, Operation: req.Operation(), Field: req.Tag(), StartedAt: c.Now(), From: AlphaProtocol, To: c.Upstream.Name(), Attempts: 1, Status: StatusFailed}
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		t.Frames = append(t.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	ctx = WithFrameRecorder(ctx, recorder)
	resp := AlphaResponse{Address: req.Address, Function: req.Function, Status: AlphaStatusError}
	mapping, ok := c.Mappings[req.Tag()]
	if !ok {
		c.fail(&t, "mapping_lookup", fmt.Errorf("%w: %s", ErrUnsupportedField, req.Tag()))
		return resp, t
	}
	t.Field = mapping.Name
	c.event(&t, "mapping_lookup", "mapped "+mapping.Alpha.Tag+" to "+mapping.Beta.Tag)
	if req.Function == AlphaWrite {
		return c.processWrite(ctx, &t, req, mapping, resp)
	}
	return c.processRead(ctx, &t, mapping, resp)
}

func (c *Channel) processWrite(ctx context.Context, t *Trace, req AlphaRequest, mapping FieldMapping, resp AlphaResponse) (AlphaResponse, Trace) {
	in := float64(req.Value)
	t.InValue = &in
	if err := mapping.Alpha.ValidateRaw(in); err != nil {
		c.fail(t, "range_check", err)
		return resp, *t
	}
	betaValue, err := mapping.AlphaToBeta(in)
	if err != nil {
		c.fail(t, "convert_write", err)
		return resp, *t
	}
	canonical, _ := fromEndpoint(mapping.Alpha, in)
	t.Canonical = &canonical
	t.OutValue = &betaValue
	c.event(t, "convert_write", fmt.Sprintf("%g %s -> %g %s", in, mapping.Alpha.Unit, betaValue, mapping.Beta.Unit))
	if err := c.Upstream.Write(ctx, mapping.Beta.Tag, betaValue); err != nil {
		if errors.Is(err, ErrResultUnknown) {
			t.Status = StatusUnknown
			resp.Status = AlphaStatusUnknown
		}
		c.failNoStatusChange(t, "upstream_write", err)
		return resp, *t
	}
	t.BackValue = &betaValue
	t.Status = StatusCompleted
	resp.Status = AlphaStatusOK
	resp.Value = uint16(int16(betaValue))
	c.event(t, "upstream_write", "acknowledged; no retry was attempted")
	c.finish(t)
	return resp, *t
}

func (c *Channel) processRead(ctx context.Context, t *Trace, mapping FieldMapping, resp AlphaResponse) (AlphaResponse, Trace) {
	betaValue, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
	if err != nil {
		c.fail(t, "upstream_read", err)
		return resp, *t
	}
	if err := mapping.Beta.ValidateRaw(betaValue); err != nil {
		c.fail(t, "range_check", err)
		return resp, *t
	}
	alphaValue, err := mapping.BetaToAlpha(betaValue)
	if err != nil {
		c.fail(t, "convert_read", err)
		return resp, *t
	}
	canonical, _ := fromEndpoint(mapping.Beta, betaValue)
	out := float64(alphaValue)
	t.OutValue = &betaValue
	t.Canonical = &canonical
	t.BackValue = &out
	t.Status = StatusCompleted
	resp.Status = AlphaStatusOK
	resp.Value = uint16(alphaValue)
	c.event(t, "convert_read", fmt.Sprintf("%g %s -> raw %d", betaValue, mapping.Beta.Unit, alphaValue))
	c.finish(t)
	return resp, *t
}

func (c *Channel) event(t *Trace, stage, message string) {
	t.Events = append(t.Events, TraceEvent{At: c.Now(), Stage: stage, Message: message})
}

func (c *Channel) finish(t *Trace) { t.Duration = c.Now().Sub(t.StartedAt) }
func (c *Channel) fail(t *Trace, stage string, err error) {
	t.Status = StatusFailed
	c.failNoStatusChange(t, stage, err)
}
func (c *Channel) failNoStatusChange(t *Trace, stage string, err error) {
	t.Error = err.Error()
	c.event(t, stage, err.Error())
	c.finish(t)
}

type frameRecorderFunc func(string, string, []byte)

func (f frameRecorderFunc) RecordFrame(direction, protocol string, data []byte) {
	f(direction, protocol, data)
}
