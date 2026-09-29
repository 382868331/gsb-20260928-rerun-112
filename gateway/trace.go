package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type EventStage string

const (
	StageReceived  EventStage = "received"
	StageDecode    EventStage = "decoded"
	StageEncode    EventStage = "encoded"
	StageWireSend  EventStage = "wire_send"
	StageWireRecv  EventStage = "wire_recv"
	StageCompleted EventStage = "completed"
	StageFailed    EventStage = "failed"
	StageUncertain EventStage = "uncertain"
)

type OperationStatus string

const (
	StatusPending   OperationStatus = "pending"
	StatusSucceeded OperationStatus = "succeeded"
	StatusFailed    OperationStatus = "failed"
	StatusUnknown   OperationStatus = "unknown"
)

type WireEvent struct {
	Adapter string    `json:"adapter"`
	Dir     Direction `json:"dir"`
	Hex     string    `json:"hex"`
	At      time.Time `json:"at"`
}

type Event struct {
	Stage   EventStage `json:"stage"`
	Message string     `json:"message"`
	At      time.Time  `json:"at"`
}

type Trace struct {
	ID        string          `json:"id"`
	Direction Direction       `json:"direction"`
	Point     string          `json:"point"`
	Status    OperationStatus `json:"status"`
	StartedAt time.Time       `json:"startedAt"`
	EndedAt   *time.Time      `json:"endedAt,omitempty"`
	Value     *Value          `json:"value,omitempty"`
	Result    *Value          `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	Messages  []WireEvent     `json:"wire,omitempty"`
	Events    []Event         `json:"events,omitempty"`
	mu        *sync.Mutex
}

func NewTrace(direction Direction, point string, v *Value) *Trace {
	return &Trace{ID: newOperationID(), Direction: direction, Point: point, Status: StatusPending, StartedAt: time.Now(), Value: v, mu: &sync.Mutex{}}
}

func (t *Trace) Event(stage EventStage, format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Events = append(t.Events, Event{Stage: stage, Message: fmt.Sprintf(format, args...), At: time.Now()})
}

func (t *Trace) Wire(adapter string, dir Direction, b []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Messages = append(t.Messages, WireEvent{Adapter: adapter, Dir: dir, Hex: hex.EncodeToString(b), At: time.Now()})
}

func (t *Trace) Succeed(result *Value) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.EndedAt = &now
	t.Status = StatusSucceeded
	t.Result = result
}

func (t *Trace) Fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.markLocked(StatusFailed, err)
}

func (t *Trace) MarkUnknown(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.markLocked(StatusUnknown, err)
}

func (t *Trace) markLocked(status OperationStatus, err error) {
	now := time.Now()
	t.EndedAt = &now
	t.Status = status
	if err != nil {
		t.Error = err.Error()
	}
}

func (t *Trace) Snapshot() Trace {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := *t
	cp.Messages = append([]WireEvent(nil), t.Messages...)
	cp.Events = append([]Event(nil), t.Events...)
	return cp
}

type Tracker struct {
	mu     sync.RWMutex
	traces map[string]*Trace
}

func NewTracker() *Tracker {
	return &Tracker{traces: make(map[string]*Trace)}
}

func (r *Tracker) Add(t *Trace) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.traces[t.ID] = t
}

func (r *Tracker) Get(id string) (Trace, bool) {
	r.mu.RLock()
	t := r.traces[id]
	r.mu.RUnlock()
	if t == nil {
		return Trace{}, false
	}
	return t.Snapshot(), true
}

func (r *Tracker) List() []Trace {
	r.mu.RLock()
	out := make([]Trace, 0, len(r.traces))
	for _, t := range r.traces {
		out = append(out, t.Snapshot())
	}
	r.mu.RUnlock()
	return out
}

func newOperationID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("operation id entropy unavailable: %v", err))
	}
	return hex.EncodeToString(b[:])
}
