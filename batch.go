package gateway

import (
	"context"
	"fmt"
	"time"
)

const (
	// ReadSnapshotNonAtomic reports that the fields of one logical read were
	// collected by separate device round trips and therefore cannot be claimed
	// as a single atomic device-time snapshot.
	ReadSnapshotNonAtomic = "non-atomic"

	VerifyConfirmed = "confirmed"
	VerifyMismatch  = "mismatch"
	VerifyUnknown   = "unknown"
)

// FieldRead is the per-field evidence of a multi-field read.
// All evidence is retained even when the whole logical read fails halfway.
type FieldRead struct {
	Field     string       `json:"field"`
	BetaTag   string       `json:"beta_tag"`
	Status    string       `json:"status"`
	RawValue  *float64     `json:"beta_raw_value,omitempty"`
	Value     *float64     `json:"alpha_raw_value,omitempty"`
	Canonical *float64     `json:"canonical_value,omitempty"`
	ReadAt    time.Time    `json:"read_at"`
	Error     string       `json:"error,omitempty"`
	Frames    []TraceFrame `json:"frames"`
	TraceID   string       `json:"trace_id"`
}

// BatchReadResult never pretends to be an atomic snapshot: Snapshot is always
// ReadSnapshotNonAtomic because every field uses its own device exchange.
type BatchReadResult struct {
	ID        string        `json:"id"`
	Channel   string        `json:"channel"`
	Operation string        `json:"operation"`
	Snapshot  string        `json:"snapshot"`
	Status    string        `json:"status"`
	StartedAt time.Time     `json:"started_at"`
	Duration  time.Duration `json:"duration"`
	Error     string        `json:"error,omitempty"`
	Fields    []FieldRead   `json:"fields"`
	Events    []TraceEvent  `json:"events"`
}

// FieldWriteItem carries the caller-visible state of one item in a batch write.
// Status uses the same completed/failed/unknown vocabulary as single writes.
type FieldWriteItem struct {
	Field     string       `json:"field"`
	BetaTag   string       `json:"beta_tag"`
	Requested float64      `json:"requested_alpha_raw"`
	Sent      *float64     `json:"sent_beta_raw,omitempty"`
	Status    string       `json:"status"`
	Error     string       `json:"error,omitempty"`
	Attempts  int          `json:"attempts"`
	Frames    []TraceFrame `json:"frames"`
	TraceID   string       `json:"trace_id"`
}

// BatchWriteResult keeps per-item state. A partial success is reported as such;
// the gateway never retries the whole batch and never replays a completed item.
type BatchWriteResult struct {
	ID        string           `json:"id"`
	Channel   string           `json:"channel"`
	Operation string           `json:"operation"`
	Status    string           `json:"status"`
	StartedAt time.Time        `json:"started_at"`
	Duration  time.Duration    `json:"duration"`
	Error     string           `json:"error,omitempty"`
	Items     []FieldWriteItem `json:"items"`
	Events    []TraceEvent     `json:"events"`
}

type BatchWriteRequest struct {
	Field string
	Value float64
}

// BatchRead executes one logical read over several related fields.
func (c *Channel) BatchRead(ctx context.Context, fields []string) BatchReadResult {
	result := BatchReadResult{
		ID:        c.NewID(),
		Channel:   c.Name,
		Operation: OpRead,
		Snapshot:  ReadSnapshotNonAtomic,
		StartedAt: c.Now(),
	}
	c.batchEvent(&result, "batch_start", fmt.Sprintf("logical read of %d fields; snapshot=%s", len(fields), ReadSnapshotNonAtomic))
	failed := false
	for _, name := range fields {
		item := c.readOneField(ctx, name)
		result.Fields = append(result.Fields, item)
		if item.Status != StatusCompleted {
			failed = true
			c.batchEvent(&result, "field_failed", name+": "+item.Error)
		}
	}
	if failed {
		result.Status = StatusFailed
		result.Error = "one or more fields could not be read; per-field evidence retained"
	} else {
		result.Status = StatusCompleted
	}
	result.Duration = c.Now().Sub(result.StartedAt)
	c.Traces.AddBatch(BatchTrace{ID: result.ID, Kind: "batch_read", StartedAt: result.StartedAt, Status: result.Status, Snapshot: result.Snapshot})
	return result
}

// BatchWrite executes one logical modification over several related fields.
// Each item gets its own device exchange and its own state. Uncertain items
// stay unknown; callers verify them with VerifyWrite instead of re-issuing.
func (c *Channel) BatchWrite(ctx context.Context, requests []BatchWriteRequest) BatchWriteResult {
	result := BatchWriteResult{
		ID:        c.NewID(),
		Channel:   c.Name,
		Operation: OpWrite,
		StartedAt: c.Now(),
	}
	c.batchEvent(&result, "batch_start", fmt.Sprintf("logical write of %d fields; no batch retry", len(requests)))
	completed, failed, unknown := 0, 0, 0
	for _, request := range requests {
		item := c.writeOneField(ctx, request)
		result.Items = append(result.Items, item)
		switch item.Status {
		case StatusCompleted:
			completed++
		case StatusUnknown:
			unknown++
			c.batchEvent(&result, "field_unknown", item.Field+": "+item.Error)
		default:
			failed++
			c.batchEvent(&result, "field_failed", item.Field+": "+item.Error)
		}
	}
	switch {
	case failed == 0 && unknown == 0:
		result.Status = StatusCompleted
	case completed == 0 && unknown == 0:
		result.Status = StatusFailed
	default:
		result.Status = StatusPartial
		result.Error = fmt.Sprintf("partial outcome: %d completed, %d failed, %d unknown; verify unknown items, do not resubmit the batch", completed, failed, unknown)
	}
	result.Duration = c.Now().Sub(result.StartedAt)
	c.Traces.AddBatch(BatchTrace{ID: result.ID, Kind: "batch_write", StartedAt: result.StartedAt, Status: result.Status})
	return result
}

func (c *Channel) readOneField(ctx context.Context, name string) FieldRead {
	item := FieldRead{Field: name, ReadAt: c.Now(), TraceID: c.NewID()}
	mapping, ok := c.Mappings[name]
	if !ok {
		item.Status = StatusFailed
		item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), name)
		return item
	}
	item.BetaTag = mapping.Beta.Tag
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	fieldCtx := WithFrameRecorder(ctx, recorder)
	betaValue, err := c.Upstream.Read(fieldCtx, mapping.Beta.Tag)
	item.ReadAt = c.Now()
	if err != nil {
		item.Status = StatusFailed
		item.Error = err.Error()
		return item
	}
	if err := mapping.Beta.ValidateRaw(betaValue); err != nil {
		item.Status = StatusFailed
		item.Error = err.Error()
		raw := betaValue
		item.RawValue = &raw
		return item
	}
	alphaRaw, err := mapping.BetaToAlpha(betaValue)
	if err != nil {
		item.Status = StatusFailed
		item.Error = err.Error()
		raw := betaValue
		item.RawValue = &raw
		return item
	}
	canonical, _ := fromEndpoint(mapping.Beta, betaValue)
	out := float64(alphaRaw)
	item.RawValue = &betaValue
	item.Canonical = &canonical
	item.Value = &out
	item.Status = StatusCompleted
	return item
}

func (c *Channel) writeOneField(ctx context.Context, request BatchWriteRequest) FieldWriteItem {
	item := FieldWriteItem{Field: request.Field, Requested: request.Value, Attempts: 1, TraceID: c.NewID()}
	mapping, ok := c.Mappings[request.Field]
	if !ok {
		item.Status = StatusFailed
		item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), request.Field)
		return item
	}
	item.BetaTag = mapping.Beta.Tag
	if err := mapping.Alpha.ValidateRaw(request.Value); err != nil {
		item.Status = StatusFailed
		item.Error = err.Error()
		return item
	}
	betaValue, err := mapping.AlphaToBeta(request.Value)
	if err != nil {
		item.Status = StatusFailed
		item.Error = err.Error()
		return item
	}
	item.Sent = &betaValue
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	fieldCtx := WithFrameRecorder(ctx, recorder)
	if err := c.Upstream.Write(fieldCtx, mapping.Beta.Tag, betaValue); err != nil {
		if isUnknownWriteError(err) {
			item.Status = StatusUnknown
		} else {
			item.Status = StatusFailed
		}
		item.Error = err.Error()
		return item
	}
	item.Status = StatusCompleted
	return item
}

func (c *Channel) batchEvent(result any, stage, message string) {
	at := c.Now()
	switch r := result.(type) {
	case *BatchReadResult:
		r.Events = append(r.Events, TraceEvent{At: at, Stage: stage, Message: message})
	case *BatchWriteResult:
		r.Events = append(r.Events, TraceEvent{At: at, Stage: stage, Message: message})
	}
}
