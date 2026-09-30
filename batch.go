package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

type batchRecorder struct {
	fieldIndex int
	t          *Trace
}

func (r batchRecorder) RecordFrame(direction, protocol string, data []byte) {
	field := &r.t.Fields[r.fieldIndex]
	r.t.Frames = append(r.t.Frames, TraceFrame{Direction: direction, Protocol: protocol, Field: field.Field, Hex: fmtHex(data)})
	field.Frames = append(field.Frames, TraceFrame{Direction: direction, Protocol: protocol, Field: field.Field, Hex: fmtHex(data)})
}

func (c *Channel) pushField(t *Trace, field TraceField) *TraceField {
	t.Fields = append(t.Fields, field)
	return &t.Fields[len(t.Fields)-1]
}

// ProcessBatch executes one multi-field logical operation. Each field keeps its
// own status, conversion evidence and frames; a partial failure never triggers a
// blind retry of the whole batch.
func (c *Channel) ProcessBatch(ctx context.Context, req AlphaBatchRequest) (AlphaBatchResponse, Trace) {
	t := Trace{ID: c.NewID(), Channel: c.Name, Operation: req.Operation(), StartedAt: c.Now(), From: AlphaProtocol, To: c.Upstream.Name(), Attempts: 1}
	resp := AlphaBatchResponse{Function: req.Function, Results: make([]AlphaBatchItemResult, len(req.Items))}
	if req.Function == AlphaBatchWrite {
		c.processBatchWrite(ctx, &t, &resp, req.Items)
	} else {
		c.processBatchRead(ctx, &t, &resp, req.Items)
	}
	t.Status = aggregateStatus(t.Fields)
	t.Duration = c.Now().Sub(t.StartedAt)
	resp.Status = mapOverallStatus(t.Status)
	return resp, t
}

func (c *Channel) processBatchRead(ctx context.Context, t *Trace, resp *AlphaBatchResponse, items []AlphaBatchItem) {
	for i, item := range items {
		field := c.pushField(t, TraceField{Field: item.Tag(), Register: item.Tag(), Attempts: 1})
		mapping, ok := c.Mappings[item.Tag()]
		if !ok {
			field.Status = StatusFailed
			field.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField, item.Tag())
			c.batchEvent(t, item.Tag(), "mapping_lookup", field.Error)
			resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusError}
			continue
		}
		field.Field = mapping.Name
		field.Register = mapping.Alpha.Tag
		fieldCtx := contextWithFieldRecorder(ctx, batchRecorder{fieldIndex: i, t: t})
		c.event(t, "mapping_lookup", "mapped "+mapping.Alpha.Tag+" to "+mapping.Beta.Tag)
		betaValue, err := c.Upstream.Read(fieldCtx, mapping.Beta.Tag)
		observedAt := c.Now()
		field.ObservedAt = &observedAt
		if err != nil {
			field.Status = StatusFailed
			field.Error = err.Error()
			c.batchEvent(t, mapping.Name, "upstream_read", err.Error())
			resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusError}
			continue
		}
		field.OutValue = &betaValue
		if err := mapping.Beta.ValidateRaw(betaValue); err != nil {
			field.Status = StatusFailed
			field.Error = err.Error()
			c.batchEvent(t, mapping.Name, "range_check", err.Error())
			resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusError}
			continue
		}
		alphaValue, err := mapping.BetaToAlpha(betaValue)
		if err != nil {
			field.Status = StatusFailed
			field.Error = err.Error()
			c.batchEvent(t, mapping.Name, "convert_read", err.Error())
			resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusError}
			continue
		}
		canonical, _ := fromEndpoint(mapping.Beta, betaValue)
		back := float64(alphaValue)
		field.Canonical = &canonical
		field.BackValue = &back
		field.Status = StatusCompleted
		resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusOK, Value: uint16(alphaValue)}
		c.batchEvent(t, mapping.Name, "convert_read", fmt.Sprintf("%g %s -> raw %d at %s", betaValue, mapping.Beta.Unit, alphaValue, observedAt.Format(time.RFC3339Nano)))
	}
	t.Snapshot = false
	resp.Snapshot = AlphaSnapshotMixed
}

func (c *Channel) processBatchWrite(ctx context.Context, t *Trace, resp *AlphaBatchResponse, items []AlphaBatchItem) {
	for i, item := range items {
		field := c.pushField(t, TraceField{Field: item.Tag(), Register: item.Tag(), Attempts: 1})
		mapping, ok := c.Mappings[item.Tag()]
		if !ok {
			field.Status = StatusFailed
			field.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField, item.Tag())
			c.batchEvent(t, item.Tag(), "mapping_lookup", field.Error)
			resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusError}
			continue
		}
		field.Field = mapping.Name
		field.Register = mapping.Alpha.Tag
		c.event(t, "mapping_lookup", "mapped "+mapping.Alpha.Tag+" to "+mapping.Beta.Tag)
		in := float64(item.Value)
		field.InValue = &in
		if err := mapping.Alpha.ValidateRaw(in); err != nil {
			c.markRejected(field, resp, i, "range_check", mapping.Name, t, err)
			continue
		}
		betaValue, err := mapping.AlphaToBeta(in)
		if err != nil {
			c.markRejected(field, resp, i, "convert_write", mapping.Name, t, err)
			continue
		}
		canonical, _ := fromEndpoint(mapping.Alpha, in)
		field.Canonical = &canonical
		field.OutValue = &betaValue
		fieldCtx := contextWithFieldRecorder(ctx, batchRecorder{fieldIndex: i, t: t})
		err = c.Upstream.Write(fieldCtx, mapping.Beta.Tag, betaValue)
		if err != nil {
			if errors.Is(err, ErrResultUnknown) {
				field.Status = StatusUnknown
				field.Error = err.Error()
				resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusUnknown}
				c.batchEvent(t, mapping.Name, "upstream_write", err.Error())
				c.batchEvent(t, mapping.Name, "verify_path", "write outcome is unverified; re-read "+mapping.Beta.Tag+" or call VerifyUncertainWrites before any corrective write")
			} else {
				field.Status = StatusFailed
				field.Error = err.Error()
				resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusError}
				c.batchEvent(t, mapping.Name, "upstream_write", err.Error())
			}
			continue
		}
		back := betaValue
		field.BackValue = &back
		field.Status = StatusCompleted
		resp.Results[i] = AlphaBatchItemResult{Status: AlphaStatusOK, Value: uint16(int16(betaValue))}
		c.batchEvent(t, mapping.Name, "upstream_write", "acknowledged; no retry was attempted")
	}
}

func (c *Channel) markRejected(field *TraceField, resp *AlphaBatchResponse, index int, stage, name string, t *Trace, err error) {
	field.Status = StatusFailed
	field.Error = err.Error()
	resp.Results[index] = AlphaBatchItemResult{Status: AlphaStatusError}
	c.batchEvent(t, name, stage, err.Error())
}

func (c *Channel) batchEvent(t *Trace, field, stage, message string) {
	t.Events = append(t.Events, TraceEvent{At: c.Now(), Stage: stage, Message: field + ": " + message})
}

func contextWithFieldRecorder(ctx context.Context, recorder FrameRecorder) context.Context {
	if existing, ok := ctx.Value(frameRecorderKey{}).(FrameRecorder); ok && existing != nil {
		return WithFrameRecorder(ctx, chainedRecorder{existing: existing, scoped: recorder})
	}
	return WithFrameRecorder(ctx, recorder)
}

type chainedRecorder struct {
	existing FrameRecorder
	scoped   FrameRecorder
}

func (r chainedRecorder) RecordFrame(direction, protocol string, data []byte) {
	r.existing.RecordFrame(direction, protocol, data)
	r.scoped.RecordFrame(direction, protocol, data)
}

func aggregateStatus(fields []TraceField) string {
	hasCompleted, hasFailed, hasUnknown := false, false, false
	for _, field := range fields {
		switch field.Status {
		case StatusCompleted:
			hasCompleted = true
		case StatusUnknown:
			hasUnknown = true
		default:
			hasFailed = true
		}
	}
	if !hasFailed && !hasUnknown && hasCompleted {
		return StatusCompleted
	}
	if hasUnknown {
		return StatusUnknown
	}
	return StatusPartial
}

func mapOverallStatus(status string) byte {
	switch status {
	case StatusCompleted:
		return AlphaStatusOK
	case StatusUnknown:
		return AlphaStatusUnknown
	default:
		return AlphaStatusError
	}
}

const (
	VerifyApplied    = "applied"
	VerifyNotApplied = "not_applied"
	VerifyUncertain  = "uncertain"
)

type FieldVerification struct {
	Field      string    `json:"field"`
	Status     string    `json:"status"`
	ObservedAt time.Time `json:"observed_at"`
	Expected   float64   `json:"expected_raw"`
	Observed   *float64  `json:"observed_raw,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// VerifyUncertainWrites performs a fresh read-back for every field still in the
// unknown state and compares the current device value with the value that was
// sent. This is the documented follow-up path; it never repeats a write.
func (c *Channel) VerifyUncertainWrites(ctx context.Context, t Trace) []FieldVerification {
	results := make([]FieldVerification, 0)
	for _, field := range t.Fields {
		if field.Status != StatusUnknown || field.OutValue == nil {
			continue
		}
		mapping, ok := c.Mappings[field.Field]
		verification := FieldVerification{Field: field.Field, ObservedAt: c.Now(), Expected: *field.OutValue}
		if !ok {
			verification.Status = VerifyUncertain
			verification.Error = ErrUnsupportedField.Error()
			results = append(results, verification)
			continue
		}
		value, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
		verification.ObservedAt = c.Now()
		if err != nil {
			verification.Status = VerifyUncertain
			verification.Error = err.Error()
			results = append(results, verification)
			continue
		}
		verification.Observed = &value
		if math.Abs(value-*field.OutValue) <= 1e-9 {
			verification.Status = VerifyApplied
		} else {
			verification.Status = VerifyNotApplied
		}
		results = append(results, verification)
	}
	return results
}
