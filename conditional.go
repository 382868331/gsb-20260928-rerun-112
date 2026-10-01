package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	// StatusNotAttempted marks an item whose precondition could not be
	// evaluated (the device read failed), so no write was ever sent.
	StatusNotAttempted = "not_attempted"
	// StatusConditionMismatch marks an item whose precondition did not match
	// the device value the operator based the request on; no write was sent.
	StatusConditionMismatch = "condition_mismatch"
	// StatusConflict marks a follow-up that found the device holding a value
	// neither written nor expected by this batch (the device changed on its
	// own). The gateway never resolves a conflict by blind rewriting.
	StatusConflict = "conflict"
)

// Precondition is the operator-visible device state a conditional write is
// based on: the alpha raw value the operator saw and the moment it was read.
// The gateway converts it to beta raw once and keeps both, together with the
// observation time, so later verification survives unit conversion and
// restarts.
type Precondition struct {
	ExpectedAlphaRaw float64   `json:"expected_alpha_raw"`
	ObservedAt       time.Time `json:"observed_at"`
}

type ConditionalWriteRequest struct {
	Field string
	Value float64
	// Expect, when non-nil, requires the device to still hold the value the
	// operator saw; otherwise the item is skipped as condition_mismatch.
	Expect *Precondition
}

// ConditionalItem is the per-item evidence of a conditional batch write.
// Status is one of completed / unknown / failed / not_attempted /
// condition_mismatch / conflict.
type ConditionalItem struct {
	Field     string   `json:"field"`
	BetaTag   string   `json:"beta_tag"`
	Requested float64  `json:"requested_alpha_raw"`
	Sent      *float64 `json:"sent_beta_raw,omitempty"`
	Status    string   `json:"status"`
	Error     string   `json:"error,omitempty"`
	Attempts  int      `json:"attempts"`
	// Condition evidence: what the operator expected (converted to beta raw)
	// and what the device actually held when the gateway checked, with the
	// gateway-side check time. Both are retained across unit conversion.
	ConditionExpected *float64   `json:"condition_expected_beta_raw,omitempty"`
	ConditionSeenAt   *time.Time `json:"condition_observed_at,omitempty"`
	Observed          *float64   `json:"observed_beta_raw,omitempty"`
	CheckedAt         *time.Time `json:"checked_at,omitempty"`
	// PreValue is the device value (beta raw) captured before this batch
	// applied its write; it is the only value RevertBatch may restore.
	PreValue *float64     `json:"pre_value_beta_raw,omitempty"`
	Frames   []TraceFrame `json:"frames"`
	TraceID  string       `json:"trace_id"`
}

// ConditionalBatchResult keeps per-item state; a partial batch is reported
// per item and never collapsed into one total failure.
type ConditionalBatchResult struct {
	ID        string            `json:"id"`
	Channel   string            `json:"channel"`
	Operation string            `json:"operation"`
	Status    string            `json:"status"`
	StartedAt time.Time         `json:"started_at"`
	Duration  time.Duration     `json:"duration"`
	Error     string            `json:"error,omitempty"`
	Items     []ConditionalItem `json:"items"`
	Events    []TraceEvent      `json:"events"`
}

// ConditionalBatchWrite executes a batch where each item is applied only if
// the device still holds the value the operator saw. Every item gets its own
// read-check-write sequence and its own state; items are never atomic and a
// single total failure is never used to erase actual writes.
func (c *Channel) ConditionalBatchWrite(ctx context.Context, requests []ConditionalWriteRequest) ConditionalBatchResult {
	result := ConditionalBatchResult{
		ID:        c.NewID(),
		Channel:   c.Name,
		Operation: OpWrite,
		StartedAt: c.Now(),
	}
	c.conditionalEvent(&result, "batch_start", fmt.Sprintf("conditional write of %d fields; per-item states, no batch retry", len(requests)))
	for _, request := range requests {
		item := c.conditionalWriteOne(ctx, request)
		result.Items = append(result.Items, item)
		if item.Status != StatusCompleted {
			c.conditionalEvent(&result, "field_"+item.Status, item.Field+": "+item.Error)
		}
	}
	result.Status, result.Error = summarizeConditional(result.Items)
	result.Duration = c.Now().Sub(result.StartedAt)
	c.Traces.AddBatch(BatchTrace{ID: result.ID, Kind: "conditional_batch_write", StartedAt: result.StartedAt, Status: result.Status})
	c.persistConditional(&result)
	return result
}

func (c *Channel) conditionalWriteOne(ctx context.Context, request ConditionalWriteRequest) ConditionalItem {
	item := ConditionalItem{Field: request.Field, Requested: request.Value, Attempts: 1, TraceID: c.NewID()}
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
	if request.Expect != nil {
		expected, err := mapping.AlphaToBeta(request.Expect.ExpectedAlphaRaw)
		if err != nil {
			item.Status = StatusFailed
			item.Error = fmt.Sprintf("precondition value not convertible: %v", err)
			return item
		}
		item.ConditionExpected = &expected
		seen := request.Expect.ObservedAt
		item.ConditionSeenAt = &seen
	}
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	fieldCtx := WithFrameRecorder(ctx, recorder)
	// Read-only precondition check against the real device value.
	observed, err := c.Upstream.Read(fieldCtx, mapping.Beta.Tag)
	checkedAt := c.Now()
	if err != nil {
		item.Status = StatusNotAttempted
		item.Error = fmt.Sprintf("precondition check unreadable: %v; write not attempted", err)
		return item
	}
	item.Observed = &observed
	item.CheckedAt = &checkedAt
	item.PreValue = &observed
	if item.ConditionExpected != nil && math.Abs(observed-*item.ConditionExpected) > 1e-9 {
		item.Status = StatusConditionMismatch
		item.Error = fmt.Sprintf("device holds %g, operator saw %g at %s; write skipped", observed, *item.ConditionExpected, request.Expect.ObservedAt.Format(time.RFC3339))
		return item
	}
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

func summarizeConditional(items []ConditionalItem) (string, string) {
	completed, pending := 0, 0
	for _, item := range items {
		switch item.Status {
		case StatusCompleted:
			completed++
		default:
			pending++
		}
	}
	switch {
	case pending == 0:
		return StatusCompleted, ""
	case completed == 0:
		return StatusFailed, fmt.Sprintf("no item applied: %d pending/skipped; per-item states retained", pending)
	default:
		return StatusPartial, fmt.Sprintf("partial outcome: %d completed, %d not applied or uncertain; use ContinueBatch/RevertBatch, do not resubmit the batch", completed, pending)
	}
}

// VerifyConditionalItem resolves one unknown item of a conditional batch by
// read-back only. It works on results reloaded from the journal after a
// restart, because every item retains its sent beta raw value.
func (c *Channel) VerifyConditionalItem(ctx context.Context, result *ConditionalBatchResult, field string) (VerifyResult, error) {
	for i := range result.Items {
		item := &result.Items[i]
		if item.Field != field {
			continue
		}
		if item.Status != StatusUnknown {
			return VerifyResult{}, fmt.Errorf("field %s is %s, only unknown items require verification", field, item.Status)
		}
		if item.Sent == nil {
			return VerifyResult{}, errors.New("unknown item never left the gateway")
		}
		verify := c.VerifyWrite(ctx, field, *item.Sent)
		switch verify.Outcome {
		case VerifyConfirmed:
			item.Status = StatusCompleted
			item.Error = ""
			c.conditionalEvent(result, "field_confirmed", field+": read-back matches the sent value")
		case VerifyMismatch:
			item.Status = StatusConflict
			item.Error = "device holds another value; lost response plus external change, not rewriting blindly"
			c.conditionalEvent(result, "field_conflict", field+": "+verify.Error)
		}
		result.Status, result.Error = summarizeConditional(result.Items)
		c.persistConditional(result)
		return verify, nil
	}
	return VerifyResult{}, fmt.Errorf("%w: %s", ErrUnsupportedField, field)
}

func (c *Channel) conditionalEvent(result *ConditionalBatchResult, stage, message string) {
	result.Events = append(result.Events, TraceEvent{At: c.Now(), Stage: stage, Message: message})
}

func (c *Channel) persistConditional(result *ConditionalBatchResult) {
	if c.Journal != nil {
		_ = c.Journal.Put(*result)
	}
}
