package gateway

import (
	"context"
	"fmt"
	"math"
	"time"
)

// Per-item outcomes of a conditional batch write. A batch never collapses
// into one total failure: every item keeps its own state.
const (
	// ItemNotAttempted: the write never left the gateway (e.g. the
	// precondition could not be evaluated), so the device is untouched.
	ItemNotAttempted = "not_attempted"
	// ItemConditionMismatch: the device current value differs from what the
	// operator saw, so the modification was skipped.
	ItemConditionMismatch = "condition_mismatch"
	// ItemApplied: the write was acknowledged by the device.
	ItemApplied = "applied"
	// ItemPendingConfirm: the response was lost after the request went out;
	// the device may or may not hold the new value. Never blindly rewrite.
	ItemPendingConfirm = "pending_confirm"
	// ItemConflict: a follow-up found the device holding an unexpected
	// value (it changed on its own). Operator decision required.
	ItemConflict = "conflict"
	// ItemReverted: a rollback restored the pre-write value and the device
	// acknowledged it.
	ItemReverted = "reverted"
	// ItemFailed: local validation or conversion rejected the item before
	// any device write.
	ItemFailed = "failed"
)

// WriteCondition is the value the operator saw for the field, in the
// operator-facing (alpha) raw units, plus when it was observed. The write is
// only attempted while the device still holds this value.
type WriteCondition struct {
	ExpectedAlphaRaw float64   `json:"expected_alpha_raw"`
	ObservedAt       time.Time `json:"observed_at"`
}

type ConditionalWriteRequest struct {
	Field     string
	Value     float64
	Condition *WriteCondition
}

// ConditionEvidence keeps the original field source and the reading moments
// across unit conversion, so a reopened batch can still be audited: which
// field the condition came from, what the operator saw (alpha raw), what
// that converts to on the wire (beta raw), what the device actually held
// when the gateway checked, and both timestamps.
type ConditionEvidence struct {
	Field            string    `json:"field"`
	ExpectedAlphaRaw float64   `json:"expected_alpha_raw"`
	ExpectedBetaRaw  float64   `json:"expected_beta_raw"`
	ObservedBetaRaw  *float64  `json:"observed_beta_raw,omitempty"`
	OperatorSeenAt   time.Time `json:"operator_seen_at"`
	CheckedAt        time.Time `json:"checked_at"`
}

type ConditionalItem struct {
	Field     string             `json:"field"`
	BetaTag   string             `json:"beta_tag"`
	Requested float64            `json:"requested_alpha_raw"`
	Sent      *float64           `json:"sent_beta_raw,omitempty"`
	Condition *ConditionEvidence `json:"condition,omitempty"`
	Status    string             `json:"status"`
	Error     string             `json:"error,omitempty"`
	Attempts  int                `json:"attempts"`
	Frames    []TraceFrame       `json:"frames"`
	TraceID   string             `json:"trace_id"`
}

// ConditionalBatchResult is persisted in the TraceStore so an interrupted
// session can reopen the batch and keep resolving uncertain items.
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

func (r *ConditionalBatchResult) event(at time.Time, stage, message string) {
	r.Events = append(r.Events, TraceEvent{At: at, Stage: stage, Message: message})
}

func (r *ConditionalBatchResult) summarize(now func() time.Time) {
	applied, pending, failed := 0, 0, 0
	for _, item := range r.Items {
		switch item.Status {
		case ItemApplied, ItemReverted:
			applied++
		case ItemPendingConfirm, ItemNotAttempted, ItemConflict, ItemNeedsReconfirm:
			pending++
		default:
			failed++
		}
	}
	switch {
	case pending == 0 && failed == 0:
		r.Status = StatusCompleted
		r.Error = ""
	case applied == 0 && pending == 0:
		r.Status = StatusFailed
	default:
		r.Status = StatusPartial
		r.Error = fmt.Sprintf("partial outcome: %d applied, %d unresolved, %d rejected; continue or roll back per item, never resubmit the whole batch", applied, pending, failed)
	}
	r.Duration = now().Sub(r.StartedAt)
}

// ConditionalBatchWrite applies a batch of modifications where each item may
// carry a precondition: the write only happens while the device current value
// still equals what the operator saw. Items are independent; a rejected or
// mismatched item never blocks or undoes the others.
func (c *Channel) ConditionalBatchWrite(ctx context.Context, requests []ConditionalWriteRequest) ConditionalBatchResult {
	result := ConditionalBatchResult{
		ID:        c.NewID(),
		Channel:   c.Name,
		Operation: OpWrite,
		StartedAt: c.Now(),
	}
	result.event(c.Now(), "batch_start", fmt.Sprintf("conditional write of %d fields; per-item preconditions, no batch retry", len(requests)))
	for _, request := range requests {
		item := c.conditionalWriteOne(ctx, request)
		result.Items = append(result.Items, item)
		result.event(c.Now(), "item_"+item.Status, item.Field+": "+item.Error)
	}
	result.summarize(c.Now)
	c.Traces.AddConditionalBatch(result)
	return result
}

func (c *Channel) conditionalWriteOne(ctx context.Context, request ConditionalWriteRequest) ConditionalItem {
	item := ConditionalItem{Field: request.Field, Requested: request.Value, Attempts: 1, TraceID: c.NewID()}
	mapping, ok := c.Mappings[request.Field]
	if !ok {
		item.Status = ItemFailed
		item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), request.Field)
		return item
	}
	item.BetaTag = mapping.Beta.Tag
	if err := mapping.Alpha.ValidateRaw(request.Value); err != nil {
		item.Status = ItemFailed
		item.Error = err.Error()
		return item
	}
	betaValue, err := mapping.AlphaToBeta(request.Value)
	if err != nil {
		item.Status = ItemFailed
		item.Error = err.Error()
		return item
	}
	item.Sent = &betaValue
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	ctx = WithFrameRecorder(ctx, recorder)
	if request.Condition != nil {
		evidence, proceed := c.checkCondition(ctx, mapping, request.Condition, &item)
		item.Condition = evidence
		if !proceed {
			return item
		}
	}
	if err := c.Upstream.Write(ctx, mapping.Beta.Tag, betaValue); err != nil {
		if isUnknownWriteError(err) {
			item.Status = ItemPendingConfirm
			item.Error = err.Error() + "; verify by read-back before any further write"
		} else {
			item.Status = ItemFailed
			item.Error = err.Error()
		}
		return item
	}
	item.Status = ItemApplied
	return item
}

// checkCondition reads the device once (read-only) and compares against the
// operator-observed value after unit conversion. It reports whether the write
// may proceed and sets the item status when it may not.
func (c *Channel) checkCondition(ctx context.Context, mapping FieldMapping, condition *WriteCondition, item *ConditionalItem) (*ConditionEvidence, bool) {
	expectedBeta, err := mapping.AlphaToBeta(condition.ExpectedAlphaRaw)
	if err != nil {
		item.Status = ItemFailed
		item.Error = fmt.Sprintf("condition value not representable on device: %v", err)
		return &ConditionEvidence{Field: mapping.Name, ExpectedAlphaRaw: condition.ExpectedAlphaRaw, OperatorSeenAt: condition.ObservedAt}, false
	}
	evidence := &ConditionEvidence{
		Field:            mapping.Name,
		ExpectedAlphaRaw: condition.ExpectedAlphaRaw,
		ExpectedBetaRaw:  expectedBeta,
		OperatorSeenAt:   condition.ObservedAt,
	}
	observed, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
	evidence.CheckedAt = c.Now()
	if err != nil {
		item.Status = ItemNotAttempted
		item.Error = fmt.Sprintf("precondition could not be verified (read failed: %v); device left untouched", err)
		return evidence, false
	}
	evidence.ObservedBetaRaw = &observed
	if math.Abs(observed-expectedBeta) > 1e-9 {
		item.Status = ItemConditionMismatch
		item.Error = fmt.Sprintf("device holds %g but operator saw %g (beta raw); write skipped", observed, expectedBeta)
		return evidence, false
	}
	return evidence, true
}

// ContinueConditionalBatch resumes the unfinished items of a stored batch:
//   - pending_confirm items are resolved by read-back only. A confirmed value
//     becomes applied; a mismatch means the device changed on its own and the
//     item becomes conflict — the write is never blindly repeated.
//   - not_attempted / condition_mismatch items re-run the full conditional
//     write, so the precondition is checked against the live device value.
//
// The updated result is stored under the same batch ID for later reopening.
func (c *Channel) ContinueConditionalBatch(ctx context.Context, batchID string, fields []string) (ConditionalBatchResult, error) {
	result, ok := c.Traces.GetConditionalBatch(batchID)
	if !ok {
		return ConditionalBatchResult{}, fmt.Errorf("unknown conditional batch %s", batchID)
	}
	want := map[string]bool{}
	for _, f := range fields {
		want[f] = true
	}
	result.event(c.Now(), "batch_continue", fmt.Sprintf("resuming %d item(s) of batch %s", len(fields), batchID))
	for i, item := range result.Items {
		if !want[item.Field] {
			continue
		}
		switch item.Status {
		case ItemPendingConfirm:
			result.Items[i] = c.resolvePending(ctx, item)
		case ItemNotAttempted, ItemConditionMismatch:
			request := ConditionalWriteRequest{Field: item.Field, Value: item.Requested}
			if item.Condition != nil {
				request.Condition = &WriteCondition{ExpectedAlphaRaw: item.Condition.ExpectedAlphaRaw, ObservedAt: item.Condition.OperatorSeenAt}
			}
			retry := c.conditionalWriteOne(ctx, request)
			retry.TraceID = item.TraceID
			retry.Frames = append(item.Frames, retry.Frames...)
			result.Items[i] = retry
		default:
			result.event(c.Now(), "item_skipped", item.Field+": already "+item.Status)
		}
		result.event(c.Now(), "item_"+result.Items[i].Status, item.Field+": "+result.Items[i].Error)
	}
	result.summarize(c.Now)
	c.Traces.AddConditionalBatch(result)
	return result, nil
}

func (c *Channel) resolvePending(ctx context.Context, item ConditionalItem) ConditionalItem {
	if item.Sent == nil {
		item.Status = ItemNotAttempted
		item.Error = "no write ever left the gateway"
		return item
	}
	verify := c.VerifyWrite(ctx, item.Field, *item.Sent)
	item.Frames = append(item.Frames, verify.Frames...)
	switch verify.Outcome {
	case VerifyConfirmed:
		item.Status = ItemApplied
		item.Error = ""
	case VerifyMismatch:
		// Response was lost AND the device moved on its own: replaying the
		// write would clobber the newer value, so stop and ask the operator.
		item.Status = ItemConflict
		item.Error = fmt.Sprintf("device holds %g, neither confirmed nor safe to overwrite; operator decision required", *verify.Observed)
	default:
		item.Error = verify.Error
	}
	return item
}

// RollbackConditionalBatch restores applied items to the value recorded in
// their precondition (the value the operator saw before the change). The
// rollback first reads the device: it only reverts while the device still
// holds exactly the value this batch wrote. If the device changed on its own
// the item becomes conflict and is left untouched.
func (c *Channel) RollbackConditionalBatch(ctx context.Context, batchID string, fields []string) (ConditionalBatchResult, error) {
	result, ok := c.Traces.GetConditionalBatch(batchID)
	if !ok {
		return ConditionalBatchResult{}, fmt.Errorf("unknown conditional batch %s", batchID)
	}
	want := map[string]bool{}
	for _, f := range fields {
		want[f] = true
	}
	result.event(c.Now(), "batch_rollback", fmt.Sprintf("reverting %d item(s) of batch %s to precondition values", len(fields), batchID))
	for i, item := range result.Items {
		if !want[item.Field] {
			continue
		}
		if item.Status != ItemApplied {
			result.event(c.Now(), "item_skipped", item.Field+": status "+item.Status+" is not applied")
			continue
		}
		result.Items[i] = c.rollbackOne(ctx, item)
		result.event(c.Now(), "item_"+result.Items[i].Status, item.Field+": "+result.Items[i].Error)
	}
	result.summarize(c.Now)
	c.Traces.AddConditionalBatch(result)
	return result, nil
}

func (c *Channel) rollbackOne(ctx context.Context, item ConditionalItem) ConditionalItem {
	mapping, ok := c.Mappings[item.Field]
	if !ok {
		item.Status = ItemConflict
		item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), item.Field)
		return item
	}
	if item.Condition == nil {
		item.Status = ItemConflict
		item.Error = "no precondition value recorded; rollback rule requires the pre-write value"
		return item
	}
	restoreBeta, err := mapping.AlphaToBeta(item.Condition.ExpectedAlphaRaw)
	if err != nil {
		item.Status = ItemConflict
		item.Error = fmt.Sprintf("pre-write value not representable on device: %v", err)
		return item
	}
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	ctx = WithFrameRecorder(ctx, recorder)
	current, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
	if err != nil {
		item.Status = ItemPendingConfirm
		item.Error = fmt.Sprintf("rollback read-back failed: %v; not reverting blindly", err)
		return item
	}
	if item.Sent == nil || math.Abs(current-*item.Sent) > 1e-9 {
		item.Status = ItemConflict
		item.Error = fmt.Sprintf("device holds %g, expected our write %v; device changed on its own, rollback aborted", current, item.Sent)
		return item
	}
	if err := c.Upstream.Write(ctx, mapping.Beta.Tag, restoreBeta); err != nil {
		if isUnknownWriteError(err) {
			item.Status = ItemPendingConfirm
			item.Sent = &restoreBeta
			item.Error = err.Error() + "; rollback response lost, verify by read-back"
		} else {
			item.Status = ItemConflict
			item.Error = err.Error()
		}
		return item
	}
	item.Sent = &restoreBeta
	item.Status = ItemReverted
	item.Error = ""
	return item
}
