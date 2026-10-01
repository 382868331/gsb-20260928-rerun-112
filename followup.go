package gateway

import (
	"context"
	"fmt"
	"math"
)

// ContinueBatch resumes the unfinished items of a partially applied
// conditional batch. Unknown items are verified read-only first: a confirmed
// item is marked completed without any new write; a mismatch means the device
// changed on its own after a possibly applied write, so the item becomes a
// conflict and is never blindly rewritten. Items that were not attempted or
// skipped by a condition mismatch re-run their full conditional check and
// are written only if the device now matches the operator's precondition.
func (c *Channel) ContinueBatch(ctx context.Context, result *ConditionalBatchResult) {
	c.conditionalEvent(result, "continue_start", "resuming unfinished items; unknown items verified read-only first")
	for i := range result.Items {
		item := &result.Items[i]
		switch item.Status {
		case StatusUnknown:
			if item.Sent == nil {
				item.Status = StatusFailed
				item.Error = "unknown item has no sent value to verify against"
				continue
			}
			verify := c.VerifyWrite(ctx, item.Field, *item.Sent)
			switch verify.Outcome {
			case VerifyConfirmed:
				item.Status = StatusCompleted
				item.Error = ""
				c.conditionalEvent(result, "field_confirmed", item.Field+": read-back matches, no second write sent")
			case VerifyMismatch:
				item.Status = StatusConflict
				item.Error = "device holds another value; lost response plus external change, not rewriting blindly"
				c.conditionalEvent(result, "field_conflict", item.Field+": "+verify.Error)
			default:
				c.conditionalEvent(result, "field_unknown", item.Field+": read-back failed, kept pending")
			}
		case StatusNotAttempted, StatusConditionMismatch:
			request := ConditionalWriteRequest{Field: item.Field, Value: item.Requested}
			if item.ConditionExpected != nil && item.ConditionSeenAt != nil {
				mapping, ok := c.Mappings[item.Field]
				if !ok {
					item.Status = StatusFailed
					item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), item.Field)
					continue
				}
				alphaRaw, err := mapping.BetaToAlpha(*item.ConditionExpected)
				if err != nil {
					item.Status = StatusFailed
					item.Error = fmt.Sprintf("stored precondition not convertible: %v", err)
					continue
				}
				request.Expect = &Precondition{ExpectedAlphaRaw: float64(alphaRaw), ObservedAt: *item.ConditionSeenAt}
			}
			retry := c.conditionalWriteOne(ctx, request)
			retry.TraceID = item.TraceID
			retry.Frames = append(item.Frames, retry.Frames...)
			*item = retry
			c.conditionalEvent(result, "field_"+item.Status, item.Field+": continued, "+item.Error)
		}
	}
	result.Status, result.Error = summarizeConditional(result.Items)
	result.Duration = c.Now().Sub(result.StartedAt)
	c.persistConditional(result)
}

// RevertRule names the explicit restore policy for one batch. The only
// supported rule restores each applied item to the device value captured
// immediately before this batch wrote it.
const RevertToPreBatchValue = "pre_batch_value"

// RevertBatch restores items this batch applied, but only while the device
// still holds exactly the value this batch wrote. The restore target is the
// recorded pre-batch device value, never the operator's stale view. If the
// device changed on its own (current value differs from what was sent), the
// item becomes a conflict and is left untouched. Items never applied by this
// batch have nothing to revert.
func (c *Channel) RevertBatch(ctx context.Context, result *ConditionalBatchResult, rule string) {
	if rule != RevertToPreBatchValue {
		c.conditionalEvent(result, "revert_rejected", "unknown revert rule: "+rule)
		result.Error = "unsupported revert rule: " + rule
		c.persistConditional(result)
		return
	}
	c.conditionalEvent(result, "revert_start", "restoring applied items to recorded pre-batch device values")
	for i := range result.Items {
		item := &result.Items[i]
		if item.Status != StatusCompleted || item.Sent == nil {
			continue
		}
		if item.PreValue == nil {
			item.Status = StatusFailed
			item.Error = "no pre-batch device value recorded; refusing to guess a restore target"
			c.conditionalEvent(result, "field_failed", item.Field+": "+item.Error)
			continue
		}
		mapping, ok := c.Mappings[item.Field]
		if !ok {
			item.Status = StatusFailed
			item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), item.Field)
			continue
		}
		recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
			item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
		})
		fieldCtx := WithFrameRecorder(ctx, recorder)
		current, err := c.Upstream.Read(fieldCtx, mapping.Beta.Tag)
		if err != nil {
			item.Status = StatusUnknown
			item.Error = fmt.Sprintf("revert check unreadable: %v; item left as-is, verify later", err)
			c.conditionalEvent(result, "field_unknown", item.Field+": "+item.Error)
			continue
		}
		if math.Abs(current-*item.Sent) > 1e-9 {
			item.Status = StatusConflict
			item.Error = fmt.Sprintf("device holds %g, this batch wrote %g; external change detected, not overwriting", current, *item.Sent)
			c.conditionalEvent(result, "field_conflict", item.Field+": "+item.Error)
			continue
		}
		if err := c.Upstream.Write(fieldCtx, mapping.Beta.Tag, *item.PreValue); err != nil {
			if isUnknownWriteError(err) {
				item.Status = StatusUnknown
			} else {
				item.Status = StatusFailed
			}
			item.Error = err.Error()
			c.conditionalEvent(result, "field_"+item.Status, item.Field+": revert write, "+item.Error)
			continue
		}
		restored := *item.PreValue
		item.Sent = &restored
		item.Status = StatusCompleted
		item.Error = ""
		c.conditionalEvent(result, "field_reverted", fmt.Sprintf("%s: restored to pre-batch device value %g", item.Field, restored))
	}
	result.Status, result.Error = summarizeConditional(result.Items)
	result.Duration = c.Now().Sub(result.StartedAt)
	c.persistConditional(result)
}
