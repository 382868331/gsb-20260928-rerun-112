package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"time"
)

// Confirmation lifecycle of one previewed item. A withdrawn confirmation is
// terminal: a late confirm request can never re-enable it — the operator must
// run a new preview.
const (
	ConfirmPending        = "pending"
	ConfirmConfirmed      = "confirmed"
	ConfirmWithdrawn      = "withdrawn"
	ConfirmNeedsReconfirm = "needs_reconfirm"
)

// PreviewItem is the dry-run view of one requested modification: the target
// after unit conversion and the impact against the value the device held when
// the preview read it. Nothing is written during a preview.
type PreviewItem struct {
	Field              string             `json:"field"`
	BetaTag            string             `json:"beta_tag"`
	Requested          float64            `json:"requested_alpha_raw"`
	TargetBetaRaw      *float64           `json:"target_beta_raw,omitempty"`
	CurrentBetaRaw     *float64           `json:"current_beta_raw,omitempty"`
	CurrentAlphaRaw    *float64           `json:"current_alpha_raw,omitempty"`
	Condition          *ConditionEvidence `json:"condition,omitempty"`
	MappingFingerprint string             `json:"mapping_fingerprint,omitempty"`
	Status             string             `json:"status"` // "ok" or "blocked"
	Error              string             `json:"error,omitempty"`
	Frames             []TraceFrame       `json:"frames"`
}

// ItemConfirmation binds the operator's approval to exactly the facts seen at
// preview time: this field and write endpoint, requested/confirmed values,
// device value, precondition, and the mapping rules in effect then.
type ItemConfirmation struct {
	State              string             `json:"state"`
	BetaTag            string             `json:"beta_tag"`
	RequestedAlphaRaw  float64            `json:"requested_alpha_raw"`
	TargetBetaRaw      *float64           `json:"target_beta_raw,omitempty"`
	ObservedBetaRaw    *float64           `json:"observed_beta_raw,omitempty"`
	Condition          *ConditionEvidence `json:"condition,omitempty"`
	MappingFingerprint string             `json:"mapping_fingerprint"`
	ConfirmedAt        time.Time          `json:"confirmed_at,omitempty"`
	WithdrawnAt        *time.Time         `json:"withdrawn_at,omitempty"`
}

// EffectFact records what actually happened to the device at execution time,
// kept separately from the preview and confirmation facts.
type EffectFact struct {
	Field           string    `json:"field"`
	SentBetaRaw     *float64  `json:"sent_beta_raw,omitempty"`
	Outcome         string    `json:"outcome"`               // applied / pending_confirm / conflict / failed
	VerifiedBy      string    `json:"verified_by,omitempty"` // "ack" or "read_back"
	ObservedBetaRaw *float64  `json:"observed_beta_raw,omitempty"`
	At              time.Time `json:"at"`
}

// PreviewBatch keeps the three kinds of facts of the dry-run workflow —
// preview (items), confirmation (per-item approvals) and effect (execution
// outcomes) — so an interrupted session can reopen and continue.
type PreviewBatch struct {
	ID          string                       `json:"id"`
	Channel     string                       `json:"channel"`
	CreatedAt   time.Time                    `json:"created_at"`
	Fingerprint string                       `json:"fingerprint"`
	Items       []PreviewItem                `json:"items"`
	Confirms    map[string]*ItemConfirmation `json:"confirmations"`
	Effects     []EffectFact                 `json:"effects,omitempty"`
	Conflicts   []string                     `json:"conflicts,omitempty"`
	Events      []TraceEvent                 `json:"events"`
}

func (p *PreviewBatch) event(at time.Time, stage, message string) {
	p.Events = append(p.Events, TraceEvent{At: at, Stage: stage, Message: message})
}

// mappingFingerprint identifies every fact in one mapping that can change the
// converted value or the device endpoint that receives it. Fingerprints are
// per field so an unrelated mapping change does not invalidate another item.
func mappingFingerprint(mapping FieldMapping) string {
	h := fnv.New64a()
	data, _ := json.Marshal(mapping)
	_, _ = h.Write(data)
	return fmt.Sprintf("%016x", h.Sum64())
}

func previewFingerprint(items []PreviewItem) string {
	h := fnv.New64a()
	for _, item := range items {
		_, _ = h.Write([]byte(item.Field))
		_, _ = h.Write([]byte(item.MappingFingerprint))
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

func conditionsEqual(want, got *ConditionEvidence) bool {
	if want == nil || got == nil {
		return want == got
	}
	wantObserved, gotObserved := false, false
	if want.ObservedBetaRaw != nil {
		wantObserved = true
	}
	if got.ObservedBetaRaw != nil {
		gotObserved = true
	}
	return want.Field == got.Field &&
		want.ExpectedAlphaRaw == got.ExpectedAlphaRaw &&
		want.ExpectedBetaRaw == got.ExpectedBetaRaw &&
		wantObserved == gotObserved &&
		(!wantObserved || *want.ObservedBetaRaw == *got.ObservedBetaRaw) &&
		want.OperatorSeenAt.Equal(got.OperatorSeenAt) &&
		want.CheckedAt.Equal(got.CheckedAt)
}

// PreviewConditionalBatch is the dry run: it converts every requested value
// to the device-side raw target and reads the device once per item to show
// the impact (current -> target in both unit systems). It never writes.
func (c *Channel) PreviewConditionalBatch(ctx context.Context, requests []ConditionalWriteRequest) PreviewBatch {
	preview := PreviewBatch{
		ID:        c.NewID(),
		Channel:   c.Name,
		CreatedAt: c.Now(),
		Confirms:  map[string]*ItemConfirmation{},
	}
	preview.event(c.Now(), "preview_start", fmt.Sprintf("dry run of %d fields; read-only, no device modification", len(requests)))
	for _, request := range requests {
		item := c.previewOne(ctx, request)
		preview.Items = append(preview.Items, item)
		preview.Confirms[item.Field] = &ItemConfirmation{State: ConfirmPending}
		if item.Status != "ok" {
			preview.event(c.Now(), "preview_blocked", item.Field+": "+item.Error)
		}
	}
	preview.Fingerprint = previewFingerprint(preview.Items)
	c.Traces.AddPreviewBatch(preview)
	return preview
}

func (c *Channel) previewOne(ctx context.Context, request ConditionalWriteRequest) PreviewItem {
	item := PreviewItem{Field: request.Field, Requested: request.Value, Status: "ok"}
	mapping, ok := c.Mappings[request.Field]
	if !ok {
		item.Status = "blocked"
		item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), request.Field)
		return item
	}
	item.MappingFingerprint = mappingFingerprint(mapping)
	item.BetaTag = mapping.Beta.Tag
	if err := mapping.Alpha.ValidateRaw(request.Value); err != nil {
		item.Status = "blocked"
		item.Error = err.Error()
		return item
	}
	target, err := mapping.AlphaToBeta(request.Value)
	if err != nil {
		item.Status = "blocked"
		item.Error = err.Error()
		return item
	}
	item.TargetBetaRaw = &target
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	ctx = WithFrameRecorder(ctx, recorder)
	current, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
	if err != nil {
		item.Status = "blocked"
		item.Error = fmt.Sprintf("impact unknown, device read failed: %v", err)
		return item
	}
	item.CurrentBetaRaw = &current
	if alphaRaw, err := mapping.BetaToAlpha(current); err == nil {
		out := float64(alphaRaw)
		item.CurrentAlphaRaw = &out
	}
	if request.Condition != nil {
		expectedBeta, err := mapping.AlphaToBeta(request.Condition.ExpectedAlphaRaw)
		if err != nil {
			item.Status = "blocked"
			item.Error = fmt.Sprintf("condition value not representable on device: %v", err)
			return item
		}
		item.Condition = &ConditionEvidence{
			Field:            mapping.Name,
			ExpectedAlphaRaw: request.Condition.ExpectedAlphaRaw,
			ExpectedBetaRaw:  expectedBeta,
			ObservedBetaRaw:  &current,
			OperatorSeenAt:   request.Condition.ObservedAt,
			CheckedAt:        c.Now(),
		}
		if math.Abs(current-expectedBeta) > 1e-9 {
			item.Status = "blocked"
			item.Error = fmt.Sprintf("condition already unmet: device holds %g but operator saw %g (beta raw)", current, expectedBeta)
		}
	}
	return item
}

// ConfirmPreviewItem approves one previewed item. The approval covers only
// the field, the device value and the conversion rules seen at preview time.
// A withdrawn confirmation can never be re-enabled by a late request.
// Parallel previews may write different fields of the same device; when two
// open previews share a constraint (one writes a field the other's
// precondition reads, or both write the same field) the conflict is reported.
func (c *Channel) ConfirmPreviewItem(previewID, field string) ([]string, error) {
	preview, ok := c.Traces.GetPreviewBatch(previewID)
	if !ok {
		return nil, fmt.Errorf("unknown preview %s", previewID)
	}
	confirmation, ok := preview.Confirms[field]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedField, field)
	}
	if confirmation.State == ConfirmWithdrawn {
		return nil, fmt.Errorf("confirmation for %s was withdrawn and cannot be re-enabled; run a new preview", field)
	}
	if confirmation.State == ConfirmNeedsReconfirm {
		return nil, fmt.Errorf("confirmation for %s is stale; run a new preview before confirming it", field)
	}
	var item *PreviewItem
	for i := range preview.Items {
		if preview.Items[i].Field == field {
			item = &preview.Items[i]
		}
	}
	if item == nil || item.Status != "ok" {
		return nil, fmt.Errorf("preview item %s is blocked and cannot be confirmed", field)
	}
	conflicts := c.crossPreviewConflicts(preview, *item)
	confirmation.State = ConfirmConfirmed
	confirmation.ConfirmedAt = c.Now()
	confirmation.BetaTag = item.BetaTag
	confirmation.RequestedAlphaRaw = item.Requested
	confirmation.TargetBetaRaw = item.TargetBetaRaw
	confirmation.ObservedBetaRaw = item.CurrentBetaRaw
	confirmation.Condition = item.Condition
	confirmation.MappingFingerprint = item.MappingFingerprint
	preview.Conflicts = append(preview.Conflicts, conflicts...)
	preview.event(c.Now(), "item_confirmed", field)
	for _, conflict := range conflicts {
		preview.event(c.Now(), "constraint_conflict", conflict)
	}
	c.Traces.AddPreviewBatch(preview)
	return conflicts, nil
}

// crossPreviewConflicts reports shared constraints between this preview and
// other open previews on the same channel. Writing different fields of the
// same device is fine; overlapping write fields or a write that invalidates
// another preview's precondition is a conflict.
func (c *Channel) crossPreviewConflicts(preview PreviewBatch, item PreviewItem) []string {
	var conflicts []string
	for _, other := range c.Traces.ListPreviewBatches() {
		if other.ID == preview.ID || other.Channel != preview.Channel {
			continue
		}
		for _, otherItem := range other.Items {
			if otherItem.BetaTag == "" || item.BetaTag == "" {
				continue
			}
			if otherItem.BetaTag == item.BetaTag {
				conflicts = append(conflicts, fmt.Sprintf("preview %s also writes %s (%s); shared constraint on the same device field", other.ID, item.Field, item.BetaTag))
				continue
			}
			if item.Condition != nil && otherItem.Field == item.Condition.Field {
				conflicts = append(conflicts, fmt.Sprintf("preview %s writes %s which the precondition of %s reads; shared constraint", other.ID, otherItem.Field, item.Field))
			}
			if otherItem.Condition != nil && otherItem.Condition.Field == item.Field {
				conflicts = append(conflicts, fmt.Sprintf("write to %s invalidates a precondition of preview %s; shared constraint", item.Field, other.ID))
			}
		}
	}
	return conflicts
}

// WithdrawPreviewConfirmation revokes an approval. The withdrawal is
// terminal: ConfirmPreviewItem will reject any later attempt to re-enable it.
func (c *Channel) WithdrawPreviewConfirmation(previewID, field string) error {
	preview, ok := c.Traces.GetPreviewBatch(previewID)
	if !ok {
		return fmt.Errorf("unknown preview %s", previewID)
	}
	confirmation, ok := preview.Confirms[field]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnsupportedField, field)
	}
	now := c.Now()
	confirmation.State = ConfirmWithdrawn
	confirmation.WithdrawnAt = &now
	preview.event(now, "confirmation_withdrawn", field)
	c.Traces.AddPreviewBatch(preview)
	return nil
}

// ExecuteConfirmedPreview applies the confirmed items of a stored preview.
// Before each write the endpoint, mapping rules, requested/confirmed target,
// precondition and device value are compared against the confirmed facts. If
// any fact changed, only the affected item is marked needs_reconfirm and
// skipped; the other confirmed items proceed.
// A lost response is resolved by a read-only read-back, never by resending.
func (c *Channel) ExecuteConfirmedPreview(ctx context.Context, previewID string) (PreviewBatch, error) {
	preview, ok := c.Traces.GetPreviewBatch(previewID)
	if !ok {
		return PreviewBatch{}, fmt.Errorf("unknown preview %s", previewID)
	}
	preview.event(c.Now(), "execute_start", fmt.Sprintf("executing confirmed items of preview %s", previewID))
	for i := range preview.Items {
		item := preview.Items[i]
		confirmation := preview.Confirms[item.Field]
		if confirmation == nil || confirmation.State != ConfirmConfirmed {
			preview.event(c.Now(), "item_skipped", item.Field+": not confirmed")
			continue
		}
		effect := c.executeOne(ctx, &preview, i, confirmation)
		preview.Effects = append(preview.Effects, effect)
		preview.event(c.Now(), "effect_"+effect.Outcome, item.Field)
	}
	c.Traces.AddPreviewBatch(preview)
	return preview, nil
}

func (c *Channel) executeOne(ctx context.Context, preview *PreviewBatch, index int, confirmation *ItemConfirmation) EffectFact {
	item := preview.Items[index]
	effect := EffectFact{Field: item.Field, At: c.Now()}
	mapping, ok := c.Mappings[item.Field]
	if !ok {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemFailed
		return effect
	}
	if mapping.Name != item.Field || confirmation.BetaTag != item.BetaTag || confirmation.BetaTag != mapping.Beta.Tag ||
		confirmation.MappingFingerprint != mappingFingerprint(mapping) || confirmation.MappingFingerprint != item.MappingFingerprint ||
		confirmation.RequestedAlphaRaw != item.Requested || confirmation.TargetBetaRaw == nil || item.TargetBetaRaw == nil ||
		*confirmation.TargetBetaRaw != *item.TargetBetaRaw || !conditionsEqual(confirmation.Condition, item.Condition) {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemNotAttempted
		return effect
	}
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	ctx = WithFrameRecorder(ctx, recorder)
	current, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
	if err != nil {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemFailed
		return effect
	}
	effect.ObservedBetaRaw = &current
	if confirmation.ObservedBetaRaw == nil || math.Abs(current-*confirmation.ObservedBetaRaw) > 1e-9 {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemNotAttempted
		return effect
	}
	if confirmation.Condition != nil && math.Abs(current-confirmation.Condition.ExpectedBetaRaw) > 1e-9 {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemConditionMismatch
		return effect
	}
	target, err := mapping.AlphaToBeta(item.Requested)
	if err != nil || math.Abs(target-*confirmation.TargetBetaRaw) > 1e-9 {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemNotAttempted
		return effect
	}
	effect.SentBetaRaw = &target
	if err := c.Upstream.Write(ctx, mapping.Beta.Tag, target); err != nil {
		if isUnknownWriteError(err) {
			// Response lost: confirm the effect by a real read, never resend.
			verify := c.VerifyWrite(ctx, item.Field, target)
			item.Frames = append(item.Frames, verify.Frames...)
			effect.VerifiedBy = "read_back"
			effect.ObservedBetaRaw = verify.Observed
			switch verify.Outcome {
			case VerifyConfirmed:
				effect.Outcome = ItemApplied
			case VerifyMismatch:
				effect.Outcome = ItemConflict
			default:
				effect.Outcome = ItemPendingConfirm
			}
			preview.Items[index] = item
			return effect
		}
		effect.Outcome = ItemFailed
		preview.Items[index] = item
		return effect
	}
	effect.VerifiedBy = "ack"
	effect.Outcome = ItemApplied
	preview.Items[index] = item
	return effect
}
