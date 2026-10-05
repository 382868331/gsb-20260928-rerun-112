package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
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
	Field           string             `json:"field"`
	BetaTag         string             `json:"beta_tag"`
	Requested       float64            `json:"requested_alpha_raw"`
	TargetBetaRaw   *float64           `json:"target_beta_raw,omitempty"`
	CurrentBetaRaw  *float64           `json:"current_beta_raw,omitempty"`
	CurrentAlphaRaw *float64           `json:"current_alpha_raw,omitempty"`
	Condition       *ConditionEvidence `json:"condition,omitempty"`
	Status          string             `json:"status"` // "ok" or "blocked"
	Error           string             `json:"error,omitempty"`
	Frames          []TraceFrame       `json:"frames"`
}

// ItemConfirmation binds the operator's approval to exactly the facts seen at
// preview time: this field, the device value read then, and the conversion
// rules (mapping fingerprint) in effect then.
type ItemConfirmation struct {
	State           string     `json:"state"`
	ObservedBetaRaw *float64   `json:"observed_beta_raw,omitempty"`
	TargetBetaRaw   *float64   `json:"target_beta_raw,omitempty"`
	Fingerprint     string     `json:"fingerprint"`
	ConfirmedAt     time.Time  `json:"confirmed_at,omitempty"`
	WithdrawnAt     *time.Time `json:"withdrawn_at,omitempty"`
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

// mappingFingerprint identifies the conversion rules currently in effect
// across the whole channel. Every map entry is hashed (sorted by key) so
// that updating any single registered key is always detected. If any
// endpoint spec changes, previews confirmed under the old rules are no
// longer valid.
func (c *Channel) mappingFingerprint() string {
	keys := make([]string, 0, len(c.Mappings))
	for key := range c.Mappings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, key := range keys {
		_, _ = h.Write([]byte(key))
		data, _ := json.Marshal(c.Mappings[key])
		_, _ = h.Write(data)
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

// fieldFingerprint identifies the conversion rules of one field: the full
// endpoint specs (data type, unit, scale, offset, range, endianness, tags)
// that decide how the confirmed value is converted and written.
func fieldFingerprint(mapping FieldMapping) string {
	data, _ := json.Marshal(mapping)
	h := fnv.New64a()
	_, _ = h.Write(data)
	return fmt.Sprintf("%016x", h.Sum64())
}

// PreviewConditionalBatch is the dry run: it converts every requested value
// to the device-side raw target and reads the device once per item to show
// the impact (current -> target in both unit systems). It never writes.
func (c *Channel) PreviewConditionalBatch(ctx context.Context, requests []ConditionalWriteRequest) PreviewBatch {
	preview := PreviewBatch{
		ID:          c.NewID(),
		Channel:     c.Name,
		CreatedAt:   c.Now(),
		Fingerprint: c.mappingFingerprint(),
		Confirms:    map[string]*ItemConfirmation{},
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
	var item *PreviewItem
	for i := range preview.Items {
		if preview.Items[i].Field == field {
			item = &preview.Items[i]
		}
	}
	if item == nil || item.Status != "ok" {
		return nil, fmt.Errorf("preview item %s is blocked and cannot be confirmed", field)
	}
	mapping, ok := c.Mappings[field]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedField, field)
	}
	conflicts := c.crossPreviewConflicts(preview, *item)
	confirmation.State = ConfirmConfirmed
	confirmation.ConfirmedAt = c.Now()
	confirmation.ObservedBetaRaw = item.CurrentBetaRaw
	confirmation.TargetBetaRaw = item.TargetBetaRaw
	confirmation.Fingerprint = fieldFingerprint(mapping)
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
// Before each write the device value and the conversion rules are compared
// against the confirmed facts: if either changed, only the affected items are
// marked needs_reconfirm and skipped; the other confirmed items proceed.
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
	// Re-validate this item against every fact the operator confirmed,
	// before any device traffic: the conversion rules of this field and
	// the exact device-side value that was approved.
	if confirmation.Fingerprint != fieldFingerprint(mapping) {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemNotAttempted
		return effect
	}
	target, err := mapping.AlphaToBeta(item.Requested)
	if err != nil {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemFailed
		return effect
	}
	if confirmation.TargetBetaRaw == nil || math.Abs(target-*confirmation.TargetBetaRaw) > 1e-9 {
		confirmation.State = ConfirmNeedsReconfirm
		effect.Outcome = ItemNotAttempted
		return effect
	}
	// Send exactly the value the operator confirmed.
	target = *confirmation.TargetBetaRaw
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
