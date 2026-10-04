package gateway

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"
)

// Review-flow item status: an item whose facts changed between confirmation
// and execution is sent back for a fresh human decision instead of being
// executed or silently dropped.
const ItemNeedsReconfirm = "needs_reconfirm"

// Preview item outcomes.
const (
	// PreviewReady: the target converts cleanly and the device was read;
	// the item can be confirmed.
	PreviewReady = "ready"
	// PreviewRejected: the request can never be executed as given (unknown
	// field, out of range, not exactly representable). No device write was
	// sent; the device was at most read.
	PreviewRejected = "rejected"
)

var (
	ErrPreviewWithdrawn    = errors.New("preview was withdrawn and cannot be confirmed again")
	ErrConfirmationStale   = errors.New("confirmation does not match the current preview facts")
	ErrConfirmationRevoked = errors.New("confirmation was withdrawn")
)

// PreviewItem is the dry-run evidence for one field: the unit-converted
// target, the device state read at preview time, and the conversion-rule
// fingerprint, so a later execution can tell exactly what changed.
type PreviewItem struct {
	Field              string       `json:"field"`
	BetaTag            string       `json:"beta_tag"`
	RequestedAlphaRaw  float64      `json:"requested_alpha_raw"`
	TargetBetaRaw      *float64     `json:"target_beta_raw,omitempty"`
	Canonical          *float64     `json:"canonical_value,omitempty"`
	CurrentBetaRaw     *float64     `json:"current_beta_raw,omitempty"`
	CurrentAlphaRaw    *float64     `json:"current_alpha_raw,omitempty"`
	Impact             string       `json:"impact,omitempty"`
	MappingFingerprint string       `json:"mapping_fingerprint"`
	Status             string       `json:"status"`
	Error              string       `json:"error,omitempty"`
	ReadAt             time.Time    `json:"read_at"`
	Frames             []TraceFrame `json:"frames"`
}

// PreviewConflict reports that another in-flight preview touches the same
// device constraint (the same beta tag). Parallel previews on different
// fields of the same device are fine and never reported.
type PreviewConflict struct {
	OtherPreviewID string `json:"other_preview_id"`
	Field          string `json:"field"`
	BetaTag        string `json:"beta_tag"`
}

// PreviewResult is the dry-run fact record. It never modifies the device:
// the only device traffic is read-only.
type PreviewResult struct {
	ID          string            `json:"id"`
	Channel     string            `json:"channel"`
	Operation   string            `json:"operation"`
	Status      string            `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	Fingerprint string            `json:"fingerprint"`
	Items       []PreviewItem     `json:"items"`
	Conflicts   []PreviewConflict `json:"conflicts,omitempty"`
	Events      []TraceEvent      `json:"events"`
}

// Confirmation is the human decision fact record: exactly the fields of one
// preview the operator approved, bound to the preview facts fingerprint they
// saw. A withdrawn confirmation is tombstoned; a late request can never
// re-enable it.
type Confirmation struct {
	ID               string    `json:"id"`
	PreviewID        string    `json:"preview_id"`
	Fields           []string  `json:"fields"`
	FactsFingerprint string    `json:"facts_fingerprint"`
	CreatedAt        time.Time `json:"created_at"`
	Withdrawn        bool      `json:"withdrawn"`
	WithdrawnAt      time.Time `json:"withdrawn_at,omitempty"`
}

func (r *PreviewResult) event(at time.Time, stage, message string) {
	r.Events = append(r.Events, TraceEvent{At: at, Stage: stage, Message: message})
}

// mappingFingerprint identifies the conversion rules of one field. If any
// unit, scale, offset, range or data type changes, the fingerprint changes
// and previously confirmed items must be re-confirmed.
func mappingFingerprint(m FieldMapping) string {
	h := fnv.New64a()
	for _, s := range []EndpointSpec{m.Alpha, m.Beta} {
		fmt.Fprintf(h, "%s|%s|%s|%s|%g|%g|%g|%g\n", s.Protocol, s.Tag, s.DataType, s.Unit, s.Scale, s.Offset, s.MinRaw, s.MaxRaw)
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

// previewFingerprint binds a confirmation to the exact facts the operator
// saw: fields, requested and converted targets, device readings and the
// conversion rules in force at preview time.
func previewFingerprint(items []PreviewItem) string {
	h := fnv.New64a()
	for _, item := range items {
		fmt.Fprintf(h, "%s|%s|%g|%s|%s|", item.Field, item.Status, item.RequestedAlphaRaw, item.MappingFingerprint, item.ReadAt.UTC().Format(time.RFC3339Nano))
		if item.TargetBetaRaw != nil {
			fmt.Fprintf(h, "t%g|", *item.TargetBetaRaw)
		}
		if item.CurrentBetaRaw != nil {
			fmt.Fprintf(h, "c%g|", *item.CurrentBetaRaw)
		}
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

// PreviewConditionalBatch is the dry run: for every requested field it
// converts the target to device units, reads the device once (read-only) to
// capture the current value and the impact, and records the conversion-rule
// fingerprint. Nothing is written. The result is persisted so the session
// can be reopened before confirming.
func (c *Channel) PreviewConditionalBatch(ctx context.Context, requests []ConditionalWriteRequest) PreviewResult {
	result := PreviewResult{
		ID:        c.NewID(),
		Channel:   c.Name,
		Operation: OpWrite,
		CreatedAt: c.Now(),
	}
	result.event(c.Now(), "preview_start", fmt.Sprintf("dry run of %d fields; read-only, no device modification", len(requests)))
	ready := 0
	for _, request := range requests {
		item := c.previewOne(ctx, request)
		result.Items = append(result.Items, item)
		if item.Status == PreviewReady {
			ready++
		} else {
			result.event(c.Now(), "item_rejected", item.Field+": "+item.Error)
		}
	}
	result.Conflicts = c.Traces.previewConflicts(result.ID, c.Name, result.Items)
	for _, conflict := range result.Conflicts {
		result.event(c.Now(), "preview_conflict", fmt.Sprintf("field %s shares device constraint %s with preview %s", conflict.Field, conflict.BetaTag, conflict.OtherPreviewID))
	}
	result.Fingerprint = previewFingerprint(result.Items)
	switch {
	case ready == len(result.Items):
		result.Status = StatusCompleted
	case ready == 0:
		result.Status = StatusFailed
	default:
		result.Status = StatusPartial
	}
	c.Traces.AddPreview(result)
	return result
}

func (c *Channel) previewOne(ctx context.Context, request ConditionalWriteRequest) PreviewItem {
	item := PreviewItem{Field: request.Field, RequestedAlphaRaw: request.Value, ReadAt: c.Now()}
	mapping, ok := c.Mappings[request.Field]
	if !ok {
		item.Status = PreviewRejected
		item.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), request.Field)
		return item
	}
	item.BetaTag = mapping.Beta.Tag
	item.MappingFingerprint = mappingFingerprint(mapping)
	if err := mapping.Alpha.ValidateRaw(request.Value); err != nil {
		item.Status = PreviewRejected
		item.Error = err.Error()
		return item
	}
	target, err := mapping.AlphaToBeta(request.Value)
	if err != nil {
		item.Status = PreviewRejected
		item.Error = err.Error()
		return item
	}
	item.TargetBetaRaw = &target
	canonical, _ := fromEndpoint(mapping.Alpha, request.Value)
	item.Canonical = &canonical
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	current, err := c.Upstream.Read(WithFrameRecorder(ctx, recorder), mapping.Beta.Tag)
	item.ReadAt = c.Now()
	if err != nil {
		item.Status = PreviewRejected
		item.Error = fmt.Sprintf("device state unreadable (%v); target computed but impact unknown", err)
		return item
	}
	item.CurrentBetaRaw = &current
	if alphaRaw, err := mapping.BetaToAlpha(current); err == nil {
		alpha := float64(alphaRaw)
		item.CurrentAlphaRaw = &alpha
	}
	item.Impact = fmt.Sprintf("%s (%s): %g %s -> %g %s (beta raw %g -> %g)",
		mapping.Name, mapping.Beta.Tag, current, mapping.Beta.Unit, target, mapping.Beta.Unit, current, target)
	item.Status = PreviewReady
	return item
}

// ConfirmPreview records the human approval for exactly the fields and facts
// shown in one preview. seenFingerprint must equal the preview fingerprint
// the operator reviewed; a stale or reconstructed approval is rejected. A
// preview whose confirmation was withdrawn is tombstoned: late confirmations
// can never re-enable it.
func (c *Channel) ConfirmPreview(previewID string, fields []string, seenFingerprint string) (Confirmation, error) {
	preview, ok := c.Traces.GetPreview(previewID)
	if !ok {
		return Confirmation{}, fmt.Errorf("unknown preview %s", previewID)
	}
	if c.Traces.IsPreviewWithdrawn(previewID) {
		return Confirmation{}, fmt.Errorf("%w: %s", ErrPreviewWithdrawn, previewID)
	}
	if seenFingerprint != preview.Fingerprint {
		return Confirmation{}, fmt.Errorf("%w: saw %s, preview now %s", ErrConfirmationStale, seenFingerprint, preview.Fingerprint)
	}
	ready := map[string]bool{}
	for _, item := range preview.Items {
		if item.Status == PreviewReady {
			ready[item.Field] = true
		}
	}
	confirmed := Confirmation{
		ID:               c.NewID(),
		PreviewID:        previewID,
		FactsFingerprint: seenFingerprint,
		CreatedAt:        c.Now(),
	}
	for _, field := range fields {
		if !ready[field] {
			return Confirmation{}, fmt.Errorf("field %s is not a ready item of preview %s", field, previewID)
		}
		confirmed.Fields = append(confirmed.Fields, field)
	}
	if len(confirmed.Fields) == 0 {
		return Confirmation{}, errors.New("confirmation covers no fields")
	}
	c.Traces.AddConfirmation(confirmed)
	return confirmed, nil
}

// WithdrawConfirmation revokes a confirmation and tombstones its preview:
// neither the confirmation nor any late confirmation of the same preview can
// be executed afterwards.
func (c *Channel) WithdrawConfirmation(confirmationID string) error {
	confirmation, ok := c.Traces.GetConfirmation(confirmationID)
	if !ok {
		return fmt.Errorf("unknown confirmation %s", confirmationID)
	}
	confirmation.Withdrawn = true
	confirmation.WithdrawnAt = c.Now()
	c.Traces.AddConfirmation(confirmation)
	c.Traces.WithdrawPreview(confirmation.PreviewID)
	return nil
}

// ExecuteConfirmedBatch runs the confirmed items of a preview. Before each
// write it re-checks the facts the operator approved: the conversion rules
// (mapping fingerprint), the device state (read-back against the preview
// reading) and the converted target. Only items whose facts changed are
// marked needs_reconfirm and skipped; every other confirmed item proceeds.
// A lost response becomes pending_confirm and is resolved by read-back only,
// never by resending. The execution facts are persisted as a conditional
// batch so the session can be reopened and continued.
func (c *Channel) ExecuteConfirmedBatch(ctx context.Context, confirmationID string) (ConditionalBatchResult, error) {
	confirmation, ok := c.Traces.GetConfirmation(confirmationID)
	if !ok {
		return ConditionalBatchResult{}, fmt.Errorf("unknown confirmation %s", confirmationID)
	}
	if confirmation.Withdrawn {
		return ConditionalBatchResult{}, fmt.Errorf("%w: %s", ErrConfirmationRevoked, confirmationID)
	}
	preview, ok := c.Traces.GetPreview(confirmation.PreviewID)
	if !ok {
		return ConditionalBatchResult{}, fmt.Errorf("unknown preview %s", confirmation.PreviewID)
	}
	if c.Traces.IsPreviewWithdrawn(preview.ID) {
		return ConditionalBatchResult{}, fmt.Errorf("%w: %s", ErrPreviewWithdrawn, preview.ID)
	}
	byField := map[string]PreviewItem{}
	for _, item := range preview.Items {
		byField[item.Field] = item
	}
	result := ConditionalBatchResult{
		ID:        c.NewID(),
		Channel:   c.Name,
		Operation: OpWrite,
		StartedAt: c.Now(),
	}
	result.event(c.Now(), "batch_execute", fmt.Sprintf("executing confirmation %s of preview %s (%d confirmed fields)", confirmation.ID, preview.ID, len(confirmation.Fields)))
	for _, field := range confirmation.Fields {
		item := c.executeConfirmedOne(ctx, byField[field])
		result.Items = append(result.Items, item)
		result.event(c.Now(), "item_"+item.Status, item.Field+": "+item.Error)
	}
	result.summarize(c.Now)
	c.Traces.AddConditionalBatch(result)
	c.Traces.MarkPreviewExecuted(preview.ID)
	return result, nil
}

func (c *Channel) executeConfirmedOne(ctx context.Context, preview PreviewItem) ConditionalItem {
	item := ConditionalItem{
		Field:     preview.Field,
		BetaTag:   preview.BetaTag,
		Requested: preview.RequestedAlphaRaw,
		Attempts:  1,
		TraceID:   c.NewID(),
		Condition: &ConditionEvidence{
			Field:            preview.Field,
			ExpectedAlphaRaw: valueOrZero(preview.CurrentAlphaRaw),
			OperatorSeenAt:   preview.ReadAt,
		},
	}
	if preview.CurrentBetaRaw != nil {
		item.Condition.ExpectedBetaRaw = *preview.CurrentBetaRaw
	}
	reconfirm := func(reason string) ConditionalItem {
		item.Status = ItemNeedsReconfirm
		item.Error = reason + "; facts changed since confirmation, operator must re-confirm this item"
		return item
	}
	mapping, ok := c.Mappings[preview.Field]
	if !ok {
		return reconfirm(fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), preview.Field))
	}
	if mappingFingerprint(mapping) != preview.MappingFingerprint {
		return reconfirm("conversion rules changed since preview")
	}
	target, err := mapping.AlphaToBeta(preview.RequestedAlphaRaw)
	if err != nil || preview.TargetBetaRaw == nil || math.Abs(target-*preview.TargetBetaRaw) > 1e-9 {
		return reconfirm("converted target changed since preview")
	}
	item.Sent = &target
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		item.Frames = append(item.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	ctx = WithFrameRecorder(ctx, recorder)
	current, err := c.Upstream.Read(ctx, mapping.Beta.Tag)
	item.Condition.CheckedAt = c.Now()
	if err != nil {
		return reconfirm(fmt.Sprintf("device state unreadable (%v)", err))
	}
	item.Condition.ObservedBetaRaw = &current
	if preview.CurrentBetaRaw == nil || math.Abs(current-*preview.CurrentBetaRaw) > 1e-9 {
		return reconfirm(fmt.Sprintf("device holds %g but preview saw %v", current, preview.CurrentBetaRaw))
	}
	if err := c.Upstream.Write(ctx, mapping.Beta.Tag, target); err != nil {
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

func valueOrZero(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}
