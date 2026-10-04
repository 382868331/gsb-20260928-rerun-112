package gateway

import (
	"sync"
	"time"
)

// BatchTrace is the index entry for one logical multi-field operation.
// Per-field evidence (values, frames, errors, trace ids) lives in the
// BatchReadResult / BatchWriteResult returned to the caller.
type BatchTrace struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	StartedAt time.Time `json:"started_at"`
	Status    string    `json:"status"`
	Snapshot  string    `json:"snapshot,omitempty"`
}

type batchStore struct {
	mu            sync.RWMutex
	rows          []BatchTrace
	byID          map[string]BatchTrace
	conditional   map[string]ConditionalBatchResult
	previews      map[string]PreviewResult
	confirmations map[string]Confirmation
	withdrawn     map[string]bool
	executed      map[string]bool
}

func newBatchStore() *batchStore {
	return &batchStore{
		byID:          map[string]BatchTrace{},
		conditional:   map[string]ConditionalBatchResult{},
		previews:      map[string]PreviewResult{},
		confirmations: map[string]Confirmation{},
		withdrawn:     map[string]bool{},
		executed:      map[string]bool{},
	}
}

func (s *batchStore) add(t BatchTrace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, t)
	s.byID[t.ID] = t
}

func (s *batchStore) get(id string) (BatchTrace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.byID[id]
	return t, ok
}

func (s *batchStore) list() []BatchTrace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]BatchTrace, len(s.rows))
	copy(out, s.rows)
	return out
}

func (s *TraceStore) AddBatch(t BatchTrace)                 { s.batches.add(t) }
func (s *TraceStore) GetBatch(id string) (BatchTrace, bool) { return s.batches.get(id) }
func (s *TraceStore) ListBatches() []BatchTrace             { return s.batches.list() }

// AddConditionalBatch persists the full per-item evidence of a conditional
// batch write, replacing any earlier state under the same ID, so a reopened
// session can keep resolving uncertain items.
func (s *TraceStore) AddConditionalBatch(r ConditionalBatchResult) {
	s.batches.mu.Lock()
	defer s.batches.mu.Unlock()
	s.batches.conditional[r.ID] = r
	s.batches.rows = append(s.batches.rows, BatchTrace{ID: r.ID, Kind: "conditional_batch_write", StartedAt: r.StartedAt, Status: r.Status})
	s.batches.byID[r.ID] = s.batches.rows[len(s.batches.rows)-1]
}

// GetConditionalBatch reopens a stored conditional batch by ID.
func (s *TraceStore) GetConditionalBatch(id string) (ConditionalBatchResult, bool) {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	r, ok := s.batches.conditional[id]
	return r, ok
}

// AddPreview persists the dry-run facts so the session can be reopened
// before a human confirms anything.
func (s *TraceStore) AddPreview(r PreviewResult) {
	s.batches.mu.Lock()
	defer s.batches.mu.Unlock()
	s.batches.previews[r.ID] = r
	s.batches.rows = append(s.batches.rows, BatchTrace{ID: r.ID, Kind: "conditional_batch_preview", StartedAt: r.CreatedAt, Status: r.Status})
	s.batches.byID[r.ID] = s.batches.rows[len(s.batches.rows)-1]
}

// GetPreview reopens a stored preview by ID.
func (s *TraceStore) GetPreview(id string) (PreviewResult, bool) {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	r, ok := s.batches.previews[id]
	return r, ok
}

// AddConfirmation persists a human decision, replacing any earlier state
// under the same ID (e.g. a later withdrawal).
func (s *TraceStore) AddConfirmation(c Confirmation) {
	s.batches.mu.Lock()
	defer s.batches.mu.Unlock()
	s.batches.confirmations[c.ID] = c
	s.batches.rows = append(s.batches.rows, BatchTrace{ID: c.ID, Kind: "conditional_batch_confirmation", StartedAt: c.CreatedAt, Status: "recorded"})
	s.batches.byID[c.ID] = s.batches.rows[len(s.batches.rows)-1]
}

// GetConfirmation reopens a stored confirmation by ID.
func (s *TraceStore) GetConfirmation(id string) (Confirmation, bool) {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	c, ok := s.batches.confirmations[id]
	return c, ok
}

// WithdrawPreview tombstones a preview so no late confirmation can ever
// re-enable it.
func (s *TraceStore) WithdrawPreview(previewID string) {
	s.batches.mu.Lock()
	defer s.batches.mu.Unlock()
	s.batches.withdrawn[previewID] = true
}

// IsPreviewWithdrawn reports whether the preview was tombstoned.
func (s *TraceStore) IsPreviewWithdrawn(previewID string) bool {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	return s.batches.withdrawn[previewID]
}

// MarkPreviewExecuted closes a preview after its execution ran.
func (s *TraceStore) MarkPreviewExecuted(previewID string) {
	s.batches.mu.Lock()
	defer s.batches.mu.Unlock()
	s.batches.executed[previewID] = true
}

// previewConflicts reports other in-flight previews on the same channel that
// touch the same device constraint (beta tag). Previews on different fields
// of the same device never conflict; executed or withdrawn previews are
// closed and no longer constrain anything.
func (s *TraceStore) previewConflicts(selfID, channel string, items []PreviewItem) []PreviewConflict {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	tags := map[string]string{}
	for _, item := range items {
		if item.Status == PreviewReady && item.BetaTag != "" {
			tags[item.BetaTag] = item.Field
		}
	}
	var out []PreviewConflict
	for id, other := range s.batches.previews {
		if id == selfID || other.Channel != channel || s.batches.withdrawn[id] || s.batches.executed[id] {
			continue
		}
		seen := map[string]bool{}
		for _, item := range other.Items {
			field, clash := tags[item.BetaTag]
			if !clash || item.BetaTag == "" || seen[item.BetaTag] {
				continue
			}
			seen[item.BetaTag] = true
			out = append(out, PreviewConflict{OtherPreviewID: id, Field: field, BetaTag: item.BetaTag})
		}
	}
	return out
}
