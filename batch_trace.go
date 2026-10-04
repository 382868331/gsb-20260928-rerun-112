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
	mu          sync.RWMutex
	rows        []BatchTrace
	byID        map[string]BatchTrace
	conditional map[string]ConditionalBatchResult
	previews    map[string]PreviewBatch
}

func newBatchStore() *batchStore {
	return &batchStore{byID: map[string]BatchTrace{}, conditional: map[string]ConditionalBatchResult{}, previews: map[string]PreviewBatch{}}
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

// AddPreviewBatch persists the preview / confirmation / effect facts of a
// dry-run batch, replacing any earlier state under the same ID, so an
// interrupted session can reopen and continue confirming or executing.
func (s *TraceStore) AddPreviewBatch(p PreviewBatch) {
	s.batches.mu.Lock()
	defer s.batches.mu.Unlock()
	s.batches.previews[p.ID] = p
	if _, seen := s.batches.byID[p.ID]; !seen {
		s.batches.rows = append(s.batches.rows, BatchTrace{ID: p.ID, Kind: "preview_batch", StartedAt: p.CreatedAt, Status: "preview"})
		s.batches.byID[p.ID] = s.batches.rows[len(s.batches.rows)-1]
	}
}

// GetPreviewBatch reopens a stored preview batch by ID.
func (s *TraceStore) GetPreviewBatch(id string) (PreviewBatch, bool) {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	p, ok := s.batches.previews[id]
	return p, ok
}

// ListPreviewBatches returns all stored preview batches, e.g. to detect
// shared constraints between parallel batches on the same channel.
func (s *TraceStore) ListPreviewBatches() []PreviewBatch {
	s.batches.mu.RLock()
	defer s.batches.mu.RUnlock()
	out := make([]PreviewBatch, 0, len(s.batches.previews))
	for _, p := range s.batches.previews {
		out = append(out, p)
	}
	return out
}
