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
	mu   sync.RWMutex
	rows []BatchTrace
	byID map[string]BatchTrace
}

func newBatchStore() *batchStore {
	return &batchStore{byID: map[string]BatchTrace{}}
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
