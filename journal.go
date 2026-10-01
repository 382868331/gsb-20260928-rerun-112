package gateway

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
)

// BatchJournal persists conditional batch results so that uncertain items
// can still be verified after a gateway restart. Each record keeps the
// original field, the requested alpha raw value, the converted beta raw
// value actually sent, the precondition source value with its observation
// time, and the pre-batch device value; unit conversion never erases the
// original field source or read timestamps.
type BatchJournal struct {
	mu      sync.Mutex
	path    string
	Records map[string]ConditionalBatchResult `json:"records"`
}

// LoadBatchJournal opens (or creates) the journal file at path.
func LoadBatchJournal(path string) (*BatchJournal, error) {
	journal := &BatchJournal{path: path, Records: map[string]ConditionalBatchResult{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return journal, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return journal, nil
	}
	if err := json.Unmarshal(data, &journal.Records); err != nil {
		return nil, err
	}
	return journal, nil
}

// Put stores or replaces a batch record and flushes the journal to disk.
func (j *BatchJournal) Put(result ConditionalBatchResult) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Records[result.ID] = result
	data, err := json.MarshalIndent(j.Records, "", "  ")
	if err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, j.path)
}

// Get returns a stored batch record by ID, e.g. to resume verification of
// unknown items after a restart.
func (j *BatchJournal) Get(id string) (ConditionalBatchResult, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result, ok := j.Records[id]
	return result, ok
}
