package gateway

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConditionalBatchWriteSkipsMismatchAndAppliesMatch(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 100})
	channel, _ := newTestChannel(t, device, time.Second)
	seenAt := time.Now().Add(-time.Minute)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		// Operator saw 250.0 degC (alpha raw 2500) and it still matches.
		{Field: "temperature", Value: 2600, Expect: &Precondition{ExpectedAlphaRaw: 2500, ObservedAt: seenAt}},
		// Operator saw 0 kPa but the device holds 100 psi raw: not written.
		{Field: "pressure", Value: 0, Expect: &Precondition{ExpectedAlphaRaw: 0, ObservedAt: seenAt}},
	})
	if result.Status != StatusPartial {
		t.Fatalf("status=%s error=%s", result.Status, result.Error)
	}
	applied, skipped := result.Items[0], result.Items[1]
	if applied.Status != StatusCompleted {
		t.Fatalf("applied item status=%s error=%s", applied.Status, applied.Error)
	}
	if skipped.Status != StatusConditionMismatch || skipped.Observed == nil || *skipped.Observed != 100 {
		t.Fatalf("skipped item status=%s observed=%v", skipped.Status, skipped.Observed)
	}
	if skipped.ConditionExpected == nil || skipped.ConditionSeenAt == nil || !skipped.ConditionSeenAt.Equal(seenAt) {
		t.Fatalf("skipped item lost precondition source: %#v", skipped)
	}
	if v, _ := device.Value("T1"); v != 26000 {
		t.Fatalf("T1=%d, matching item must be applied", v)
	}
	if v, _ := device.Value("P1"); v != 100 {
		t.Fatalf("P1=%d, mismatched item must not mutate the device", v)
	}
	// The skipped item performed one read-only check and no write.
	if len(skipped.Frames) != 2 || skipped.Frames[0].Direction != "out" {
		t.Fatalf("skipped item frames=%#v", skipped.Frames)
	}
}

func TestConditionalBatchWriteUnreadablePreconditionIsNotAttempted(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "P1": 0})
	device.CloseOn["P1"] = true
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "pressure", Value: 0, Expect: &Precondition{ExpectedAlphaRaw: 0, ObservedAt: time.Now()}},
	})
	item := result.Items[0]
	if item.Status != StatusNotAttempted {
		t.Fatalf("status=%s, unreadable precondition must leave the item not attempted", item.Status)
	}
	if v, _ := device.Value("P1"); v != 0 {
		t.Fatalf("P1=%d, not-attempted item must not mutate the device", v)
	}
}

func TestContinueBatchVerifiesUnknownAndFlagsExternalChange(t *testing.T) {
	// T1 write loses its response after applying; P1 loses its response and
	// the device then changes on its own before anyone verifies.
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "P1": 0})
	device.ApplyThenDrop["T1"] = true
	device.ApplyThenDrop["P1"] = true
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 1234},
		{Field: "pressure", Value: 0},
	})
	if result.Items[0].Status != StatusUnknown || result.Items[1].Status != StatusUnknown {
		t.Fatalf("statuses=%s,%s", result.Items[0].Status, result.Items[1].Status)
	}
	// Device drifts on its own after the lost P1 write.
	device.Hooks["P1"] = nil
	device.ApplyThenDrop["P1"] = false
	if _, ok := device.Value("P1"); !ok {
		t.Fatal("P1 missing")
	}
	device.Hooks["P1"] = func(req BetaRequest) BetaResponse {
		if req.Op == BetaRead {
			return BetaResponse{Status: BetaOK, Tag: req.Tag, Value: 777}
		}
		return BetaResponse{Status: BetaOK, Tag: req.Tag, Value: req.Value}
	}

	channel.ContinueBatch(context.Background(), &result)
	lost, drifted := result.Items[0], result.Items[1]
	if lost.Status != StatusCompleted {
		t.Fatalf("lost item status=%s, read-back should confirm it without rewriting", lost.Status)
	}
	if drifted.Status != StatusConflict {
		t.Fatalf("drifted item status=%s, external change must become a conflict", drifted.Status)
	}
	// No blind rewrite: T1 was written exactly once, P1 was never rewritten.
	writes := 0
	for _, frame := range lost.Frames {
		if frame.Direction == "out" {
			writes++
		}
	}
	if writes != 2 { // one conditional read + one write, no second write
		t.Fatalf("T1 outbound frames=%d, continue must not rewrite a confirmed item", writes)
	}
}

func TestRevertBatchRestoresOnlyWhileDeviceHoldsWrittenValue(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 100})
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 0},
	})
	if result.Status != StatusCompleted {
		t.Fatalf("status=%s error=%s", result.Status, result.Error)
	}
	// P1 drifts externally after the batch applied 0.
	device.Hooks["P1"] = func(req BetaRequest) BetaResponse {
		if req.Op == BetaRead {
			return BetaResponse{Status: BetaOK, Tag: req.Tag, Value: 555}
		}
		return BetaResponse{Status: BetaOK, Tag: req.Tag, Value: req.Value}
	}

	channel.RevertBatch(context.Background(), &result, RevertToPreBatchValue)
	reverted, drifted := result.Items[0], result.Items[1]
	if reverted.Status != StatusCompleted {
		t.Fatalf("reverted item status=%s error=%s", reverted.Status, reverted.Error)
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, revert must restore the pre-batch device value", v)
	}
	if drifted.Status != StatusConflict {
		t.Fatalf("drifted item status=%s, revert must not overwrite external changes", drifted.Status)
	}
	if !strings.Contains(drifted.Error, "external change") {
		t.Fatalf("drifted item error=%q", drifted.Error)
	}
}

func TestJournalPersistsUncertainItemsAcrossRestart(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.ApplyThenDrop["T1"] = true
	channel, _ := newTestChannel(t, device, time.Second)
	path := filepath.Join(t.TempDir(), "journal.json")
	journal, err := LoadBatchJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	channel.Journal = journal
	seenAt := time.Now().Add(-time.Minute)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 1234, Expect: &Precondition{ExpectedAlphaRaw: 0, ObservedAt: seenAt}},
	})
	if result.Items[0].Status != StatusUnknown {
		t.Fatalf("status=%s", result.Items[0].Status)
	}

	// Simulate a restart: reload the journal from disk and resume verifying.
	reloaded, err := LoadBatchJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	restored, ok := reloaded.Get(result.ID)
	if !ok {
		t.Fatal("batch record missing after reload")
	}
	item := restored.Items[0]
	if item.Sent == nil || *item.Sent != 12340 {
		t.Fatalf("reloaded item lost sent beta raw: %#v", item.Sent)
	}
	if item.ConditionSeenAt == nil || !item.ConditionSeenAt.Equal(seenAt) {
		t.Fatalf("reloaded item lost precondition read time: %#v", item.ConditionSeenAt)
	}
	if item.PreValue == nil || *item.PreValue != 0 {
		t.Fatalf("reloaded item lost pre-batch device value: %#v", item.PreValue)
	}
	verify, err := channel.VerifyConditionalItem(context.Background(), &restored, "temperature")
	if err != nil {
		t.Fatal(err)
	}
	if verify.Outcome != VerifyConfirmed || restored.Items[0].Status != StatusCompleted {
		t.Fatalf("outcome=%s status=%s", verify.Outcome, restored.Items[0].Status)
	}
}
