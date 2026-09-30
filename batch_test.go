package gateway

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBatchReadMarksNonAtomicSnapshotWithFieldEvidence(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.BatchRead(context.Background(), []string{"temperature", "pressure"})
	if result.Snapshot != ReadSnapshotNonAtomic {
		t.Fatalf("snapshot=%s, must not claim atomic device-time snapshot", result.Snapshot)
	}
	if result.Status != StatusCompleted || len(result.Fields) != 2 {
		t.Fatalf("status=%s fields=%d", result.Status, len(result.Fields))
	}
	prev := time.Time{}
	for _, field := range result.Fields {
		if field.Status != StatusCompleted {
			t.Fatalf("field %s: %s", field.Field, field.Error)
		}
		if field.ReadAt.Before(prev) {
			t.Fatalf("field %s read_at moved backwards", field.Field)
		}
		prev = field.ReadAt
		// Each field needs its own request/response pair as field-level evidence.
		if len(field.Frames) != 2 {
			t.Fatalf("field %s frames=%#v", field.Field, field.Frames)
		}
		if field.Frames[0].Direction != "out" || field.Frames[1].Direction != "in" {
			t.Fatalf("field %s frame directions=%#v", field.Field, field.Frames)
		}
		if field.TraceID == "" {
			t.Fatalf("field %s missing trace id", field.Field)
		}
	}
	if result.Fields[0].Value == nil || *result.Fields[0].Value != 2500 {
		t.Fatalf("temperature alpha raw=%v want 2500", result.Fields[0].Value)
	}
	if result.Fields[1].Value == nil || *result.Fields[1].Value != 0 {
		t.Fatalf("pressure alpha raw=%v want 0", result.Fields[1].Value)
	}
	if _, ok := channel.Traces.GetBatch(result.ID); !ok {
		t.Fatal("batch trace is not queryable")
	}
}

func TestBatchReadStopsAndKeepsEvidenceWhenFieldFails(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.BatchRead(context.Background(), []string{"temperature", "pressure"})
	if result.Status != StatusFailed || len(result.Fields) != 2 {
		t.Fatalf("status=%s fields=%d", result.Status, len(result.Fields))
	}
	if result.Fields[0].Status != StatusCompleted || result.Fields[1].Status != StatusFailed {
		t.Fatalf("statuses=%s,%s", result.Fields[0].Status, result.Fields[1].Status)
	}
}

func TestBatchWritePartialSuccessKeepsPerItemState(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	// 25.0 degC converts exactly; 1000 kPa -> ~145.038 psi is not exactly
	// representable in psi/0.01 int raw, so the second item must be rejected.
	result := channel.BatchWrite(context.Background(), []BatchWriteRequest{
		{Field: "temperature", Value: 2500},
		{Field: "pressure", Value: 1000},
	})
	if result.Status != StatusPartial {
		t.Fatalf("batch status=%s error=%s", result.Status, result.Error)
	}
	if !strings.Contains(result.Error, "do not resubmit") {
		t.Fatalf("error must direct verification, not blind retry: %s", result.Error)
	}
	first, second := result.Items[0], result.Items[1]
	if first.Status != StatusCompleted || first.Attempts != 1 {
		t.Fatalf("first item status=%s attempts=%d", first.Status, first.Attempts)
	}
	if second.Status != StatusFailed || second.Attempts != 1 || second.Error == "" {
		t.Fatalf("second item status=%s error=%q", second.Status, second.Error)
	}
	if !strings.Contains(second.Error, "truncation") {
		t.Fatalf("second item must reject lossy conversion: %s", second.Error)
	}
	// The rejected second item never reached the device.
	if second.Sent != nil || len(second.Frames) != 0 {
		t.Fatalf("rejected item sent frames=%#v sent=%v", second.Frames, second.Sent)
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, first item must stay applied", v)
	}
	if v, _ := device.Value("P1"); v != 0 {
		t.Fatalf("P1=%d, rejected item must not mutate device", v)
	}
}

func TestBatchWriteLostResponseIsVerifiedNotAssumedUnapplied(t *testing.T) {
	// The device commits 1234 but never sends a response.
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "P1": 1450})
	device.ApplyThenDrop["T1"] = true
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.BatchWrite(context.Background(), []BatchWriteRequest{
		{Field: "temperature", Value: 1234},
		// 0 kPa converts exactly to 0 psi raw, so this item completes.
		{Field: "pressure", Value: 0},
	})
	if result.Status != StatusPartial {
		t.Fatalf("status=%s error=%s", result.Status, result.Error)
	}
	lost, applied := result.Items[0], result.Items[1]
	if lost.Status != StatusUnknown || lost.Attempts != 1 {
		t.Fatalf("lost item status=%s attempts=%d", lost.Status, lost.Attempts)
	}
	// Only the outbound write frame exists; no response frame ever came back.
	if len(lost.Frames) != 1 || lost.Frames[0].Direction != "out" {
		t.Fatalf("lost item frames=%#v", lost.Frames)
	}
	if applied.Status != StatusCompleted {
		t.Fatalf("second item status=%s", applied.Status)
	}
	// The side effect really happened even though the response was lost.
	if v, _ := device.Value("T1"); v != 12340 {
		t.Fatalf("T1=%d, device did apply the timed-out write", v)
	}

	// Follow-up path: read back instead of resubmitting the batch.
	verify, err := channel.VerifyBatchItem(context.Background(), result, "temperature")
	if err != nil {
		t.Fatal(err)
	}
	if verify.Outcome != VerifyConfirmed {
		t.Fatalf("outcome=%s error=%s observed=%v", verify.Outcome, verify.Error, verify.Observed)
	}
	if verify.Observed == nil || *verify.Observed != *lost.Sent {
		t.Fatalf("observed=%v expected=%v", verify.Observed, *lost.Sent)
	}
	// Verification is read-only: the only write frame remains the original one.
	if len(lost.Frames) != 1 {
		t.Fatalf("verification triggered another write: %#v", lost.Frames)
	}

	// A completed item cannot be "verified" (it is not uncertain).
	if _, err := channel.VerifyBatchItem(context.Background(), result, "pressure"); err == nil {
		t.Fatal("expected error when verifying a completed item")
	}
}

func TestVerifyReportsMismatchWhenDeviceHoldsOtherValue(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 999})
	channel, _ := newTestChannel(t, device, time.Second)

	verify := channel.VerifyWrite(context.Background(), "temperature", 12340)
	if verify.Outcome != VerifyMismatch || verify.Observed == nil || *verify.Observed != 999 {
		t.Fatalf("outcome=%s observed=%v", verify.Outcome, verify.Observed)
	}
}
