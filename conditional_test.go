package gateway

import (
	"context"
	"testing"
	"time"
)

func TestConditionalBatchWriteAppliesOnlyWhenConditionMatches(t *testing.T) {
	// T1 holds 250.00 degC (alpha raw 2500); P1 holds 0 psi.
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)
	seen := time.Now().Add(-time.Minute)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600, Condition: &WriteCondition{ExpectedAlphaRaw: 2500, ObservedAt: seen}},
		// Operator thinks pressure is 100 kPa, device actually holds 0.
		{Field: "pressure", Value: 137.89514, Condition: &WriteCondition{ExpectedAlphaRaw: 68.94757, ObservedAt: seen}},
	})
	if result.Status != StatusPartial {
		t.Fatalf("status=%s error=%s", result.Status, result.Error)
	}
	applied, skipped := result.Items[0], result.Items[1]
	if applied.Status != ItemApplied {
		t.Fatalf("first item=%s error=%s", applied.Status, applied.Error)
	}
	if skipped.Status != ItemConditionMismatch {
		t.Fatalf("second item=%s error=%s", skipped.Status, skipped.Error)
	}
	// The mismatched item only produced the condition read round trip
	// (out+in); no write frame was ever sent.
	if len(skipped.Frames) != 2 {
		t.Fatalf("mismatched item frames=%#v, want read round trip only", skipped.Frames)
	}
	if v, _ := device.Value("T1"); v != 26000 {
		t.Fatalf("T1=%d, conditional write not applied", v)
	}
	if v, _ := device.Value("P1"); v != 0 {
		t.Fatalf("P1=%d, mismatched item must not modify the device", v)
	}
	// Evidence keeps source field, both units and both timestamps.
	ev := applied.Condition
	if ev == nil || ev.Field != "temperature" || ev.ExpectedAlphaRaw != 2500 || ev.ExpectedBetaRaw != 25000 {
		t.Fatalf("evidence=%+v", ev)
	}
	if !ev.OperatorSeenAt.Equal(seen) || ev.CheckedAt.IsZero() || ev.ObservedBetaRaw == nil || *ev.ObservedBetaRaw != 25000 {
		t.Fatalf("evidence timing/observation=%+v", ev)
	}
	if _, ok := channel.Traces.GetConditionalBatch(result.ID); !ok {
		t.Fatal("conditional batch is not reopenable from the trace store")
	}
}

func TestConditionalBatchWriteUnreadableConditionLeavesItemNotAttempted(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 100})
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		// P1 does not exist on the device: the precondition read fails.
		{Field: "pressure", Value: 68.94757, Condition: &WriteCondition{ExpectedAlphaRaw: 0}},
	})
	item := result.Items[0]
	if item.Status != ItemNotAttempted {
		t.Fatalf("status=%s error=%s", item.Status, item.Error)
	}
	if _, ok := device.Value("P1"); ok {
		t.Fatal("device must remain untouched")
	}
}

func TestContinueConditionalBatchResolvesLostResponseWithoutRewrite(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.ApplyThenDrop["T1"] = true // write commits, response is lost
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 1234},
	})
	if result.Items[0].Status != ItemPendingConfirm {
		t.Fatalf("status=%s", result.Items[0].Status)
	}
	if v, _ := device.Value("T1"); v != 12340 {
		t.Fatalf("T1=%d, side effect did happen", v)
	}

	// Reopen the stored batch and resolve: read-back confirms, no rewrite.
	reopened, err := channel.ContinueConditionalBatch(context.Background(), result.ID, []string{"temperature"})
	if err != nil {
		t.Fatal(err)
	}
	item := reopened.Items[0]
	if item.Status != ItemApplied {
		t.Fatalf("status=%s error=%s", item.Status, item.Error)
	}
	writes := 0
	for _, f := range item.Frames {
		if f.Direction == "out" {
			writes++
		}
	}
	// One original write + one read-back request; never a second write.
	if writes != 2 {
		t.Fatalf("out frames=%d, a lost response must not trigger a rewrite: %#v", writes, item.Frames)
	}
}

func TestContinueConditionalBatchDetectsDeviceSelfChange(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.ApplyThenDrop["T1"] = true
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 1234},
	})
	// Response lost AND the device moved on its own afterwards.
	device.ApplyThenDrop["T1"] = false
	deviceValue := int32(777)
	deviceMuSet(device, "T1", deviceValue)

	reopened, err := channel.ContinueConditionalBatch(context.Background(), result.ID, []string{"temperature"})
	if err != nil {
		t.Fatal(err)
	}
	item := reopened.Items[0]
	if item.Status != ItemConflict {
		t.Fatalf("status=%s error=%s", item.Status, item.Error)
	}
	if v, _ := device.Value("T1"); v != deviceValue {
		t.Fatalf("T1=%d, gateway must not clobber the device change", v)
	}
}

func TestRollbackConditionalBatchRestoresPreconditionValue(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600, Condition: &WriteCondition{ExpectedAlphaRaw: 2500}},
	})
	if result.Items[0].Status != ItemApplied {
		t.Fatalf("status=%s", result.Items[0].Status)
	}

	rolled, err := channel.RollbackConditionalBatch(context.Background(), result.ID, []string{"temperature"})
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Items[0].Status != ItemReverted {
		t.Fatalf("status=%s error=%s", rolled.Items[0].Status, rolled.Items[0].Error)
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, rollback must restore the pre-write value", v)
	}
}

func TestRollbackConditionalBatchAbortsWhenDeviceChanged(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	result := channel.ConditionalBatchWrite(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600, Condition: &WriteCondition{ExpectedAlphaRaw: 2500}},
	})
	// Device changed on its own after our write.
	deviceMuSet(device, "T1", 999)

	rolled, err := channel.RollbackConditionalBatch(context.Background(), result.ID, []string{"temperature"})
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Items[0].Status != ItemConflict {
		t.Fatalf("status=%s error=%s", rolled.Items[0].Status, rolled.Items[0].Error)
	}
	if v, _ := device.Value("T1"); v != 999 {
		t.Fatalf("T1=%d, rollback must not overwrite a foreign value", v)
	}
}

func deviceMuSet(d *SimulatedBetaDevice, tag string, value int32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.registers[tag] = value
}
