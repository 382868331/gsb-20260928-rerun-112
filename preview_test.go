package gateway

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPreviewShowsConvertedTargetAndImpactWithoutWriting(t *testing.T) {
	// T1 holds 250.00 degC; P1 holds 0 psi.
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 100}, // 100 kPa -> 14.5038 psi -> not exactly representable
	})
	if preview.Status != StatusPartial {
		t.Fatalf("status=%s", preview.Status)
	}
	temp, pressure := preview.Items[0], preview.Items[1]
	if temp.Status != PreviewReady {
		t.Fatalf("temp item=%s error=%s", temp.Status, temp.Error)
	}
	if temp.TargetBetaRaw == nil || *temp.TargetBetaRaw != 26000 {
		t.Fatalf("target=%v, want converted beta raw 26000", temp.TargetBetaRaw)
	}
	if temp.CurrentBetaRaw == nil || *temp.CurrentBetaRaw != 25000 {
		t.Fatalf("current=%v, want device reading 25000", temp.CurrentBetaRaw)
	}
	if !strings.Contains(temp.Impact, "25000 -> 26000") {
		t.Fatalf("impact=%q must show the unit-converted change", temp.Impact)
	}
	if pressure.Status != PreviewRejected {
		t.Fatalf("pressure item=%s, 100 kPa is not exactly representable in psi", pressure.Status)
	}
	// Dry run is read-only: the device must be untouched.
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, preview must never modify the device", v)
	}
	for _, item := range preview.Items {
		for _, f := range item.Frames {
			if f.Direction == "out" && item.Status == PreviewReady && len(item.Frames) != 2 {
				t.Fatalf("ready item frames=%#v, want a single read round trip", item.Frames)
			}
		}
	}
	if _, ok := channel.Traces.GetPreview(preview.ID); !ok {
		t.Fatal("preview is not reopenable from the trace store")
	}
}

func TestConfirmAndExecuteAppliesOnlyConfirmedItems(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 68.94757},
	})
	// A stale fingerprint (operator saw something else) is rejected.
	if _, err := channel.ConfirmPreview(preview.ID, []string{"temperature"}, "stale"); err == nil {
		t.Fatal("confirmation with wrong facts fingerprint must be rejected")
	}
	// Confirm only temperature; pressure stays unconfirmed.
	confirmation, err := channel.ConfirmPreview(preview.ID, []string{"temperature"}, preview.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	result, err := channel.ExecuteConfirmedBatch(context.Background(), confirmation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Status != ItemApplied {
		t.Fatalf("items=%+v", result.Items)
	}
	if v, _ := device.Value("T1"); v != 26000 {
		t.Fatalf("T1=%d, confirmed write not applied", v)
	}
	if v, _ := device.Value("P1"); v != 0 {
		t.Fatalf("P1=%d, unconfirmed field must not be executed", v)
	}
	if _, ok := channel.Traces.GetConditionalBatch(result.ID); !ok {
		t.Fatal("execution facts are not reopenable from the trace store")
	}
}

func TestExecuteReconfirmsOnlyAffectedItems(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 68.94757},
	})
	confirmation, err := channel.ConfirmPreview(preview.ID, []string{"temperature", "pressure"}, preview.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	// Device state of T1 changed after the confirmation; P1 did not.
	deviceMuSet(device, "T1", 999)

	result, err := channel.ExecuteConfirmedBatch(context.Background(), confirmation.ID)
	if err != nil {
		t.Fatal(err)
	}
	temp, pressure := result.Items[0], result.Items[1]
	if temp.Status != ItemNeedsReconfirm {
		t.Fatalf("temp=%s error=%s, changed device state must require re-confirmation", temp.Status, temp.Error)
	}
	if pressure.Status != ItemApplied {
		t.Fatalf("pressure=%s error=%s, unaffected item must proceed", pressure.Status, pressure.Error)
	}
	if v, _ := device.Value("T1"); v != 999 {
		t.Fatalf("T1=%d, item needing re-confirmation must not be written", v)
	}
	if v, _ := device.Value("P1"); v != 1000 {
		t.Fatalf("P1=%d, unaffected confirmed item must execute", v)
	}
}

func TestExecuteReconfirmsWhenConversionRulesChange(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 1000})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 68.94757},
	})
	confirmation, err := channel.ConfirmPreview(preview.ID, []string{"temperature", "pressure"}, preview.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	// Conversion rules for temperature changed after confirmation.
	changed := channel.Mappings["temperature"]
	changed.Beta.Scale = 0.1
	channel.Mappings["temperature"] = changed

	result, err := channel.ExecuteConfirmedBatch(context.Background(), confirmation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Items[0].Status != ItemNeedsReconfirm {
		t.Fatalf("temp=%s, changed conversion rules must require re-confirmation", result.Items[0].Status)
	}
	if result.Items[1].Status != ItemApplied {
		t.Fatalf("pressure=%s, unaffected item must proceed", result.Items[1].Status)
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, item needing re-confirmation must not be written", v)
	}
}

func TestWithdrawnConfirmationCannotBeReenabled(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
	})
	confirmation, err := channel.ConfirmPreview(preview.ID, []string{"temperature"}, preview.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.WithdrawConfirmation(confirmation.ID); err != nil {
		t.Fatal(err)
	}
	// The withdrawn confirmation cannot be executed.
	if _, err := channel.ExecuteConfirmedBatch(context.Background(), confirmation.ID); err == nil {
		t.Fatal("withdrawn confirmation must not execute")
	}
	// A late confirmation request for the same preview is rejected.
	if _, err := channel.ConfirmPreview(preview.ID, []string{"temperature"}, preview.Fingerprint); err == nil {
		t.Fatal("late confirmation must not re-enable a withdrawn preview")
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, withdrawn flow must never touch the device", v)
	}
}

func TestParallelPreviewsReportSharedConstraintConflicts(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	first := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
	})
	// Different field of the same device: no conflict.
	second := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "pressure", Value: 68.94757},
	})
	if len(second.Conflicts) != 0 {
		t.Fatalf("different fields of one device must not conflict: %+v", second.Conflicts)
	}
	// Same field (shared device constraint): conflict reported.
	third := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2700},
	})
	if len(third.Conflicts) != 1 || third.Conflicts[0].OtherPreviewID != first.ID || third.Conflicts[0].BetaTag != "T1" {
		t.Fatalf("conflicts=%+v, want shared constraint on T1 with %s", third.Conflicts, first.ID)
	}
}

func TestExecuteLostResponseResolvedByReadBackOnly(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.ApplyThenDrop["T1"] = true // write commits, response is lost
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 1234},
	})
	confirmation, err := channel.ConfirmPreview(preview.ID, []string{"temperature"}, preview.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	result, err := channel.ExecuteConfirmedBatch(context.Background(), confirmation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Items[0].Status != ItemPendingConfirm {
		t.Fatalf("status=%s", result.Items[0].Status)
	}
	if v, _ := device.Value("T1"); v != 12340 {
		t.Fatalf("T1=%d, side effect did happen", v)
	}
	// Reopen the stored execution and resolve: read-back confirms, no rewrite.
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
	// Pre-write read + one write + one read-back; never a second write.
	if writes != 3 {
		t.Fatalf("out frames=%d, a lost response must not trigger a rewrite: %#v", writes, item.Frames)
	}
}
