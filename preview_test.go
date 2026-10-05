package gateway

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPreviewShowsConvertedTargetAndImpactWithoutWriting(t *testing.T) {
	// T1 holds 250.00 degC; P1 holds 1000.00 psi.
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 100000})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 689.4757}, // kPa -> 9999.99... psi rounds to 10000
	})
	if len(preview.Items) != 2 {
		t.Fatalf("items=%d", len(preview.Items))
	}
	temp := preview.Items[0]
	if temp.Status != "ok" || temp.TargetBetaRaw == nil || *temp.TargetBetaRaw != 26000 {
		t.Fatalf("temperature preview=%+v", temp)
	}
	if temp.CurrentBetaRaw == nil || *temp.CurrentBetaRaw != 25000 || temp.CurrentAlphaRaw == nil || *temp.CurrentAlphaRaw != 2500 {
		t.Fatalf("impact facts=%+v", temp)
	}
	pressure := preview.Items[1]
	if pressure.TargetBetaRaw == nil || *pressure.TargetBetaRaw != 10000 {
		t.Fatalf("pressure target=%+v", pressure)
	}
	// Dry run must not modify the device.
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, preview must be read-only", v)
	}
	if v, _ := device.Value("P1"); v != 100000 {
		t.Fatalf("P1=%d, preview must be read-only", v)
	}
	// Every confirmation starts pending.
	for _, item := range preview.Items {
		if preview.Confirms[item.Field].State != ConfirmPending {
			t.Fatalf("confirmation=%s", preview.Confirms[item.Field].State)
		}
	}
	if _, ok := channel.Traces.GetPreviewBatch(preview.ID); !ok {
		t.Fatal("preview batch is not reopenable from the trace store")
	}
}

func TestExecuteAppliesOnlyConfirmedItems(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 68.94757},
	})
	if _, err := channel.ConfirmPreviewItem(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	// pressure is deliberately left unconfirmed.
	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(executed.Effects) != 1 || executed.Effects[0].Field != "temperature" || executed.Effects[0].Outcome != ItemApplied {
		t.Fatalf("effects=%+v", executed.Effects)
	}
	if v, _ := device.Value("T1"); v != 26000 {
		t.Fatalf("T1=%d, confirmed item not applied", v)
	}
	if v, _ := device.Value("P1"); v != 0 {
		t.Fatalf("P1=%d, unconfirmed item must not execute", v)
	}
}

func TestExecuteFlagsOnlyAffectedItemsWhenDeviceOrRulesChange(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
		{Field: "pressure", Value: 68.94757},
	})
	for _, field := range []string{"temperature", "pressure"} {
		if _, err := channel.ConfirmPreviewItem(preview.ID, field); err != nil {
			t.Fatal(err)
		}
	}
	// Device state changed for temperature only.
	deviceMuSet(device, "T1", 111)

	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if executed.Confirms["temperature"].State != ConfirmNeedsReconfirm {
		t.Fatalf("temperature confirmation=%s", executed.Confirms["temperature"].State)
	}
	if v, _ := device.Value("T1"); v != 111 {
		t.Fatalf("T1=%d, stale confirmation must not execute", v)
	}
	// The unaffected confirmed item still proceeds.
	if v, _ := device.Value("P1"); v != 1000 {
		t.Fatalf("P1=%d, unaffected confirmed item must proceed", v)
	}

	// Conversion rules change: only items confirmed under the old rules are
	// bounced to needs_reconfirm.
	preview2 := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "pressure", Value: 137.89514},
	})
	if _, err := channel.ConfirmPreviewItem(preview2.ID, "pressure"); err != nil {
		t.Fatal(err)
	}
	mapping := channel.Mappings["pressure"]
	mapping.Beta.Scale = 0.001
	channel.Mappings["pressure"] = mapping
	channel.Mappings[mapping.Alpha.Tag] = mapping

	executed2, err := channel.ExecuteConfirmedPreview(context.Background(), preview2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if executed2.Confirms["pressure"].State != ConfirmNeedsReconfirm {
		t.Fatalf("confirmation=%s after rule change", executed2.Confirms["pressure"].State)
	}
	if v, _ := device.Value("P1"); v != 1000 {
		t.Fatalf("P1=%d, confirmation under old conversion rules must not execute", v)
	}
}

func TestMappingRuleChangeAfterConfirmInvalidatesTemperatureOnly(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2310}, // 231.0 degC -> beta raw 23100
		{Field: "pressure", Value: 68.94757},
	})
	temp := preview.Items[0]
	if temp.Status != "ok" || temp.TargetBetaRaw == nil || *temp.TargetBetaRaw != 23100 {
		t.Fatalf("temperature preview=%+v", temp)
	}
	for _, field := range []string{"temperature", "pressure"} {
		if _, err := channel.ConfirmPreviewItem(preview.ID, field); err != nil {
			t.Fatal(err)
		}
	}
	// Change the temperature alpha->device conversion ratio after the
	// confirmation, updating only the field-name key of the mapping table.
	mapping := channel.Mappings["temperature"]
	mapping.Alpha.Scale = 0.01
	channel.Mappings["temperature"] = mapping

	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if executed.Confirms["temperature"].State != ConfirmNeedsReconfirm {
		t.Fatalf("temperature confirmation=%s after rule change", executed.Confirms["temperature"].State)
	}
	for _, effect := range executed.Effects {
		if effect.Field == "temperature" {
			if effect.Outcome != ItemNotAttempted || effect.SentBetaRaw != nil {
				t.Fatalf("temperature executed despite rule change: %+v", effect)
			}
		}
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, confirmation under old conversion rules must not execute", v)
	}
	// The unchanged pressure field still executes on its own.
	if executed.Confirms["pressure"].State != ConfirmConfirmed {
		t.Fatalf("pressure confirmation=%s", executed.Confirms["pressure"].State)
	}
	if v, _ := device.Value("P1"); v != 1000 {
		t.Fatalf("P1=%d, unaffected confirmed item must proceed", v)
	}

	// The invalidated item needs a fresh preview and confirmation.
	preview2 := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2310},
	})
	if _, err := channel.ConfirmPreviewItem(preview2.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	if _, err := channel.ExecuteConfirmedPreview(context.Background(), preview2.ID); err != nil {
		t.Fatal(err)
	}
	// Under the new ratio 2310 alpha raw is 23.10 degC -> beta raw 2310.
	if v, _ := device.Value("T1"); v != 2310 {
		t.Fatalf("T1=%d after re-preview under the new rules", v)
	}
}

func TestExecuteSendsExactlyTheConfirmedValue(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2310},
	})
	if _, err := channel.ConfirmPreviewItem(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	confirmation := preview.Confirms["temperature"]
	if confirmation.TargetBetaRaw == nil || *confirmation.TargetBetaRaw != 23100 {
		t.Fatalf("confirmed target=%+v", confirmation.TargetBetaRaw)
	}
	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(executed.Effects) != 1 || executed.Effects[0].Outcome != ItemApplied {
		t.Fatalf("effects=%+v", executed.Effects)
	}
	sent := executed.Effects[0].SentBetaRaw
	if sent == nil || *sent != *confirmation.TargetBetaRaw {
		t.Fatalf("sent=%v confirmed=%v: the written value must equal the confirmed value", sent, *confirmation.TargetBetaRaw)
	}
	if v, _ := device.Value("T1"); v != 23100 {
		t.Fatalf("T1=%d, device must hold the confirmed value", v)
	}
}

func TestWithdrawnConfirmationBlocksExecution(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2310},
	})
	if _, err := channel.ConfirmPreviewItem(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	if err := channel.WithdrawPreviewConfirmation(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(executed.Effects) != 0 {
		t.Fatalf("effects=%+v, withdrawn confirmation must not execute", executed.Effects)
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, withdrawn confirmation must not write", v)
	}
}

func TestWithdrawnConfirmationCannotBeReenabledByLateRequest(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600},
	})
	if _, err := channel.ConfirmPreviewItem(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	if err := channel.WithdrawPreviewConfirmation(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	// A late confirm request must not re-enable the withdrawn approval.
	if _, err := channel.ConfirmPreviewItem(preview.ID, "temperature"); err == nil {
		t.Fatal("withdrawn confirmation was re-enabled by a late request")
	}
	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(executed.Effects) != 0 {
		t.Fatalf("effects=%+v, withdrawn item must not execute", executed.Effects)
	}
	if v, _ := device.Value("T1"); v != 25000 {
		t.Fatalf("T1=%d, withdrawn confirmation must not execute", v)
	}
}

func TestExecuteConfirmsLostResponseByReadBackWithoutRewrite(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.ApplyThenDrop["T1"] = true
	channel, _ := newTestChannel(t, device, time.Second)

	preview := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 1234},
	})
	if _, err := channel.ConfirmPreviewItem(preview.ID, "temperature"); err != nil {
		t.Fatal(err)
	}
	// The pre-write read inside execute sees 0, matching the confirmed fact.
	executed, err := channel.ExecuteConfirmedPreview(context.Background(), preview.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(executed.Effects) != 1 {
		t.Fatalf("effects=%+v", executed.Effects)
	}
	effect := executed.Effects[0]
	if effect.Outcome != ItemApplied || effect.VerifiedBy != "read_back" {
		t.Fatalf("effect=%+v", effect)
	}
	if v, _ := device.Value("T1"); v != 12340 {
		t.Fatalf("T1=%d", v)
	}
	// Exactly one write ever left the gateway; the device value proves the
	// lost response was not blindly resent.
	outFrames := 0
	for _, f := range executed.Items[0].Frames {
		if f.Direction == "out" {
			outFrames++
		}
	}
	// preview read + execute read + one write + read-back = 4 outbound frames,
	// and only one of them is a write (the device value proves no duplicate).
	if outFrames != 4 {
		t.Fatalf("out frames=%d: %#v", outFrames, executed.Items[0].Frames)
	}
}

func TestParallelPreviewsReportSharedConstraintConflicts(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	// Batch A writes temperature with a precondition on the value it saw.
	previewA := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2600, Condition: &WriteCondition{ExpectedAlphaRaw: 2500}},
	})
	// Batch B writes pressure: a different field of the same device, no
	// shared constraint, so confirming it reports no conflict with A.
	previewB := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "pressure", Value: 68.94757},
	})
	conflictsB, err := channel.ConfirmPreviewItem(previewB.ID, "pressure")
	if err != nil {
		t.Fatal(err)
	}
	for _, conflict := range conflictsB {
		if strings.Contains(conflict, previewA.ID) {
			t.Fatalf("different fields of the same device reported as conflict: %s", conflict)
		}
	}

	// Batch C also writes temperature: it shares batch A's precondition
	// field, so confirming A must report the conflict.
	previewC := channel.PreviewConditionalBatch(context.Background(), []ConditionalWriteRequest{
		{Field: "temperature", Value: 2700},
	})
	conflicts, err := channel.ConfirmPreviewItem(previewA.ID, "temperature")
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) == 0 {
		t.Fatal("shared constraint between parallel previews was not reported")
	}
	found := false
	for _, conflict := range conflicts {
		if strings.Contains(conflict, previewC.ID) {
			found = true
		}
	}
	if !found {
		t.Fatalf("conflicts=%v do not name the parallel preview", conflicts)
	}
}
