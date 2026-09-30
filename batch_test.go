package gateway

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestBatchReadMarksDistinctDeviceInstants(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000, "S1": 22000})
	channel, _ := newTestChannel(t, device, time.Second)

	req := AlphaBatchRequest{Function: AlphaBatchRead, Items: []AlphaBatchItem{
		{Register: 40001},
		{Register: 40003},
	}}
	resp, trace := channel.ProcessBatch(context.Background(), req)
	if resp.Snapshot != AlphaSnapshotMixed {
		t.Fatalf("snapshot flag=%d", resp.Snapshot)
	}
	if trace.Snapshot {
		t.Fatal("separate round trips must not be reported as an atomic snapshot")
	}
	if trace.Status != StatusCompleted || resp.Status != AlphaStatusOK {
		t.Fatalf("status trace=%s resp=%d", trace.Status, resp.Status)
	}
	if len(resp.Results) != 2 || resp.Results[0].Value != 2500 || resp.Results[1].Value != 2200 {
		t.Fatalf("results=%#v", resp.Results)
	}
	if len(trace.Fields) != 2 {
		t.Fatalf("fields=%d", len(trace.Fields))
	}
	if trace.Fields[0].ObservedAt == nil || trace.Fields[1].ObservedAt == nil {
		t.Fatal("each field needs its own observation time")
	}
	if trace.Fields[0].ObservedAt.Equal(*trace.Fields[1].ObservedAt) {
		t.Fatal("per-field timestamps must distinguish the sampling instants")
	}
	for _, field := range trace.Fields {
		if len(field.Frames) != 2 || field.Frames[0].Field == "" {
			t.Fatalf("field evidence missing field-scoped frames: %#v", field.Frames)
		}
	}
}

func TestBatchWriteSecondItemRejectedKeepsFirstStatus(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "S1": 0})
	device.Hooks["S1"] = func(BetaRequest) BetaResponse {
		return BetaResponse{Status: BetaError, Tag: "S1"}
	}
	channel, _ := newTestChannel(t, device, time.Second)

	req := AlphaBatchRequest{Function: AlphaBatchWrite, Items: []AlphaBatchItem{
		{Register: 40001, Value: 200},
		{Register: 40003, Value: 230},
	}}
	resp, trace := channel.ProcessBatch(context.Background(), req)
	if trace.Status != StatusPartial || resp.Status != AlphaStatusError {
		t.Fatalf("trace=%s resp=%d", trace.Status, resp.Status)
	}
	if resp.Results[0].Status != AlphaStatusOK || resp.Results[1].Status != AlphaStatusError {
		t.Fatalf("results=%#v", resp.Results)
	}
	if trace.Fields[0].Status != StatusCompleted || trace.Fields[1].Status != StatusFailed {
		t.Fatalf("fields=%#v", trace.Fields)
	}
	if trace.Attempts != 1 {
		t.Fatalf("batch attempts=%d: must not retry the batch", trace.Attempts)
	}
	for _, field := range trace.Fields {
		if field.Attempts != 1 {
			t.Fatalf("%s attempts=%d", field.Field, field.Attempts)
		}
	}
	if v, _ := device.Value("T1"); v != 2000 {
		t.Fatalf("first write not preserved on device: %d", v)
	}
	if v, _ := device.Value("S1"); v != 0 {
		t.Fatalf("rejected second item changed device: %d", v)
	}
	pressureFrames := trace.Fields[1].Frames
	if len(pressureFrames) != 2 {
		t.Fatalf("pressure frames=%#v", pressureFrames)
	}
	wire := EncodeBetaRequest(BetaRequest{Op: BetaWrite, Tag: "S1", Value: 2300})
	if pressureFrames[0].Hex != fmtHex(wire) {
		t.Fatalf("pressure request=%s want=%s", pressureFrames[0].Hex, fmtHex(wire))
	}
}

func TestBatchWriteLossyUnitConversionRejectedWithoutFrame(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)

	req := AlphaBatchRequest{Function: AlphaBatchWrite, Items: []AlphaBatchItem{
		{Register: 40001, Value: 250},
		{Register: 40002, Value: 1000},
	}}
	resp, trace := channel.ProcessBatch(context.Background(), req)
	if resp.Results[0].Status != AlphaStatusOK {
		t.Fatalf("temperature should convert exactly: %#v", resp.Results[0])
	}
	if resp.Results[1].Status != AlphaStatusError || trace.Fields[1].Status != StatusFailed {
		t.Fatalf("lossy kPa->psi must be rejected: %#v", resp.Results[1])
	}
	if len(trace.Fields[1].Frames) != 0 {
		t.Fatalf("lossy conversion sent frames: %#v", trace.Fields[1].Frames)
	}
	if v, _ := device.Value("T1"); v != 2500 {
		t.Fatalf("T1=%d", v)
	}
	if v, _ := device.Value("P1"); v != 0 {
		t.Fatalf("P1=%d must stay untouched", v)
	}
}

func TestBatchWriteLostAckVerifiedByReadbackAfterRecovery(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0, "P1": 0})
	device.Hooks["P1"] = func(BetaRequest) BetaResponse {
		return BetaResponse{Status: BetaError, Tag: "P1"}
	}
	device.Delay["T1"] = 100 * time.Millisecond
	channel, adapter := newTestChannel(t, device, 20*time.Millisecond)

	req := AlphaBatchRequest{Function: AlphaBatchWrite, Items: []AlphaBatchItem{
		{Register: 40001, Value: 250},
		{Register: 40002, Value: 100},
	}}
	_, trace := channel.ProcessBatch(context.Background(), req)
	if trace.Status != StatusUnknown {
		t.Fatalf("trace=%s error=%s", trace.Status, trace.Error)
	}
	if trace.Fields[0].Status != StatusUnknown || trace.Fields[1].Status != StatusFailed {
		t.Fatalf("fields=%#v", trace.Fields)
	}
	if len(trace.Fields[0].Frames) != 1 || trace.Fields[0].Frames[0].Direction != "out" {
		t.Fatalf("lost-ack field frames=%#v", trace.Fields[0].Frames)
	}
	time.Sleep(120 * time.Millisecond)
	if v, _ := device.Value("T1"); v != 2500 {
		t.Fatalf("write actually applied during timeout, device=%d", v)
	}

	adapter.Timeout = time.Second
	verifications := channel.VerifyUncertainWrites(context.Background(), trace)
	if len(verifications) != 1 {
		t.Fatalf("only the unknown field needs verification, got %#v", verifications)
	}
	result := verifications[0]
	if result.Field != "temperature" || result.Status != VerifyApplied {
		t.Fatalf("verification=%#v", result)
	}
	if result.Observed == nil || *result.Observed != 2500 {
		t.Fatalf("read-back evidence=%#v", result)
	}
}

func TestVerifyStaysUncertainWhenReadbackTimesOut(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.Delay["T1"] = 100 * time.Millisecond
	channel, _ := newTestChannel(t, device, 20*time.Millisecond)

	req := AlphaBatchRequest{Function: AlphaBatchWrite, Items: []AlphaBatchItem{{Register: 40001, Value: 250}}}
	_, trace := channel.ProcessBatch(context.Background(), req)
	if trace.Fields[0].Status != StatusUnknown {
		t.Fatalf("expected unknown, got %s", trace.Fields[0].Status)
	}
	// Verification read also times out: claiming not_applied would hide the
	// write that the device may already have committed.
	verifications := channel.VerifyUncertainWrites(context.Background(), trace)
	if len(verifications) != 1 || verifications[0].Status != VerifyUncertain {
		t.Fatalf("verification=%#v", verifications)
	}
	if verifications[0].Observed != nil {
		t.Fatal("no read-back value may be presented on verification failure")
	}
}

func TestBatchWireRoundTripOverAlphaConn(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 20000, "S1": 21000})
	channel, _ := newTestChannel(t, device, time.Second)

	alphaClient, gatewayConn := net.Pipe()
	defer alphaClient.Close()
	go func() { _ = channel.HandleAlphaConn(context.Background(), gatewayConn) }()

	req := AlphaBatchRequest{Function: AlphaBatchRead, Items: []AlphaBatchItem{{Register: 40001}, {Register: 40003}}}
	if _, err := alphaClient.Write(req.Encode()); err != nil {
		t.Fatal(err)
	}

	head := make([]byte, 7)
	if _, err := readFull(alphaClient, head); err != nil {
		t.Fatal(err)
	}
	if head[0] != 0xA5 || head[3] != AlphaBatchRead || head[4] != AlphaStatusOK || head[5] != AlphaSnapshotMixed || head[6] != 2 {
		t.Fatalf("batch response header=% x", head)
	}
	entries := make([]byte, 6)
	if _, err := readFull(alphaClient, entries); err != nil {
		t.Fatal(err)
	}
	if entries[0] != AlphaStatusOK || entries[1] != 0x07 || entries[2] != 0xD0 {
		t.Fatalf("first result=% x", entries[:3])
	}
	if entries[3] != AlphaStatusOK || entries[4] != 0x08 || entries[5] != 0x34 {
		t.Fatalf("second result=% x", entries[3:])
	}
}

func readFull(c net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := c.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
