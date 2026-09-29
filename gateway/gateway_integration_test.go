package gateway_test

import (
	"context"
	"math"
	"testing"
	"time"

	"industrialgateway/adapters"
	"industrialgateway/gateway"
	"industrialgateway/simdev"
)

func floatPtr(v float64) *float64 { return &v }

func testGateway(t *testing.T) (*gateway.Gateway, *simdev.Device) {
	t.Helper()
	dev := simdev.New()
	t.Cleanup(func() { _ = dev.Close() })

	mapping, err := gateway.NewMapping([]gateway.Point{
		{
			Name: "reg-to-tag",
			From: gateway.FieldRef{
				Adapter: "reg",
				Address: "reg:10",
				Field:   gateway.Field{DataType: gateway.Uint16, Endian: gateway.BigEndian, Unit: "C", Scale: 1, Min: floatPtr(0), Max: floatPtr(200)},
			},
			To: gateway.FieldRef{
				Adapter: "tag",
				Address: "tag:7",
				Field:   gateway.Field{DataType: gateway.Float32, Endian: gateway.LittleEndian, Unit: "F", Scale: 1, Min: floatPtr(32), Max: floatPtr(392)},
			},
		},
		{
			Name: "tag-to-reg",
			From: gateway.FieldRef{
				Adapter: "tag",
				Address: "tag:7",
				Field:   gateway.Field{DataType: gateway.Float32, Endian: gateway.LittleEndian, Unit: "F", Scale: 1},
			},
			To: gateway.FieldRef{
				Adapter: "reg",
				Address: "reg:10",
				Field:   gateway.Field{DataType: gateway.Uint16, Endian: gateway.BigEndian, Unit: "C", Scale: 1, Min: floatPtr(0), Max: floatPtr(200)},
			},
		},
		{
			Name: "jsonl-to-tag",
			From: gateway.FieldRef{
				Adapter: "jsonl",
				Address: "jsonl:3",
				Field:   gateway.Field{DataType: gateway.Float32, Endian: gateway.LittleEndian, Unit: "C", Scale: 1},
			},
			To: gateway.FieldRef{
				Adapter: "tag",
				Address: "tag:9",
				Field:   gateway.Field{DataType: gateway.Float32, Endian: gateway.LittleEndian, Unit: "F", Scale: 1},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tracker := gateway.NewTracker()
	regClient := dev.Pipe()
	tagClient := dev.Pipe()
	jsonClient := dev.Pipe()
	t.Cleanup(func() {
		_ = regClient.Close()
		_ = tagClient.Close()
		_ = jsonClient.Close()
	})
	g := gateway.New(mapping, tracker,
		adapters.NewREGWithConn(regClient),
		adapters.NewTAGWithConn(tagClient),
		adapters.NewJSONLWithConn(jsonClient),
	)
	t.Cleanup(func() { _ = g.Close() })
	return g, dev
}

func TestReadConvertsUnitsByteOrderAndBoundary(t *testing.T) {
	g, dev := testGateway(t)
	dev.SetRegister(10, math.MaxUint16)

	// uint16 maximum is outside the configured 0..200 C field range and must be
	// rejected rather than wrapped or clamped.
	if _, err := g.Read(context.Background(), "reg-to-tag"); err == nil {
		t.Fatal("expected out-of-range register maximum to be rejected")
	}

	for _, tc := range []struct {
		register uint16
		wantF    float64
	}{
		{0, 32},
		{100, 212},
		{200, 392},
	} {
		dev.SetRegister(10, tc.register)
		tr, err := g.Read(context.Background(), "reg-to-tag")
		if err != nil {
			t.Fatalf("register %d: %v", tc.register, err)
		}
		if tr.Status != gateway.StatusSucceeded || tr.Result == nil || tr.Result.Number != tc.wantF || tr.Result.Unit != "F" {
			t.Fatalf("register %d got %+v, want %g F", tc.register, tr, tc.wantF)
		}
		saved, ok := g.Operation(tr.ID)
		if !ok || len(saved.Messages) != 2 || saved.Messages[0].Hex == "" || saved.Messages[1].Hex == "" {
			t.Fatalf("operation %s missing complete REG request/response trace: %+v", tr.ID, saved)
		}
	}
}

func TestReadReverseDirectionWithSegmentedResponse(t *testing.T) {
	g, dev := testGateway(t)
	dev.SetTag(7, 212)
	dev.Split = true
	dev.Delay = time.Millisecond

	tr, err := g.Read(context.Background(), "tag-to-reg")
	if err != nil {
		t.Fatal(err)
	}
	if tr.Result == nil || tr.Result.Number != 100 || tr.Result.Unit != "C" {
		t.Fatalf("got %+v, want 100 C", tr.Result)
	}
	if len(tr.Messages) != 2 || len(tr.Messages[1].Hex) != 24 {
		t.Fatalf("segmented 12-byte TAG response was not reassembled: %+v", tr.Messages)
	}
}

func TestAdditionalJSONLAdapterSharesTrace(t *testing.T) {
	g, dev := testGateway(t)
	dev.SetJSONL(3, 100)
	tr, err := g.Read(context.Background(), "jsonl-to-tag")
	if err != nil {
		t.Fatal(err)
	}
	if tr.Result == nil || tr.Result.Unit != "F" || math.Abs(tr.Result.Number-212) > 1e-5 {
		t.Fatalf("got %+v, want 212 F", tr.Result)
	}
	saved, ok := g.Operation(tr.ID)
	if !ok || saved.Status != gateway.StatusSucceeded || len(saved.Messages) != 2 {
		t.Fatalf("JSONL round trip missing from trace: %+v ok=%v", saved, ok)
	}
}

func TestWriteAcknowledgedAndTraceRetained(t *testing.T) {
	g, dev := testGateway(t)
	tr, err := g.Write(context.Background(), "reg-to-tag", gateway.Value{Number: 212, Unit: "F"})
	if err != nil {
		t.Fatal(err)
	}
	if got := dev.Tag(7); got != 212 {
		t.Fatalf("device tag = %v, want 212", got)
	}
	saved, ok := g.Operation(tr.ID)
	if !ok || saved.Status != gateway.StatusSucceeded {
		t.Fatalf("missing successful operation trace: %+v ok=%v", saved, ok)
	}
}

func TestWriteRejectsUnrepresentableValues(t *testing.T) {
	g, _ := testGateway(t)
	for _, v := range []gateway.Value{
		{Number: math.NaN(), Unit: "F"},
		{Number: math.Inf(1), Unit: "F"},
		{Number: 393.8, Unit: "F"},
		{Number: 100, Unit: "kPa"},
	} {
		if tr, err := g.Write(context.Background(), "reg-to-tag", v); err == nil {
			t.Fatalf("value %+v was accepted", v)
		} else if tr != nil && tr.Status != gateway.StatusFailed {
			t.Fatalf("value %+v: status = %s", v, tr.Status)
		}
	}
}

func TestWriteTimeoutOrDisconnectIsUnknownAndNotRetried(t *testing.T) {
	g, dev := testGateway(t)
	dev.SetRegister(10, 32)
	dev.DropAfterRequest = true

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	tr, err := g.Write(ctx, "tag-to-reg", gateway.Value{Number: 100, Unit: "C"})
	if err == nil {
		t.Fatal("expected disconnect error")
	}
	if tr.Status != gateway.StatusUnknown {
		t.Fatalf("status = %s, want unknown", tr.Status)
	}
	if len(tr.Messages) != 1 {
		t.Fatalf("observed %d wire requests, want exactly one; automatic retry would create two", len(tr.Messages))
	}
	saved, ok := g.Operation(tr.ID)
	if !ok || saved.Status != gateway.StatusUnknown || saved.Error == "" {
		t.Fatalf("uncertain result was not retained: %+v ok=%v", saved, ok)
	}
}
