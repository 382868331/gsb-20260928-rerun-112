package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func newTestChannel(t *testing.T, device *SimulatedBetaDevice, timeout time.Duration) (*Channel, *BetaAdapter) {
	t.Helper()
	adapter := NewBetaAdapter(func(context.Context) (net.Conn, error) {
		upstreamClient, upstreamServer := net.Pipe()
		go device.serveConn(context.Background(), upstreamServer)
		return upstreamClient, nil
	}, timeout)
	channel, err := NewChannel("local-alpha-to-beta", adapter, NewTraceStore(), DefaultMappings()...)
	if err != nil {
		t.Fatal(err)
	}
	return channel, adapter
}

func TestReadConvertsScaledTemperatureAndRecordsRoundTrip(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 25000})
	channel, _ := newTestChannel(t, device, time.Second)

	alphaClient, gatewayConn := net.Pipe()
	defer alphaClient.Close()
	go func() { _ = channel.HandleAlphaConn(context.Background(), gatewayConn) }()

	req := AlphaRequest{Address: 1, Function: AlphaRead, Register: 40001}
	if _, err := alphaClient.Write(req.Encode()); err != nil {
		t.Fatal(err)
	}
	resp, err := readAlpha(alphaClient)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != AlphaStatusOK || resp.Value != 2500 {
		t.Fatalf("status=%d value=%d", resp.Status, resp.Value)
	}
	traces := channel.Traces.List()
	if len(traces) != 1 {
		t.Fatalf("trace count=%d", len(traces))
	}
	trace := traces[0]
	if trace.Attempts != 1 || trace.Status != StatusCompleted {
		t.Fatalf("attempts=%d status=%s", trace.Attempts, trace.Status)
	}
	if len(trace.Frames) != 4 {
		t.Fatalf("frame count=%d: %#v", len(trace.Frames), trace.Frames)
	}
	if _, ok := channel.Traces.Get(trace.ID); !ok {
		t.Fatal("trace id is not queryable")
	}
}

func TestWriteRejectsUnrepresentablePressureWithoutSending(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"P1": 0})
	channel, _ := newTestChannel(t, device, time.Second)
	req := AlphaRequest{Address: 1, Function: AlphaWrite, Register: 40002, Value: 1000}

	resp, trace := channel.ProcessAlphaRequest(context.Background(), req)
	if resp.Status != AlphaStatusError || trace.Status != StatusFailed {
		t.Fatalf("resp=%d trace=%s", resp.Status, trace.Status)
	}
	if !errors.Is(ErrLossyInteger, ErrLossyInteger) {
		t.Fatal("unreachable")
	}
	if trace.Error == "" {
		t.Fatal("expected rejection reason")
	}
	if len(trace.Frames) != 0 {
		t.Fatalf("wrote %d frames before mapping rejection", len(trace.Frames))
	}
	if value, ok := device.Value("P1"); !ok || value != 0 {
		t.Fatalf("device changed to %d,%v", value, ok)
	}
}

func TestWriteBoundaryTemperature(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": -10000})
	channel, _ := newTestChannel(t, device, time.Second)
	req := AlphaRequest{Address: 1, Function: AlphaWrite, Register: 40001, Value: 5000}

	resp, trace := channel.ProcessAlphaRequest(context.Background(), req)
	if resp.Status != AlphaStatusOK || trace.Status != StatusCompleted {
		t.Fatalf("status=%d/%s error=%s", resp.Status, trace.Status, trace.Error)
	}
	value, ok := device.Value("T1")
	if !ok || value != 50000 {
		t.Fatalf("T1=%d,%v", value, ok)
	}
}

func TestDisconnectAfterRequestLeavesUnknownTrace(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.CloseOn["T1"] = true
	channel, _ := newTestChannel(t, device, time.Second)
	req := AlphaRequest{Address: 1, Function: AlphaWrite, Register: 40001, Value: 123}

	_, trace := channel.ProcessAlphaRequest(context.Background(), req)
	if trace.Status != StatusUnknown {
		t.Fatalf("status=%s error=%s", trace.Status, trace.Error)
	}
	if trace.Attempts != 1 {
		t.Fatalf("attempts=%d", trace.Attempts)
	}
	if len(trace.Frames) != 1 || trace.Frames[0].Direction != "out" {
		t.Fatalf("frames=%#v", trace.Frames)
	}
}

func TestTimeoutLeavesUnknownAndDoesNotRetry(t *testing.T) {
	device := NewSimulatedBetaDevice(map[string]int32{"T1": 0})
	device.Delay["T1"] = 100 * time.Millisecond
	channel, _ := newTestChannel(t, device, 20*time.Millisecond)
	req := AlphaRequest{Address: 1, Function: AlphaWrite, Register: 40001, Value: 123}

	_, trace := channel.ProcessAlphaRequest(context.Background(), req)
	if trace.Status != StatusUnknown || trace.Attempts != 1 {
		t.Fatalf("status=%s attempts=%d error=%s", trace.Status, trace.Attempts, trace.Error)
	}
	if len(trace.Frames) != 1 {
		t.Fatalf("frames=%#v", trace.Frames)
	}
}

func TestBetaReaderAcceptsSegmentedMessage(t *testing.T) {
	original := BetaRequest{Op: BetaRead, Tag: "T1", Value: 1234}
	wire := EncodeBetaRequest(original)
	client, server := net.Pipe()
	defer client.Close()
	go func() {
		for i := 0; i < len(wire); i++ {
			_, _ = server.Write(wire[i : i+1])
		}
		_ = server.Close()
	}()
	payload, _, err := ReadBetaMessage(client)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBetaRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Tag != "T1" || decoded.Value != 1234 || decoded.Op != BetaRead {
		t.Fatalf("decoded=%#v", decoded)
	}
}

func TestFrameEncodingsUseConfiguredEndianness(t *testing.T) {
	request := EncodeBetaRequest(BetaRequest{Op: BetaWrite, Tag: "X", Value: 1})[2:]
	want := []byte{BetaWrite, 1, 'X', 1, 0, 0, 0, 0xa4}
	if !bytes.Equal(request, want) {
		t.Fatalf("beta=% x want=% x", request, want)
	}
	alpha := (AlphaRequest{Function: AlphaWrite, Register: 40001, Value: 258}).Encode()
	if alpha[4] != 1 || alpha[5] != 2 || alpha[6] != 0x9c || alpha[7] != 0x41 {
		t.Fatalf("alpha big-endian fields=% x", alpha)
	}
}

func readAlpha(r io.Reader) (AlphaResponse, error) {
	buf := make([]byte, AlphaFrameLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return AlphaResponse{}, err
	}
	return AlphaResponse{
		Address:  uint16(buf[1])<<8 | uint16(buf[2]),
		Function: buf[3],
		Status:   buf[4],
		Value:    uint16(buf[5])<<8 | uint16(buf[6]),
	}, nil
}
