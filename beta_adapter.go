package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

type FrameRecorder interface {
	RecordFrame(direction, protocol string, data []byte)
}

type frameRecorderKey struct{}

func WithFrameRecorder(ctx context.Context, recorder FrameRecorder) context.Context {
	return context.WithValue(ctx, frameRecorderKey{}, recorder)
}

func recordFrame(ctx context.Context, direction, protocol string, data []byte) {
	if recorder, ok := ctx.Value(frameRecorderKey{}).(FrameRecorder); ok && recorder != nil {
		recorder.RecordFrame(direction, protocol, append([]byte(nil), data...))
	}
}

type BetaAdapter struct {
	Dial    func(context.Context) (net.Conn, error)
	Timeout time.Duration
}

func NewBetaAdapter(dial func(context.Context) (net.Conn, error), timeout time.Duration) *BetaAdapter {
	return &BetaAdapter{Dial: dial, Timeout: timeout}
}

func (a *BetaAdapter) Name() string { return BetaProtocol }

func (a *BetaAdapter) Read(ctx context.Context, tag string) (float64, error) {
	resp, err := a.roundTrip(ctx, BetaRequest{Op: BetaRead, Tag: tag})
	if err != nil {
		return 0, err
	}
	if resp.Status != BetaOK {
		return 0, fmt.Errorf("beta device rejected read: status %d", resp.Status)
	}
	return float64(resp.Value), nil
}

func (a *BetaAdapter) Write(ctx context.Context, tag string, value float64) error {
	if value != float64(int32(value)) {
		return fmt.Errorf("%w: beta int32 cannot represent %g", ErrLossyInteger, value)
	}
	resp, err := a.roundTrip(ctx, BetaRequest{Op: BetaWrite, Tag: tag, Value: int32(value)})
	if err != nil {
		return err
	}
	switch resp.Status {
	case BetaOK:
		return nil
	case BetaUnknown:
		return fmt.Errorf("%w: device status unknown", ErrResultUnknown)
	default:
		return fmt.Errorf("beta device rejected write: status %d", resp.Status)
	}
}

func (a *BetaAdapter) Close() error { return nil }

func (a *BetaAdapter) roundTrip(ctx context.Context, req BetaRequest) (BetaResponse, error) {
	conn, err := a.Dial(ctx)
	if err != nil {
		return BetaResponse{}, err
	}
	defer conn.Close()
	if a.Timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(a.Timeout))
	}
	wire := EncodeBetaRequest(req)
	n, err := conn.Write(wire)
	recordFrame(ctx, "out", BetaProtocol, wire)
	if err != nil {
		if n > 0 {
			return BetaResponse{}, fmt.Errorf("%w: sent %d/%d bytes: %v", ErrResultUnknown, n, len(wire), err)
		}
		return BetaResponse{}, err
	}
	payload, raw, err := ReadBetaMessage(conn)
	if err == nil {
		recordFrame(ctx, "in", BetaProtocol, raw)
	}
	if err != nil {
		if isUncertainAfterRequest(err) {
			return BetaResponse{}, fmt.Errorf("%w: response unavailable: %v", ErrResultUnknown, err)
		}
		return BetaResponse{}, err
	}
	resp, err := DecodeBetaResponse(payload)
	if err != nil {
		return BetaResponse{}, fmt.Errorf("%w: %v", ErrResultUnknown, err)
	}
	return resp, nil
}

func isUncertainAfterRequest(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || isTimeout(err)
}

func isTimeout(err error) bool {
	type timeoutError interface{ Timeout() bool }
	var target timeoutError
	return errors.As(err, &target) && target.Timeout()
}
