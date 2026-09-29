package gateway

import (
	"context"
	"net"
	"sync"
	"time"
)

type SimulatedBetaDevice struct {
	mu        sync.Mutex
	registers map[string]int32
	Hooks     map[string]func(BetaRequest) BetaResponse
	CloseOn   map[string]bool
	Delay     map[string]time.Duration
}

func NewSimulatedBetaDevice(values map[string]int32) *SimulatedBetaDevice {
	clone := make(map[string]int32, len(values))
	for k, v := range values {
		clone[k] = v
	}
	return &SimulatedBetaDevice{registers: clone, Hooks: map[string]func(BetaRequest) BetaResponse{}, CloseOn: map[string]bool{}, Delay: map[string]time.Duration{}}
}

func (d *SimulatedBetaDevice) Value(tag string) (int32, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.registers[tag]
	return v, ok
}

func (d *SimulatedBetaDevice) Serve(ctx context.Context, listener net.Listener) error {
	go func() {
		signal := ctx.Done()
		<-signal
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return err
		}
		go d.serveConn(ctx, conn)
	}
}

func (d *SimulatedBetaDevice) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	for {
		payload, _, err := ReadBetaMessage(conn)
		if err != nil {
			return
		}
		req, err := DecodeBetaRequest(payload)
		if err != nil {
			_, _ = conn.Write(EncodeBetaResponse(BetaResponse{Status: BetaError}))
			return
		}
		if delay := d.Delay[req.Tag]; delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		if d.CloseOn[req.Tag] {
			_ = conn.Close()
			return
		}
		_, _ = conn.Write(EncodeBetaResponse(d.handle(req)))
	}
}

func (d *SimulatedBetaDevice) handle(req BetaRequest) BetaResponse {
	if hook := d.Hooks[req.Tag]; hook != nil {
		return hook(req)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch req.Op {
	case BetaRead:
		v, ok := d.registers[req.Tag]
		if !ok {
			return BetaResponse{Status: BetaError, Tag: req.Tag}
		}
		return BetaResponse{Status: BetaOK, Tag: req.Tag, Value: v}
	case BetaWrite:
		d.registers[req.Tag] = req.Value
		return BetaResponse{Status: BetaOK, Tag: req.Tag, Value: req.Value}
	default:
		return BetaResponse{Status: BetaError, Tag: req.Tag}
	}
}
