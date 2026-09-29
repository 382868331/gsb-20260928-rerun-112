package adapters

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync/atomic"

	"industrialgateway/gateway"
)

const (
	tagRead  = 1
	tagWrite = 2
	tagOK    = 0
)

type TAG struct {
	seq  atomic.Uint32
	dial func() (net.Conn, error)
	conn *framedConn
}

func NewTAG(address string) *TAG {
	return &TAG{dial: func() (net.Conn, error) { return net.Dial("tcp", address) }}
}

func NewTAGWithConn(conn net.Conn) *TAG {
	t := &TAG{}
	if conn != nil {
		t.conn = &framedConn{conn: conn}
	}
	return t
}

func (a *TAG) Name() string { return "tag" }

func (a *TAG) Read(ctx context.Context, ref gateway.FieldRef, trace *gateway.Trace) (gateway.Value, error) {
	tagID, err := parseTag(ref.Address)
	if err != nil {
		return gateway.Value{}, err
	}
	req := makeTagFrame(tagRead, a.seq.Add(1), tagID, 0)
	resp := make([]byte, 12)
	trace.Wire(a.Name(), gateway.Write, req)
	if err := a.send(ctx, req, resp); err != nil {
		return gateway.Value{}, wrapUncertain(err)
	}
	trace.Wire(a.Name(), gateway.Read, resp)
	if err := validateTagFrame(req, resp); err != nil {
		return gateway.Value{}, err
	}
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(float32FromLE(resp[8:12])))
	return ref.Field.Decode(b)
}

func (a *TAG) Write(ctx context.Context, ref gateway.FieldRef, v gateway.Value, trace *gateway.Trace) error {
	tagID, err := parseTag(ref.Address)
	if err != nil {
		return err
	}
	raw, err := ref.Field.Encode(v)
	if err != nil {
		return err
	}
	if len(raw) != 4 || ref.Field.DataType != gateway.Float32 {
		return fmt.Errorf("TAG requires a float32 field")
	}
	bits := binary.LittleEndian.Uint32(raw)
	if ref.Field.Endian == gateway.BigEndian {
		bits = binary.BigEndian.Uint32(raw)
	}
	req := makeTagFrame(tagWrite, a.seq.Add(1), tagID, math.Float32frombits(bits))
	resp := make([]byte, 12)
	trace.Wire(a.Name(), gateway.Write, req)
	if err := a.send(ctx, req, resp); err != nil {
		return wrapUncertain(err)
	}
	trace.Wire(a.Name(), gateway.Read, resp)
	if err := validateTagFrame(req, resp); err != nil {
		return wrapUncertain(err)
	}
	return nil
}

func (a *TAG) Close() error {
	if a.conn == nil {
		return nil
	}
	err := a.conn.conn.Close()
	a.conn = nil
	return err
}

func (a *TAG) send(ctx context.Context, req, resp []byte) error {
	if a.conn == nil {
		if a.dial == nil {
			return fmt.Errorf("TAG adapter has no connection")
		}
		conn, err := a.dial()
		if err != nil {
			return err
		}
		a.conn = &framedConn{conn: conn}
	}
	if err := a.conn.roundTrip(ctx, req, resp); err != nil {
		_ = a.Close()
		return err
	}
	return nil
}

func parseTag(address string) (uint16, error) {
	address = strings.TrimPrefix(strings.ToLower(address), "tag:")
	n, err := strconv.ParseUint(address, 0, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid TAG id %q: %w", address, err)
	}
	return uint16(n), nil
}

func makeTagFrame(op uint8, seq uint32, tag uint16, value float32) []byte {
	b := make([]byte, 14)
	b[0], b[1], b[2] = 'T', 'Q', op
	binary.LittleEndian.PutUint32(b[4:8], seq)
	binary.LittleEndian.PutUint16(b[8:10], tag)
	binary.LittleEndian.PutUint32(b[10:14], math.Float32bits(value))
	return b
}

func validateTagFrame(req, resp []byte) error {
	if len(resp) != 12 || resp[0] != 'T' || resp[1] != 'R' {
		return fmt.Errorf("invalid TAG response header")
	}
	if status := binary.LittleEndian.Uint16(resp[2:4]); status != tagOK {
		return fmt.Errorf("TAG device status %d", status)
	}
	if binary.LittleEndian.Uint32(resp[4:8]) != binary.LittleEndian.Uint32(req[4:8]) {
		return fmt.Errorf("TAG response sequence mismatch")
	}
	return nil
}

func float32FromLE(b []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b))
}
