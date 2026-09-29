package adapters

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"

	"industrialgateway/gateway"
)

const (
	regHeader = 0xA5
	regRead   = 0x03
	regWrite  = 0x06
)

type REG struct {
	address string
	dial    func() (net.Conn, error)
	conn    *framedConn
}

func NewREG(address string) *REG {
	return &REG{address: address, dial: func() (net.Conn, error) { return net.Dial("tcp", address) }}
}

func NewREGWithConn(conn net.Conn) *REG {
	return &REG{dial: func() (net.Conn, error) { return conn, nil }}
}

func (a *REG) Name() string { return "reg" }

func (a *REG) connect() (*framedConn, error) {
	if a.conn != nil {
		return a.conn, nil
	}
	c, err := a.dial()
	if err != nil {
		return nil, err
	}
	a.conn = &framedConn{conn: c}
	return a.conn, nil
}

func (a *REG) Read(ctx context.Context, ref gateway.FieldRef, trace *gateway.Trace) (gateway.Value, error) {
	register, err := parseRegister(ref.Address)
	if err != nil {
		return gateway.Value{}, err
	}
	req := makeREGFrame(regRead, register, 1)
	resp := make([]byte, 8)
	trace.Wire(a.Name(), gateway.Write, req)
	if err := a.send(ctx, req, resp); err != nil {
		return gateway.Value{}, wrapUncertain(err)
	}
	trace.Wire(a.Name(), gateway.Read, resp)
	if err := validateREGFrame(req, resp); err != nil {
		return gateway.Value{}, err
	}
	return ref.Field.Decode(resp[4:6])
}

func (a *REG) Write(ctx context.Context, ref gateway.FieldRef, v gateway.Value, trace *gateway.Trace) error {
	register, err := parseRegister(ref.Address)
	if err != nil {
		return err
	}
	raw, err := ref.Field.Encode(v)
	if err != nil {
		return err
	}
	if len(raw) != 2 {
		return fmt.Errorf("REG register requires a two-byte field")
	}
	value := binary.BigEndian.Uint16(raw)
	if ref.Field.Endian == gateway.LittleEndian {
		value = binary.LittleEndian.Uint16(raw)
	}
	req := makeREGFrame(regWrite, register, value)
	resp := make([]byte, 8)
	trace.Wire(a.Name(), gateway.Write, req)
	if err := a.send(ctx, req, resp); err != nil {
		return wrapUncertain(err)
	}
	trace.Wire(a.Name(), gateway.Read, resp)
	if err := validateREGFrame(req, resp); err != nil {
		if isUncertain(err) {
			return gateway.Uncertain(err)
		}
		return err
	}
	return nil
}

func (a *REG) Close() error {
	if a.conn == nil {
		return nil
	}
	err := a.conn.conn.Close()
	a.conn = nil
	return err
}

func (a *REG) send(ctx context.Context, req, resp []byte) error {
	c, err := a.connect()
	if err != nil {
		return err
	}
	if err := c.roundTrip(ctx, req, resp); err != nil {
		_ = a.Close()
		return err
	}
	return nil
}

func parseRegister(address string) (uint16, error) {
	address = strings.TrimPrefix(strings.ToLower(address), "reg:")
	n, err := strconv.ParseUint(address, 0, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid REG register %q: %w", address, err)
	}
	return uint16(n), nil
}

func makeREGFrame(function, register, value uint16) []byte {
	b := []byte{regHeader, byte(function), byte(register >> 8), byte(register), byte(value >> 8), byte(value), 0, 0}
	crc := crc16Modbus(b[:6])
	binary.LittleEndian.PutUint16(b[6:], crc)
	return b
}

func validateREGFrame(req, resp []byte) error {
	if len(resp) != 8 || resp[0] != regHeader {
		return fmt.Errorf("invalid REG response header")
	}
	if binary.LittleEndian.Uint16(resp[6:]) != crc16Modbus(resp[:6]) {
		return fmt.Errorf("REG response CRC mismatch")
	}
	if resp[1]&0x80 != 0 {
		return fmt.Errorf("REG device exception 0x%02x for function 0x%02x", resp[2], resp[1]&0x7f)
	}
	if resp[1] != req[1] || resp[2] != req[2] || resp[3] != req[3] {
		return fmt.Errorf("REG response does not match request")
	}
	if req[1] == regWrite && (resp[4] != req[4] || resp[5] != req[5]) {
		return fmt.Errorf("REG write echo mismatch")
	}
	return nil
}

func wrapUncertain(err error) error {
	if isUncertain(err) {
		return gateway.Uncertain(err)
	}
	return err
}

// CRC-16/MODBUS: polynomial 0xA001, initial value 0xFFFF.
func crc16Modbus(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = crc>>1 ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
