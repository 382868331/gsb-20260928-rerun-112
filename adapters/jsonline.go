package adapters

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"industrialgateway/gateway"
)

// JSONL is a third explicitly virtual adapter: one JSON object per line.
// It exists to prove later adapters use the same mapping and trace contracts.
type JSONL struct {
	seq  atomic.Uint32
	dial func() (net.Conn, error)
	conn net.Conn
}

type jsonlRequest struct {
	Magic string   `json:"magic"`
	Op    string   `json:"op"`
	Seq   uint32   `json:"seq"`
	ID    uint16   `json:"id"`
	Value *float64 `json:"value,omitempty"`
}

type jsonlResponse struct {
	Magic  string   `json:"magic"`
	Seq    uint32   `json:"seq"`
	Status int      `json:"status"`
	Value  *float64 `json:"value,omitempty"`
}

func NewJSONL(address string) *JSONL {
	return &JSONL{dial: func() (net.Conn, error) { return net.Dial("tcp", address) }}
}

func NewJSONLWithConn(conn net.Conn) *JSONL { return &JSONL{conn: conn} }

func (a *JSONL) Name() string { return "jsonl" }

func (a *JSONL) Read(ctx context.Context, ref gateway.FieldRef, trace *gateway.Trace) (gateway.Value, error) {
	id, err := parseJSONLID(ref.Address)
	if err != nil {
		return gateway.Value{}, err
	}
	line, resp, err := a.exchange(ctx, jsonlRequest{Magic: "JQ", Op: "read", Seq: a.seq.Add(1), ID: id})
	if err != nil {
		return gateway.Value{}, err
	}
	trace.Wire(a.Name(), gateway.Write, line)
	trace.Wire(a.Name(), gateway.Read, resp)
	var parsed jsonlResponse
	if err := json.Unmarshal(resp, &parsed); err != nil {
		return gateway.Value{}, fmt.Errorf("invalid JSONL response: %w", err)
	}
	if parsed.Status != 0 || parsed.Value == nil {
		return gateway.Value{}, fmt.Errorf("JSONL device status %d", parsed.Status)
	}
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(float32(*parsed.Value)))
	return ref.Field.Decode(b)
}

func (a *JSONL) Write(ctx context.Context, ref gateway.FieldRef, v gateway.Value, trace *gateway.Trace) error {
	id, err := parseJSONLID(ref.Address)
	if err != nil {
		return err
	}
	raw, err := ref.Field.Encode(v)
	if err != nil {
		return err
	}
	if ref.Field.DataType != gateway.Float32 || len(raw) != 4 {
		return fmt.Errorf("JSONL requires a float32 field")
	}
	bits := binary.LittleEndian.Uint32(raw)
	if ref.Field.Endian == gateway.BigEndian {
		bits = binary.BigEndian.Uint32(raw)
	}
	n := float64(math.Float32frombits(bits))
	line, resp, err := a.exchange(ctx, jsonlRequest{Magic: "JQ", Op: "write", Seq: a.seq.Add(1), ID: id, Value: &n})
	trace.Wire(a.Name(), gateway.Write, line)
	if err != nil {
		return err
	}
	trace.Wire(a.Name(), gateway.Read, resp)
	var parsed jsonlResponse
	if err := json.Unmarshal(resp, &parsed); err != nil {
		return fmt.Errorf("invalid JSONL response: %w", err)
	}
	if parsed.Status != 0 {
		return fmt.Errorf("JSONL device status %d", parsed.Status)
	}
	return nil
}

func (a *JSONL) Close() error {
	if a.conn == nil {
		return nil
	}
	err := a.conn.Close()
	a.conn = nil
	return err
}

func (a *JSONL) exchange(ctx context.Context, request jsonlRequest) ([]byte, []byte, error) {
	if a.conn == nil {
		if a.dial == nil {
			return nil, nil, fmt.Errorf("JSONL adapter has no connection")
		}
		conn, err := a.dial()
		if err != nil {
			return nil, nil, err
		}
		a.conn = conn
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = a.conn.SetDeadline(deadline)
		defer a.conn.SetDeadline(time.Time{})
	}
	line, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	line = append(line, '\n')
	n, err := a.conn.Write(line)
	if err != nil {
		if n > 0 {
			err = gateway.Uncertain(err)
		}
		_ = a.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(a.conn)
	resp, err := reader.ReadBytes('\n')
	if err != nil {
		_ = a.Close()
		return nil, nil, gateway.Uncertain(err)
	}
	return line, resp[:len(resp)-1], nil
}

func parseJSONLID(address string) (uint16, error) {
	address = strings.TrimPrefix(strings.ToLower(address), "jsonl:")
	n, err := strconv.ParseUint(address, 0, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid JSONL id %q: %w", address, err)
	}
	return uint16(n), nil
}
