package simdev

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

// Device is a purely local, in-memory simulation. It listens on loopback only
// and never encodes or sends traffic to a real controller address.
type Device struct {
	listener net.Listener
	wg       sync.WaitGroup
	closed   chan struct{}
	mu       sync.Mutex
	reg      map[uint16]uint16
	tag      map[uint16]float32
	jsonID   map[uint16]float32

	// Split writes each response one byte per write with Delay between bytes.
	Split bool
	Delay time.Duration

	// DropAfterRequest closes the connection after reading one request.
	DropAfterRequest bool
}

func New() *Device {
	return &Device{
		closed: make(chan struct{}),
		reg:    map[uint16]uint16{},
		tag:    map[uint16]float32{},
		jsonID: map[uint16]float32{},
	}
}

func (d *Device) Listen(addr string) (string, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	d.listener = l
	d.wg.Add(1)
	go d.serve()
	return l.Addr().String(), nil
}

// Pipe attaches the simulator to one end of an in-process connection and
// returns the client end. It is useful in restricted sandboxes and tests.
func (d *Device) Pipe() net.Conn {
	client, server := net.Pipe()
	d.wg.Add(1)
	go d.handle(server)
	return client
}

func (d *Device) SetRegister(addr uint16, v uint16) {
	d.mu.Lock()
	d.reg[addr] = v
	d.mu.Unlock()
}

func (d *Device) Register(addr uint16) uint16 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reg[addr]
}

func (d *Device) SetTag(id uint16, v float32) {
	d.mu.Lock()
	d.tag[id] = v
	d.mu.Unlock()
}

func (d *Device) Tag(id uint16) float32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.tag[id]
}

func (d *Device) SetJSONL(id uint16, v float32) {
	d.mu.Lock()
	d.jsonID[id] = v
	d.mu.Unlock()
}

func (d *Device) JSONL(id uint16) float32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.jsonID[id]
}

func (d *Device) Close() error {
	close(d.closed)
	var err error
	if d.listener != nil {
		err = d.listener.Close()
	}
	d.wg.Wait()
	return err
}

func (d *Device) serve() {
	defer d.wg.Done()
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			return
		}
		d.wg.Add(1)
		go d.handle(conn)
	}
}

func (d *Device) handle(conn net.Conn) {
	defer func() { _ = conn.Close(); d.wg.Done() }()
	for {
		select {
		case <-d.closed:
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		first := []byte{0}
		if _, err := io.ReadFull(conn, first); err != nil {
			return
		}
		if first[0] == 0xA5 {
			b := make([]byte, 7)
			if _, err := io.ReadFull(conn, b); err != nil {
				return
			}
			req := append(first, b...)
			if d.dropAfterRequest() {
				return
			}
			resp := d.handleREG(req)
			if err := d.write(conn, resp); err != nil {
				return
			}
			continue
		}
		if first[0] == 'T' {
			magic := []byte{0}
			if _, err := io.ReadFull(conn, magic); err != nil {
				return
			}
			if magic[0] != 'Q' {
				return
			}
			b := make([]byte, 12)
			if _, err := io.ReadFull(conn, b); err != nil {
				return
			}
			req := append(append(first, magic...), b...)
			if d.dropAfterRequest() {
				return
			}
			resp := d.handleTAG(req)
			if err := d.write(conn, resp); err != nil {
				return
			}
			continue
		}
		if first[0] == '{' {
			rest, err := readLineAfterFirst(conn, first[0])
			if err != nil {
				return
			}
			if d.dropAfterRequest() {
				return
			}
			resp := d.handleJSONL(rest)
			if err := d.write(conn, append(resp, '\n')); err != nil {
				return
			}
			continue
		}
		return
	}
}

func readLineAfterFirst(conn net.Conn, first byte) ([]byte, error) {
	buf := []byte{first}
	for {
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			return nil, err
		}
		if b[0] == '\n' {
			return buf, nil
		}
		buf = append(buf, b[0])
	}
}

func (d *Device) dropAfterRequest() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.DropAfterRequest
}

func (d *Device) write(conn net.Conn, b []byte) error {
	d.mu.Lock()
	split, delay := d.Split, d.Delay
	d.mu.Unlock()
	if !split {
		_, err := conn.Write(b)
		return err
	}
	for _, one := range b {
		if _, err := conn.Write([]byte{one}); err != nil {
			return err
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	return nil
}

func (d *Device) handleREG(req []byte) []byte {
	resp := append([]byte(nil), req...)
	register := binary.BigEndian.Uint16(req[2:4])
	value := binary.BigEndian.Uint16(req[4:6])
	d.mu.Lock()
	switch req[1] {
	case 0x03:
		resp[4], resp[5] = byte(d.reg[register]>>8), byte(d.reg[register])
	case 0x06:
		d.reg[register] = value
	default:
		resp[1] |= 0x80
		resp[2] = 0x01
	}
	d.mu.Unlock()
	binary.LittleEndian.PutUint16(resp[6:], crc16(resp[:6]))
	return resp
}

func (d *Device) handleTAG(req []byte) []byte {
	resp := make([]byte, 12)
	resp[0], resp[1] = 'T', 'R'
	copy(resp[4:8], req[4:8])
	tag := binary.LittleEndian.Uint16(req[8:10])
	d.mu.Lock()
	switch req[2] {
	case 1:
		binary.LittleEndian.PutUint32(resp[8:12], math.Float32bits(d.tag[tag]))
	case 2:
		d.tag[tag] = math.Float32frombits(binary.LittleEndian.Uint32(req[10:14]))
		copy(resp[8:12], req[10:14])
	default:
		binary.LittleEndian.PutUint16(resp[2:4], 1)
	}
	d.mu.Unlock()
	return resp
}

type simJSONLRequest struct {
	Magic string   `json:"magic"`
	Op    string   `json:"op"`
	Seq   uint32   `json:"seq"`
	ID    uint16   `json:"id"`
	Value *float64 `json:"value,omitempty"`
}

type simJSONLResponse struct {
	Magic  string   `json:"magic"`
	Seq    uint32   `json:"seq"`
	Status int      `json:"status"`
	Value  *float64 `json:"value,omitempty"`
}

func (d *Device) handleJSONL(line []byte) []byte {
	var req simJSONLRequest
	resp := simJSONLResponse{Magic: "JR"}
	if err := json.Unmarshal(line, &req); err != nil || req.Magic != "JQ" {
		resp.Status = 1
		b, _ := json.Marshal(resp)
		return b
	}
	resp.Seq = req.Seq
	d.mu.Lock()
	switch req.Op {
	case "read":
		v := float64(d.jsonID[req.ID])
		resp.Value = &v
	case "write":
		if req.Value != nil {
			d.jsonID[req.ID] = float32(*req.Value)
			resp.Value = req.Value
		}
	default:
		resp.Status = 1
	}
	d.mu.Unlock()
	b, _ := json.Marshal(resp)
	return b
}

func crc16(data []byte) uint16 {
	crc := uint16(0xffff)
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&1 == 1 {
				crc = crc>>1 ^ 0xa001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
