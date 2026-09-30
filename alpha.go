package gateway

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	AlphaProtocol       = "alpha-fixed-binary"
	AlphaFrameLen       = 9
	AlphaRead           = byte(0x03)
	AlphaWrite          = byte(0x06)
	AlphaBatchRead      = byte(0x23)
	AlphaBatchWrite     = byte(0x26)
	AlphaStatusOK       = byte(0x00)
	AlphaStatusError    = byte(0x01)
	AlphaStatusUnknown  = byte(0x02)
	AlphaSnapshotAtomic = byte(0x01)
	AlphaSnapshotMixed  = byte(0x00)
)

type AlphaRequest struct {
	Address  uint16
	Function byte
	Register uint16
	Value    int16
}

func (r AlphaRequest) Operation() string {
	if r.Function == AlphaWrite {
		return OpWrite
	}
	return OpRead
}

func (r AlphaRequest) Tag() string { return fmt.Sprintf("register:%d", r.Register) }

type AlphaResponse struct {
	Address  uint16
	Function byte
	Status   byte
	Value    uint16
}

// AlphaBatchItem is one field entry in a multi-field logical operation.
type AlphaBatchItem struct {
	Register uint16
	Value    int16 // used only by batch writes
}

func (i AlphaBatchItem) Tag() string { return fmt.Sprintf("register:%d", i.Register) }

type AlphaBatchRequest struct {
	Address  uint16
	Function byte
	Items    []AlphaBatchItem
}

type AlphaBatchItemResult struct {
	Status byte
	Value  uint16
}

type AlphaBatchResponse struct {
	Address  uint16
	Function byte
	Status   byte
	Snapshot byte
	Results  []AlphaBatchItemResult
}

func IsBatchFunction(function byte) bool {
	return function == AlphaBatchRead || function == AlphaBatchWrite
}

func (r AlphaBatchRequest) Operation() string {
	if r.Function == AlphaBatchWrite {
		return OpWrite
	}
	return OpRead
}

func (r AlphaBatchRequest) Encode() []byte {
	entryLen := 2
	if r.Function == AlphaBatchWrite {
		entryLen = 4
	}
	buf := make([]byte, 0, 5+entryLen*len(r.Items))
	buf = append(buf, 0xA5)
	buf = binary.BigEndian.AppendUint16(buf, r.Address)
	buf = append(buf, r.Function, byte(len(r.Items)))
	for _, item := range r.Items {
		var entry [4]byte
		binary.BigEndian.PutUint16(entry[0:2], item.Register)
		if r.Function == AlphaBatchWrite {
			binary.BigEndian.PutUint16(entry[2:4], uint16(item.Value))
		}
		buf = append(buf, entry[:entryLen]...)
	}
	return buf
}

func (r AlphaBatchResponse) Encode() []byte {
	buf := make([]byte, 0, 6+3*len(r.Results))
	buf = append(buf, 0xA5)
	buf = binary.BigEndian.AppendUint16(buf, r.Address)
	buf = append(buf, r.Function, r.Status, r.Snapshot, byte(len(r.Results)))
	for _, result := range r.Results {
		var entry [3]byte
		entry[0] = result.Status
		binary.BigEndian.PutUint16(entry[1:3], result.Value)
		buf = append(buf, entry[:]...)
	}
	return buf
}

// ReadAlphaFrame reads either the fixed 9-byte single-field frame or the
// variable-length multi-field frame. Both share marker + 2-byte address +
// function. Single frames carry a trailing CRC; batch frames do not.
func ReadAlphaFrame(r io.Reader) (function byte, address uint16, single []byte, batch *AlphaBatchRequest, err error) {
	head := make([]byte, 4)
	if _, err = io.ReadFull(r, head); err != nil {
		return
	}
	if head[0] != 0xA5 {
		err = errors.New("invalid alpha frame")
		return
	}
	address = binary.BigEndian.Uint16(head[1:3])
	function = head[3]
	if IsBatchFunction(function) {
		countByte := make([]byte, 1)
		if _, err = io.ReadFull(r, countByte); err != nil {
			return
		}
		count := int(countByte[0])
		entryLen := 2
		if function == AlphaBatchWrite {
			entryLen = 4
		}
		body := make([]byte, count*entryLen)
		if _, err = io.ReadFull(r, body); err != nil {
			return
		}
		req := AlphaBatchRequest{Function: function, Items: make([]AlphaBatchItem, 0, count)}
		for i := 0; i < count; i++ {
			entry := body[i*entryLen:]
			item := AlphaBatchItem{Register: binary.BigEndian.Uint16(entry[:2])}
			if function == AlphaBatchWrite {
				item.Value = int16(binary.BigEndian.Uint16(entry[2:4]))
			}
			req.Items = append(req.Items, item)
		}
		batch = &req
		batch.Address = address
		return
	}
	rest := make([]byte, AlphaFrameLen-4)
	if _, err = io.ReadFull(r, rest); err != nil {
		return
	}
	single = append(head, rest...)
	if single[8] != crc8(single[:8]) {
		err = errors.New("invalid alpha frame")
		return
	}
	address = binary.BigEndian.Uint16(single[1:3])
	if function != AlphaRead && function != AlphaWrite {
		err = fmt.Errorf("%w: function %d", ErrUnsupportedOp, function)
	}
	return
}

// DecodeAlphaBatchRequest parses a complete multi-field frame.
func DecodeAlphaBatchRequest(raw []byte) (AlphaBatchRequest, error) {
	if len(raw) < 5 || raw[0] != 0xA5 || !IsBatchFunction(raw[3]) {
		return AlphaBatchRequest{}, errors.New("invalid alpha batch frame")
	}
	count := int(raw[4])
	entryLen := 2
	if raw[3] == AlphaBatchWrite {
		entryLen = 4
	}
	if len(raw) != 5+count*entryLen {
		return AlphaBatchRequest{}, errors.New("short alpha batch frame")
	}
	req := AlphaBatchRequest{Address: binary.BigEndian.Uint16(raw[1:3]), Function: raw[3], Items: make([]AlphaBatchItem, 0, count)}
	for i := 0; i < count; i++ {
		entry := raw[5+i*entryLen:]
		item := AlphaBatchItem{Register: binary.BigEndian.Uint16(entry[:2])}
		if raw[3] == AlphaBatchWrite {
			item.Value = int16(binary.BigEndian.Uint16(entry[2:4]))
		}
		req.Items = append(req.Items, item)
	}
	return req, nil
}

func (r AlphaRequest) Encode() []byte {
	buf := make([]byte, AlphaFrameLen)
	buf[0] = 0xA5
	binary.BigEndian.PutUint16(buf[1:3], r.Address)
	buf[3] = r.Function
	binary.BigEndian.PutUint16(buf[4:6], uint16(r.Value))
	binary.BigEndian.PutUint16(buf[6:8], r.Register)
	buf[8] = crc8(buf[:8])
	return buf
}

func ReadAlphaRequest(r io.Reader) (AlphaRequest, []byte, error) {
	buf := make([]byte, AlphaFrameLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return AlphaRequest{}, buf, err
	}
	req, err := DecodeAlphaRequest(buf)
	return req, buf, err
}

// DecodeAlphaRequest parses and validates a complete fixed-length frame.
func DecodeAlphaRequest(buf []byte) (AlphaRequest, error) {
	if len(buf) != AlphaFrameLen || buf[0] != 0xA5 || buf[8] != crc8(buf[:8]) {
		return AlphaRequest{}, errors.New("invalid alpha frame")
	}
	req := AlphaRequest{
		Address:  binary.BigEndian.Uint16(buf[1:3]),
		Function: buf[3],
		Value:    int16(binary.BigEndian.Uint16(buf[4:6])),
		Register: binary.BigEndian.Uint16(buf[6:8]),
	}
	if req.Function != AlphaRead && req.Function != AlphaWrite {
		return req, fmt.Errorf("%w: function %d", ErrUnsupportedOp, req.Function)
	}
	return req, nil
}

func (r AlphaResponse) Encode() []byte {
	buf := make([]byte, AlphaFrameLen)
	buf[0] = 0xA5
	binary.BigEndian.PutUint16(buf[1:3], r.Address)
	buf[3] = r.Function
	buf[4] = r.Status
	binary.BigEndian.PutUint16(buf[5:7], r.Value)
	buf[7] = 0
	buf[8] = crc8(buf[:8])
	return buf
}

func WriteAlphaResponse(w io.Writer, resp AlphaResponse) error {
	_, err := w.Write(resp.Encode())
	return err
}

func crc8(data []byte) byte {
	var crc byte
	for _, b := range data {
		crc ^= b
		for i := 0; i < 8; i++ {
			if crc&0x80 != 0 {
				crc = crc<<1 ^ 0x07
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
