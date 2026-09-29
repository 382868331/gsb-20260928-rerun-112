package gateway

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	AlphaProtocol      = "alpha-fixed-binary"
	AlphaFrameLen      = 9
	AlphaRead          = byte(0x03)
	AlphaWrite         = byte(0x06)
	AlphaStatusOK      = byte(0x00)
	AlphaStatusError   = byte(0x01)
	AlphaStatusUnknown = byte(0x02)
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
	if buf[0] != 0xA5 || buf[8] != crc8(buf[:8]) {
		return AlphaRequest{}, buf, errors.New("invalid alpha frame")
	}
	req := AlphaRequest{
		Address:  binary.BigEndian.Uint16(buf[1:3]),
		Function: buf[3],
		Value:    int16(binary.BigEndian.Uint16(buf[4:6])),
		Register: binary.BigEndian.Uint16(buf[6:8]),
	}
	if req.Function != AlphaRead && req.Function != AlphaWrite {
		return req, buf, fmt.Errorf("%w: function %d", ErrUnsupportedOp, req.Function)
	}
	return req, buf, nil
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
