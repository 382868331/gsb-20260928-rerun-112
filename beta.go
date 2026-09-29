package gateway

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	BetaProtocol = "beta-length-tlv"
	BetaRead     = byte(1)
	BetaWrite    = byte(2)
	BetaOK       = byte(0)
	BetaError    = byte(1)
	BetaUnknown  = byte(2)
)

type BetaRequest struct {
	Op    byte
	Tag   string
	Value int32
}

type BetaResponse struct {
	Status byte
	Tag    string
	Value  int32
}

func EncodeBetaRequest(req BetaRequest) []byte {
	tag := []byte(req.Tag)
	payload := make([]byte, 0, 6+len(tag))
	payload = append(payload, req.Op, byte(len(tag)))
	payload = append(payload, tag...)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], uint32(req.Value))
	payload = append(payload, v[:]...)
	return frame(payload)
}

func EncodeBetaResponse(resp BetaResponse) []byte {
	tag := []byte(resp.Tag)
	payload := make([]byte, 0, 6+len(tag))
	payload = append(payload, resp.Status, byte(len(tag)))
	payload = append(payload, tag...)
	var v [4]byte
	binary.LittleEndian.PutUint32(v[:], uint32(resp.Value))
	payload = append(payload, v[:]...)
	return frame(payload)
}

func frame(payload []byte) []byte {
	out := make([]byte, 0, len(payload)+3)
	out = append(out, 0x5A, byte(len(payload)))
	out = append(out, payload...)
	return append(out, checksum(payload))
}

func ReadBetaMessage(r io.Reader) ([]byte, []byte, error) {
	head := make([]byte, 2)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, head, err
	}
	if head[0] != 0x5A {
		return nil, head, errors.New("invalid beta marker")
	}
	payload := make([]byte, int(head[1]))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, append(head, payload...), err
	}
	checksumByte := make([]byte, 1)
	if _, err := io.ReadFull(r, checksumByte); err != nil {
		return nil, append(head, payload...), err
	}
	if checksumByte[0] != checksum(payload) {
		return nil, append(head, payload...), errors.New("invalid beta checksum")
	}
	return payload, append(append(head, payload...), checksumByte...), nil
}

func DecodeBetaRequest(payload []byte) (BetaRequest, error) {
	if len(payload) != int(payload[1])+6 {
		return BetaRequest{}, errors.New("short beta request")
	}
	if payload[0] != BetaRead && payload[0] != BetaWrite {
		return BetaRequest{}, fmt.Errorf("%w: op %d", ErrUnsupportedOp, payload[0])
	}
	tagLen := int(payload[1])
	return BetaRequest{Op: payload[0], Tag: string(payload[2 : 2+tagLen]), Value: int32(binary.LittleEndian.Uint32(payload[2+tagLen:]))}, nil
}

func DecodeBetaResponse(payload []byte) (BetaResponse, error) {
	if len(payload) != int(payload[1])+6 {
		return BetaResponse{}, errors.New("short beta response")
	}
	tagLen := int(payload[1])
	return BetaResponse{Status: payload[0], Tag: string(payload[2 : 2+tagLen]), Value: int32(binary.LittleEndian.Uint32(payload[2+tagLen:]))}, nil
}

func checksum(data []byte) byte {
	var sum byte
	for _, b := range data {
		sum += b
	}
	return ^sum + 1
}
