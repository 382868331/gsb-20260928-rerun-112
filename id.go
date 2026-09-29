package gateway

import (
	"crypto/rand"
	"encoding/hex"
)

func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "trx-" + hex.EncodeToString(b[:])
}
