package gateway

import "encoding/hex"

func fmtHex(data []byte) string { return hex.EncodeToString(data) }
