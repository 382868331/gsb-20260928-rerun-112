package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// VerifyResult is the read-back evidence used to resolve an uncertain write.
// Outcome values:
//   - VerifyConfirmed: the device currently holds exactly the requested raw value.
//   - VerifyMismatch:  the device responded, but holds a different value; the
//     original write cannot be treated as applied.
//   - VerifyUnknown:   the read-back itself failed; uncertainty persists and the
//     caller must verify again later rather than resubmitting the write.
type VerifyResult struct {
	Field    string       `json:"field"`
	BetaTag  string       `json:"beta_tag"`
	Outcome  string       `json:"outcome"`
	Expected float64      `json:"expected_beta_raw"`
	Observed *float64     `json:"observed_beta_raw,omitempty"`
	Frames   []TraceFrame `json:"frames"`
	Error    string       `json:"error,omitempty"`
}

// VerifyWrite performs a read-only follow-up for one item of a batch (or a
// single write) whose result was unknown. It never sends another write, so a
// timed-out modification can never be silently turned into a second mutation.
func (c *Channel) VerifyWrite(ctx context.Context, field string, expectedBetaRaw float64) VerifyResult {
	result := VerifyResult{Field: field, Expected: expectedBetaRaw}
	mapping, ok := c.Mappings[field]
	if !ok {
		result.Outcome = VerifyUnknown
		result.Error = fmt.Sprintf("%s: %s", ErrUnsupportedField.Error(), field)
		return result
	}
	result.BetaTag = mapping.Beta.Tag
	recorder := frameRecorderFunc(func(direction, protocol string, data []byte) {
		result.Frames = append(result.Frames, TraceFrame{Direction: direction, Protocol: protocol, Hex: fmtHex(data)})
	})
	value, err := c.Upstream.Read(WithFrameRecorder(ctx, recorder), mapping.Beta.Tag)
	if err != nil {
		result.Outcome = VerifyUnknown
		result.Error = fmt.Sprintf("read-back unavailable: %v; retain unknown and verify again", err)
		return result
	}
	result.Observed = &value
	if math.Abs(value-expectedBetaRaw) <= 1e-9 {
		result.Outcome = VerifyConfirmed
		return result
	}
	result.Outcome = VerifyMismatch
	result.Error = fmt.Sprintf("device holds %g, expected %g", value, expectedBetaRaw)
	return result
}

// VerifyBatchItem resolves one unknown item of a BatchWriteResult by field name.
func (c *Channel) VerifyBatchItem(ctx context.Context, result BatchWriteResult, field string) (VerifyResult, error) {
	for _, item := range result.Items {
		if item.Field != field {
			continue
		}
		if item.Status != StatusUnknown {
			return VerifyResult{}, fmt.Errorf("field %s is %s, only unknown items require verification", field, item.Status)
		}
		if item.Sent == nil {
			return VerifyResult{}, errors.New("unknown item never left the gateway")
		}
		return c.VerifyWrite(ctx, field, *item.Sent), nil
	}
	return VerifyResult{}, fmt.Errorf("%w: %s", ErrUnsupportedField, field)
}
