// Package messagepayload defines the small immutable proof shared by current
// message readers. It owns no storage, routing, or repair admission policy.
package messagepayload

import "errors"

var (
	ErrConflict    = errors.New("message payload correction conflicts with immutable identity")
	ErrNotFound    = errors.New("committed message for payload correction not found")
	ErrUnavailable = errors.New("current message payload authority unavailable")
)

// Proof links a current body to its unchanged original committed payload.
// It is response metadata, never part of the application message body.
type Proof struct {
	OperationID            string `json:"operation_id"`
	OriginalPayloadSHA256  string `json:"original_payload_sha256"`
	CorrectedPayloadSHA256 string `json:"corrected_payload_sha256"`
}

// Clone keeps response metadata owned by the reader receiving it.
func (p *Proof) Clone() *Proof {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}
