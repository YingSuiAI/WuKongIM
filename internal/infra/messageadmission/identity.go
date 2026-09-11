// Package messageadmission adapts mandatory application admission over authenticated HTTP.
// The provider runtime never needs to interpret the application message body.
package messageadmission

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

// ErrInvalidIdentity reports missing or malformed opaque metadata without exposing message content.
var ErrInvalidIdentity = errors.New("message admission: invalid committed application identity")

// EnvelopeIdentityReader validates the application's top-level UUIDv7 metadata, not business fields.
type EnvelopeIdentityReader struct{}

// ReadApplicationMessageID reads only committed top-level metadata, without trusting nested sender content.
func (EnvelopeIdentityReader) ReadApplicationMessageID(payload []byte) (string, error) {
	var envelope struct {
		ID string `json:"id"`
	}
	if len(payload) == 0 || json.Unmarshal(payload, &envelope) != nil || len(envelope.ID) != 36 {
		return "", ErrInvalidIdentity
	}
	id, err := uuid.Parse(envelope.ID)
	if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != envelope.ID {
		return "", ErrInvalidIdentity
	}
	return envelope.ID, nil
}
