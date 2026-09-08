package fsm

import (
	"bytes"
	"encoding/json"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

const maxPayloadCorrectionCommandBytes = 512 * 1024

type createMessagePayloadCorrectionCmd struct {
	correction metadb.MessagePayloadCorrection
	result     *metadb.PayloadCorrectionCreateResult
}

func (c *createMessagePayloadCorrectionCmd) apply(batch *metadb.WriteBatch, hashSlot uint16) error {
	var err error
	c.result, err = batch.CreateMessagePayloadCorrection(hashSlot, c.correction)
	return err
}

func (c *createMessagePayloadCorrectionCmd) applyResult() []byte {
	if c.result == nil {
		return nil
	}
	return []byte{'W', 'K', 'P', 'C', 1, byte(c.result.Status)}
}

// EncodeMessagePayloadCorrectionCommand keeps this create-only mutation apart
// from Agent stream events and the immutable Channel append protocol.
func EncodeMessagePayloadCorrectionCommand(correction metadb.MessagePayloadCorrection) ([]byte, error) {
	if err := correction.Validate(); err != nil {
		return nil, err
	}
	value, err := json.Marshal(correction)
	if err != nil || len(value) > maxPayloadCorrectionCommandBytes-headerSize-tlvOverhead {
		return nil, metadb.ErrInvalidArgument
	}
	return appendBytesTLVField([]byte{commandVersion, cmdTypeCreateMessagePayloadCorrection}, 1, value), nil
}

func decodeCreateMessagePayloadCorrection(data []byte) (command, error) {
	if len(data) > maxPayloadCorrectionCommandBytes {
		return nil, metadb.ErrInvalidArgument
	}
	var correction metadb.MessagePayloadCorrection
	seen := false
	for len(data) > 0 {
		tag, value, size, err := readTLV(data)
		if err != nil {
			return nil, err
		}
		data = data[size:]
		if tag != 1 {
			continue
		}
		if seen || json.Unmarshal(value, &correction) != nil {
			return nil, metadb.ErrInvalidArgument
		}
		seen = true
	}
	if !seen || correction.Validate() != nil {
		return nil, metadb.ErrInvalidArgument
	}
	return &createMessagePayloadCorrectionCmd{correction: correction}, nil
}

// DecodeMessagePayloadCorrectionResult validates the typed committed no-op or
// insertion result. Expected conflicts do not become fatal Slot apply errors.
func DecodeMessagePayloadCorrectionResult(value []byte) (metadb.PayloadCorrectionStatus, error) {
	if len(value) != 6 || !bytes.Equal(value[:5], []byte{'W', 'K', 'P', 'C', 1}) ||
		value[5] < byte(metadb.PayloadCorrectionApplied) || value[5] > byte(metadb.PayloadCorrectionNotFound) {
		return 0, metadb.ErrCorruptValue
	}
	return metadb.PayloadCorrectionStatus(value[5]), nil
}
