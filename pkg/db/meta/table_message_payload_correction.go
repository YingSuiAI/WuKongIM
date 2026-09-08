package meta

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"unicode/utf8"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/engine"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/keycodec"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/rowcodec"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/schema"
)

// MaxCorrectedPayloadBytes bounds one corrected application message body.
const MaxCorrectedPayloadBytes = 256 * 1024

// MessagePayloadCorrectionKey locates one immutable message sequence in a channel.
type MessagePayloadCorrectionKey struct {
	ChannelID   string `json:"channel_id"`
	ChannelType int64  `json:"channel_type"`
	MessageSeq  uint64 `json:"message_seq"`
}

// MessagePayloadCorrection is a create-only current-body projection. Original
// log bytes, their indexes, replication identity and SEND receipt are untouched.
type MessagePayloadCorrection struct {
	ChannelID              string `json:"channel_id"`
	ChannelType            int64  `json:"channel_type"`
	MessageSeq             uint64 `json:"message_seq"`
	MessageID              uint64 `json:"message_id"`
	FromUID                string `json:"from_uid"`
	ClientMsgNo            string `json:"client_msg_no"`
	OperationID            string `json:"operation_id"`
	OriginalPayloadSHA256  string `json:"original_payload_sha256"`
	CorrectedPayloadSHA256 string `json:"corrected_payload_sha256"`
	CorrectedPayload       []byte `json:"corrected_payload"`
}

// Key returns the channel-owned identity used by point and page reads.
func (c MessagePayloadCorrection) Key() MessagePayloadCorrectionKey {
	return MessagePayloadCorrectionKey{c.ChannelID, c.ChannelType, c.MessageSeq}
}

// Validate verifies bounded immutable input independently of its entry adapter.
func (c MessagePayloadCorrection) Validate() error {
	if validateChannelKey(ChannelKey{c.ChannelID, c.ChannelType}) != nil || c.ChannelType > 255 || c.MessageSeq == 0 || c.MessageID == 0 ||
		c.FromUID == "" || c.ClientMsgNo == "" || len(c.FromUID) > 512 || len(c.ClientMsgNo) > 512 ||
		c.OperationID == "" || len(c.OperationID) > 256 || !utf8.ValidString(c.OperationID) ||
		!ValidPayloadSHA256(c.OriginalPayloadSHA256) || !ValidPayloadSHA256(c.CorrectedPayloadSHA256) ||
		len(c.CorrectedPayload) == 0 || len(c.CorrectedPayload) > MaxCorrectedPayloadBytes {
		return dberrors.ErrInvalidArgument
	}
	sum := sha256.Sum256(c.CorrectedPayload)
	if hex.EncodeToString(sum[:]) != c.CorrectedPayloadSHA256 {
		return dberrors.ErrInvalidArgument
	}
	return nil
}

// ValidPayloadSHA256 recognizes the one canonical digest representation.
func ValidPayloadSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

// PayloadCorrectionStatus is a deterministic result, never a Slot FSM failure.
type PayloadCorrectionStatus uint8

const (
	PayloadCorrectionApplied PayloadCorrectionStatus = iota + 1
	PayloadCorrectionReplayed
	PayloadCorrectionConflict
	PayloadCorrectionNotFound
)

// PayloadCorrectionCreateResult is published only after the metadata batch commits.
type PayloadCorrectionCreateResult struct{ Status PayloadCorrectionStatus }

var messagePayloadCorrectionTable = registerMetaTable(TableSpec[MessagePayloadCorrection]{
	ID: TableIDMessagePayloadCorrection, Name: "message_payload_correction",
	Columns: []schema.Column{
		{ID: 1, Name: "channel_id", Type: schema.TypeString, Required: true},
		{ID: 2, Name: "channel_type", Type: schema.TypeInt64, Required: true},
		{ID: 3, Name: "message_seq", Type: schema.TypeUint64, Required: true},
		{ID: 4, Name: "message_id", Type: schema.TypeUint64, Required: true},
		{ID: 5, Name: "from_uid", Type: schema.TypeString, Required: true},
		{ID: 6, Name: "client_msg_no", Type: schema.TypeString, Required: true},
		{ID: 7, Name: "operation_id", Type: schema.TypeString, Required: true},
		{ID: 8, Name: "original_payload_sha256", Type: schema.TypeString, Required: true},
		{ID: 9, Name: "corrected_payload_sha256", Type: schema.TypeString, Required: true},
		{ID: 10, Name: "corrected_payload", Type: schema.TypeBytes, Required: true},
	},
	Families: []schema.Family{{ID: 0, Name: "primary", Columns: []uint16{4, 5, 6, 7, 8, 9, 10}}},
	Primary: PrimarySpec[MessagePayloadCorrection]{
		IndexID: 1, FamilyID: 0, Name: "pk_message_payload_correction", Columns: []uint16{1, 2, 3},
		Layout: KeyLayout{KeyString, KeyInt64Ordered, KeyUint64},
		Key:    func(c MessagePayloadCorrection) KeyParts { return payloadCorrectionPrimaryKey(c.Key()) },
	},
	Validate: MessagePayloadCorrection.Validate,
	EncodeValueWithKey: func(key []byte, c MessagePayloadCorrection) ([]byte, error) {
		var w rowcodec.Writer
		_ = w.Uint64(4, c.MessageID)
		_ = w.String(5, c.FromUID)
		_ = w.String(6, c.ClientMsgNo)
		_ = w.String(7, c.OperationID)
		_ = w.String(8, c.OriginalPayloadSHA256)
		_ = w.String(9, c.CorrectedPayloadSHA256)
		_ = w.RawBytes(10, c.CorrectedPayload)
		return rowcodec.Wrap(key, 1, rowcodec.CodecColumns, rowcodec.FlagChecksum, w.Bytes()), nil
	},
	DecodeValueWithKey: decodePayloadCorrection,
})

// GetMessagePayloadCorrection returns the one projection for this base sequence.
func (s *Shard) GetMessagePayloadCorrection(ctx context.Context, key MessagePayloadCorrectionKey) (MessagePayloadCorrection, bool, error) {
	if err := s.check(ctx); err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	return messagePayloadCorrectionTable.Get(ctx, s, payloadCorrectionPrimaryKey(key))
}

// GetMessagePayloadCorrection is the compatibility store view used by cluster.
func (s *ShardStore) GetMessagePayloadCorrection(ctx context.Context, key MessagePayloadCorrectionKey) (MessagePayloadCorrection, bool, error) {
	if err := s.validate(); err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	return s.shard.GetMessagePayloadCorrection(ctx, key)
}

// ReadCurrentPayloadCorrection distinguishes an uncorrected readable message
// from a deleted projection. Retention, terminal state and correction are read
// from one storage snapshot, never independently or through a metadata cache.
func (s *ShardStore) ReadCurrentPayloadCorrection(ctx context.Context, key MessagePayloadCorrectionKey) (MessagePayloadCorrection, bool, error) {
	if err := s.validate(); err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	if err := s.shard.check(ctx); err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	snapshot, err := s.shard.db.engine.NewSnapshot()
	if err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	defer snapshot.Close()
	runtimeKey := encodeChannelRuntimeMetaRowKey(s.hashSlot, key.ChannelID, key.ChannelType, channelRuntimeMetaPrimaryFamilyID)
	value, found, err := snapshot.Get(runtimeKey)
	if err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	if !found {
		return MessagePayloadCorrection{}, false, dberrors.ErrNotFound
	}
	runtime, err := channelRuntimeMetaTable.decodeValue(runtimeKey, channelRuntimeMetaPrimaryKey(key.ChannelID, key.ChannelType), value)
	if err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	if key.MessageSeq <= runtime.RetentionThroughSeq {
		return MessagePayloadCorrection{}, false, dberrors.ErrNotFound
	}
	channelKey := encodeChannelRowKey(s.hashSlot, key.ChannelID, key.ChannelType, channelPrimaryFamilyID)
	value, found, err = snapshot.Get(channelKey)
	if err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	if found {
		channel, err := channelTable.decodeValue(channelKey, KeyParts{String(key.ChannelID), Int64Ordered(key.ChannelType)}, value)
		if err != nil {
			return MessagePayloadCorrection{}, false, err
		}
		if channel.Disband != 0 {
			return MessagePayloadCorrection{}, false, dberrors.ErrNotFound
		}
	}
	pk := payloadCorrectionPrimaryKey(key)
	correctionKey, err := messagePayloadCorrectionTable.primaryRowKey(s.hashSlot, pk)
	if err != nil {
		return MessagePayloadCorrection{}, false, err
	}
	value, found, err = snapshot.Get(correctionKey)
	if err != nil || !found {
		return MessagePayloadCorrection{}, found, err
	}
	correction, err := messagePayloadCorrectionTable.decodeValue(correctionKey, pk, value)
	return correction, true, err
}

// CreateMessagePayloadCorrection applies a create-only correction behind current
// retention. Replay/conflict are committed no-ops so they cannot poison a Slot.
func (b *WriteBatch) CreateMessagePayloadCorrection(hashSlot uint16, correction MessagePayloadCorrection) (*PayloadCorrectionCreateResult, error) {
	if err := b.ensure(); err != nil {
		return nil, err
	}
	if err := correction.Validate(); err != nil {
		return nil, err
	}
	correction.CorrectedPayload = append([]byte(nil), correction.CorrectedPayload...)
	hs := HashSlot(hashSlot)
	pk := payloadCorrectionPrimaryKey(correction.Key())
	key, err := messagePayloadCorrectionTable.primaryRowKey(hs, pk)
	if err != nil {
		return nil, err
	}
	result := &PayloadCorrectionCreateResult{}
	b.batch.addOp(hs, func(ctx context.Context, state *batchCommitState, batch *engine.Batch) error {
		metaKey := encodeChannelRuntimeMetaRowKey(hs, correction.ChannelID, correction.ChannelType, channelRuntimeMetaPrimaryFamilyID)
		meta, exists, err := state.loadRuntimeMeta(ctx, hs, metaKey, correction.ChannelID, correction.ChannelType)
		if err != nil {
			return err
		}
		if !exists || correction.MessageSeq <= meta.RetentionThroughSeq {
			result.Status = PayloadCorrectionNotFound
			return nil
		}
		channelKey := encodeChannelRowKey(hs, correction.ChannelID, correction.ChannelType, channelPrimaryFamilyID)
		channel, channelExists, err := state.loadChannel(ctx, channelKey, correction.ChannelID, correction.ChannelType)
		if err != nil {
			return err
		}
		_, deleted := state.channelDeletes[string(channelKey)]
		if deleted || (channelExists && channel.Disband != 0) {
			result.Status = PayloadCorrectionNotFound
			return nil
		}
		existing, exists, err := messagePayloadCorrectionTable.loadBatchRow(state, hs, pk, key)
		if err != nil {
			return err
		}
		if exists {
			result.Status = PayloadCorrectionConflict
			if payloadCorrectionsEqual(existing, correction) {
				result.Status = PayloadCorrectionReplayed
			}
			return nil
		}
		value, err := messagePayloadCorrectionTable.encodeValue(key, correction)
		if err != nil {
			return err
		}
		if err := batch.Set(key, value); err != nil {
			return err
		}
		state.tableRows[string(key)] = tableRowOverlay{value: value, exists: true}
		result.Status = PayloadCorrectionApplied
		return nil
	})
	return result, nil
}

func payloadCorrectionPrimaryKey(key MessagePayloadCorrectionKey) KeyParts {
	return KeyParts{String(key.ChannelID), Int64Ordered(key.ChannelType), Uint64(key.MessageSeq)}
}

// stagePurgePayloadCorrections shares the owning retention/deletion commit.
// The range is confined to this exact channel/type and sequence prefix.
func stagePurgePayloadCorrections(batch *engine.Batch, hashSlot HashSlot, channelID string, channelType int64, through uint64) error {
	start, err := messagePayloadCorrectionTable.primaryRowKey(hashSlot, payloadCorrectionPrimaryKey(MessagePayloadCorrectionKey{channelID, channelType, 0}))
	if err != nil {
		return err
	}
	last, err := messagePayloadCorrectionTable.primaryRowKey(hashSlot, payloadCorrectionPrimaryKey(MessagePayloadCorrectionKey{channelID, channelType, through}))
	if err != nil {
		return err
	}
	return batch.DeleteRange(engine.Span{Start: start, End: keycodec.NewPrefixSpan(last).End})
}

func payloadCorrectionsEqual(a, b MessagePayloadCorrection) bool {
	return a.Key() == b.Key() && a.MessageID == b.MessageID && a.FromUID == b.FromUID && a.ClientMsgNo == b.ClientMsgNo &&
		a.OperationID == b.OperationID && a.OriginalPayloadSHA256 == b.OriginalPayloadSHA256 &&
		a.CorrectedPayloadSHA256 == b.CorrectedPayloadSHA256 && bytes.Equal(a.CorrectedPayload, b.CorrectedPayload)
}

func decodePayloadCorrection(key []byte, primary KeyParts, value []byte) (MessagePayloadCorrection, error) {
	var c MessagePayloadCorrection
	envelope, err := rowcodec.Unwrap(key, value)
	if err != nil || envelope.Version != 1 || envelope.Codec != rowcodec.CodecColumns {
		return c, dberrors.ErrCorruptValue
	}
	c.ChannelID, c.ChannelType, c.MessageSeq = primary[0].S, primary[1].I64, primary[2].U64
	scanner := rowcodec.NewScanner(envelope.Payload)
	for scanner.Next() {
		switch scanner.ColumnID() {
		case 4:
			c.MessageID, err = scanner.Uint64()
		case 5:
			c.FromUID, err = scanner.String()
		case 6:
			c.ClientMsgNo, err = scanner.String()
		case 7:
			c.OperationID, err = scanner.String()
		case 8:
			c.OriginalPayloadSHA256, err = scanner.String()
		case 9:
			c.CorrectedPayloadSHA256, err = scanner.String()
		case 10:
			var payload []byte
			payload, err = scanner.Bytes()
			c.CorrectedPayload = append([]byte(nil), payload...)
		}
		if err != nil {
			return MessagePayloadCorrection{}, err
		}
	}
	if scanner.Err() != nil || c.Validate() != nil {
		return MessagePayloadCorrection{}, dberrors.ErrCorruptValue
	}
	return c, nil
}
