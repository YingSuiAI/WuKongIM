package channel

import (
	"context"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
)

var (
	ErrPayloadCorrectionInvalid  = metadb.ErrInvalidArgument
	ErrPayloadCorrectionConflict = messagepayload.ErrConflict
	ErrPayloadCorrectionNotFound = messagepayload.ErrNotFound
)

// MessagePayloadCorrection is the bounded immutable correction command shared
// with the replicated metadata owner; it does not alter SEND admission.
type MessagePayloadCorrection = metadb.MessagePayloadCorrection

// PayloadCorrectionReceipt contains only durable identity and digest metadata.
type PayloadCorrectionReceipt struct {
	Applied                bool   `json:"applied"`
	OperationID            string `json:"operation_id"`
	ChannelID              string `json:"channel_id"`
	ChannelType            int64  `json:"channel_type"`
	MessageID              uint64 `json:"message_id"`
	MessageSeq             uint64 `json:"message_seq"`
	FromUID                string `json:"from_uid"`
	ClientMsgNo            string `json:"client_msg_no"`
	OriginalPayloadSHA256  string `json:"original_payload_sha256"`
	CorrectedPayloadSHA256 string `json:"corrected_payload_sha256"`
}

type payloadCorrectionWriter interface {
	CorrectMessagePayload(context.Context, MessagePayloadCorrection) (metadb.PayloadCorrectionStatus, error)
}

// CorrectMessagePayload applies one explicit repair after validating its owned
// input. Provider storage performs exact committed-base and retention checks.
func (a *App) CorrectMessagePayload(ctx context.Context, correction MessagePayloadCorrection) (PayloadCorrectionReceipt, error) {
	if err := correction.Validate(); err != nil {
		return PayloadCorrectionReceipt{}, err
	}
	if a == nil {
		return PayloadCorrectionReceipt{}, messagepayload.ErrUnavailable
	}
	writer, ok := a.store.(payloadCorrectionWriter)
	if !ok {
		return PayloadCorrectionReceipt{}, messagepayload.ErrUnavailable
	}
	status, err := writer.CorrectMessagePayload(ctx, correction)
	if err != nil {
		return PayloadCorrectionReceipt{}, err
	}
	switch status {
	case metadb.PayloadCorrectionConflict:
		return PayloadCorrectionReceipt{}, messagepayload.ErrConflict
	case metadb.PayloadCorrectionNotFound:
		return PayloadCorrectionReceipt{}, messagepayload.ErrNotFound
	case metadb.PayloadCorrectionApplied, metadb.PayloadCorrectionReplayed:
	default:
		return PayloadCorrectionReceipt{}, messagepayload.ErrUnavailable
	}
	return PayloadCorrectionReceipt{Applied: status == metadb.PayloadCorrectionApplied,
		OperationID: correction.OperationID, ChannelID: correction.ChannelID, ChannelType: correction.ChannelType,
		MessageID: correction.MessageID, MessageSeq: correction.MessageSeq, FromUID: correction.FromUID, ClientMsgNo: correction.ClientMsgNo,
		OriginalPayloadSHA256: correction.OriginalPayloadSHA256, CorrectedPayloadSHA256: correction.CorrectedPayloadSHA256}, nil
}

// CommittedMessageClaim selects the original durable SEND key without appending.
type CommittedMessageClaim struct {
	ChannelID                     string
	ChannelType                   uint8
	FromUID                       string
	ClientMsgNo                   string
	ExpectedOriginalPayloadSHA256 string
}

type committedMessageClaimReader interface {
	LookupCommittedMessageClaim(context.Context, CommittedMessageClaim) (CommittedMessage, bool, error)
}

// LookupCommittedMessageClaim returns only a currently proven committed result.
// False is not a promise that a previously unacknowledged SEND can be replaced.
func (a *App) LookupCommittedMessageClaim(ctx context.Context, claim CommittedMessageClaim) (CommittedMessage, bool, error) {
	if claim.ChannelID == "" || claim.ChannelType == 0 || claim.FromUID == "" || claim.ClientMsgNo == "" || !metadb.ValidPayloadSHA256(claim.ExpectedOriginalPayloadSHA256) {
		return CommittedMessage{}, false, metadb.ErrInvalidArgument
	}
	if a == nil {
		return CommittedMessage{}, false, messagepayload.ErrUnavailable
	}
	reader, ok := a.store.(committedMessageClaimReader)
	if !ok {
		return CommittedMessage{}, false, messagepayload.ErrUnavailable
	}
	return reader.LookupCommittedMessageClaim(ctx, claim)
}
