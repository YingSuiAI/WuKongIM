package cluster

import (
	"context"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	cluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
)

type currentMessagePayloadReader interface {
	ApplyMessagePayloadCorrections(context.Context, []ch.Message) ([]ch.Message, error)
}

// currentMessagePayloads is the shared adapter seam for every published body
// reader. Missing correction authority is not evidence that no correction exists.
func currentMessagePayloads(ctx context.Context, node any, messages []ch.Message) ([]ch.Message, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	reader, ok := node.(currentMessagePayloadReader)
	if !ok {
		return nil, messagepayload.ErrUnavailable
	}
	current, err := reader.ApplyMessagePayloadCorrections(ctx, messages)
	if err != nil {
		return nil, err
	}
	if len(current) != len(messages) {
		return nil, messagepayload.ErrUnavailable
	}
	return current, nil
}

func (s *ChannelMetadataStore) CorrectMessagePayload(ctx context.Context, correction metadb.MessagePayloadCorrection) (metadb.PayloadCorrectionStatus, error) {
	if s == nil {
		return 0, messagepayload.ErrUnavailable
	}
	writer, ok := s.node.(interface {
		CorrectMessagePayload(context.Context, metadb.MessagePayloadCorrection) (metadb.PayloadCorrectionStatus, error)
	})
	if !ok {
		return 0, messagepayload.ErrUnavailable
	}
	return writer.CorrectMessagePayload(ctx, correction)
}

func (s *ChannelMetadataStore) LookupCommittedMessageClaim(ctx context.Context, claim channelusecase.CommittedMessageClaim) (channelusecase.CommittedMessage, bool, error) {
	if s == nil {
		return channelusecase.CommittedMessage{}, false, messagepayload.ErrUnavailable
	}
	reader, ok := s.node.(interface {
		LookupCommittedMessageClaim(context.Context, cluster.CommittedMessageClaim) (*ch.Message, error)
	})
	if !ok {
		return channelusecase.CommittedMessage{}, false, messagepayload.ErrUnavailable
	}
	message, err := reader.LookupCommittedMessageClaim(ctx, cluster.CommittedMessageClaim{
		ChannelID: claim.ChannelID, ChannelType: claim.ChannelType, FromUID: claim.FromUID, ClientMsgNo: claim.ClientMsgNo,
		ExpectedOriginalPayloadSHA256: claim.ExpectedOriginalPayloadSHA256,
	})
	if err != nil || message == nil {
		return channelusecase.CommittedMessage{}, false, err
	}
	return committedMessageFromCurrent(*message), true, nil
}

func committedMessageFromCurrent(message ch.Message) channelusecase.CommittedMessage {
	return channelusecase.CommittedMessage{MessageID: message.MessageID, MessageSeq: message.MessageSeq,
		ChannelID: message.ChannelID, ChannelType: message.ChannelType, Setting: message.Setting,
		FromUID: message.FromUID, ClientMsgNo: message.ClientMsgNo, ServerTimestampMS: message.ServerTimestampMS, SyncOnce: message.SyncOnce,
		Payload: append([]byte(nil), message.Payload...), PayloadCorrection: message.PayloadCorrection.Clone()}
}
