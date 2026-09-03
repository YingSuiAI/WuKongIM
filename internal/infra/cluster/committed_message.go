package cluster

import (
	"context"
	"errors"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

// ReadCommittedMessage maps one exact service proof to the routed cluster facade.
func (s *ChannelMetadataStore) ReadCommittedMessage(ctx context.Context, key channelusecase.ChannelKey, identity channelusecase.CommittedMessageIdentity) (channelusecase.CommittedMessage, bool, error) {
	if s == nil {
		return channelusecase.CommittedMessage{}, false, channelusecase.ErrCommittedMessageUnavailable
	}
	reader, ok := s.node.(interface {
		ReadChannelCommittedMessage(context.Context, ch.ChannelID, uint64, uint64) (ch.Message, bool, error)
	})
	if !ok {
		return channelusecase.CommittedMessage{}, false, channelusecase.ErrCommittedMessageUnavailable
	}
	message, found, err := reader.ReadChannelCommittedMessage(ctx, ch.ChannelID{ID: key.ChannelID, Type: key.ChannelType}, identity.MessageID, identity.MessageSeq)
	if errors.Is(err, ch.ErrInvalidConfig) {
		return channelusecase.CommittedMessage{}, false, channelusecase.ErrCommittedMessageIdentity
	}
	if err != nil || !found {
		return channelusecase.CommittedMessage{}, found, err
	}
	return channelusecase.CommittedMessage{
		MessageID: message.MessageID, MessageSeq: message.MessageSeq,
		ChannelID: message.ChannelID, ChannelType: message.ChannelType, Setting: message.Setting,
		FromUID: message.FromUID, ClientMsgNo: message.ClientMsgNo,
		ServerTimestampMS: message.ServerTimestampMS, SyncOnce: message.SyncOnce,
		Payload: append([]byte(nil), message.Payload...),
	}, true, nil
}
