package cluster

import (
	"context"
	"errors"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

// ReadCommittedMessages maps one service recovery page to the routed cluster facade.
func (s *ChannelMetadataStore) ReadCommittedMessages(ctx context.Context, key channelusecase.ChannelKey, query channelusecase.CommittedMessagesQuery) (channelusecase.CommittedMessagesPage, bool, error) {
	if s == nil {
		return channelusecase.CommittedMessagesPage{}, false, channelusecase.ErrCommittedMessagesUnavailable
	}
	reader, ok := s.node.(interface {
		ReadChannelCommittedMessages(context.Context, ch.ChannelID, uint64, int, uint64) (clusterchannels.CommittedMessagesResult, bool, error)
	})
	if !ok {
		return channelusecase.CommittedMessagesPage{}, false, channelusecase.ErrCommittedMessagesUnavailable
	}
	result, found, err := reader.ReadChannelCommittedMessages(ctx, ch.ChannelID{ID: key.ChannelID, Type: key.ChannelType}, query.AfterMessageSeq, query.Limit, query.ScanHead)
	if errors.Is(err, ch.ErrInvalidConfig) {
		return channelusecase.CommittedMessagesPage{}, false, channelusecase.ErrCommittedMessagesQuery
	}
	if err != nil || !found {
		return channelusecase.CommittedMessagesPage{}, found, err
	}
	result.Messages, err = currentMessagePayloads(ctx, s.node, result.Messages)
	if err != nil {
		return channelusecase.CommittedMessagesPage{}, false, err
	}
	page := channelusecase.CommittedMessagesPage{
		Messages: make([]channelusecase.CommittedMessage, len(result.Messages)),
		ScanHead: result.ScanHead, FirstAvailableMessageSeq: result.FirstAvailableMessageSeq,
		NextAfterMessageSeq: result.NextAfterMessageSeq, RetentionGap: result.RetentionGap, HasMore: result.HasMore,
	}
	for index, message := range result.Messages {
		page.Messages[index] = committedMessageFromCurrent(message)
	}
	return page, true, nil
}
