package cluster

import (
	"context"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

// ReadCommittedHead maps the narrow service read to the routed cluster facade.
// Absence and infrastructure failures are classified by that authoritative read.
func (s *ChannelMetadataStore) ReadCommittedHead(ctx context.Context, key channelusecase.ChannelKey) (uint64, error) {
	if s == nil {
		return 0, channelusecase.ErrCommittedHeadUnavailable
	}
	reader, ok := s.node.(interface {
		ReadChannelCommittedHead(context.Context, ch.ChannelID) (uint64, error)
	})
	if !ok {
		return 0, channelusecase.ErrCommittedHeadUnavailable
	}
	return reader.ReadChannelCommittedHead(ctx, ch.ChannelID{ID: key.ChannelID, Type: key.ChannelType})
}
