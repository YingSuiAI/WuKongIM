package cluster

import (
	"context"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

// ReadChannelCommittedMessages reads one fixed-head recovery page through the current Channel Leader.
func (n *Node) ReadChannelCommittedMessages(ctx context.Context, id channelruntime.ChannelID, afterMessageSeq uint64, limit int, scanHead uint64) (clusterchannels.CommittedMessagesResult, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return clusterchannels.CommittedMessagesResult{}, false, err
	}
	if err := n.ensureForeground(); err != nil {
		return clusterchannels.CommittedMessagesResult{}, false, err
	}
	reader, ok := n.channels.(interface {
		ReadCommittedMessages(context.Context, channelruntime.ChannelID, uint64, int, uint64) (clusterchannels.CommittedMessagesResult, bool, error)
	})
	if !ok {
		return clusterchannels.CommittedMessagesResult{}, false, ErrNotStarted
	}
	return reader.ReadCommittedMessages(ctx, id, afterMessageSeq, limit, scanHead)
}
