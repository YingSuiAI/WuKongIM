package cluster

import (
	"context"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
)

// ReadChannelCommittedMessage proves one exact committed message through the
// current Channel Leader without applying a user membership check.
func (n *Node) ReadChannelCommittedMessage(ctx context.Context, id channelruntime.ChannelID, messageID, messageSeq uint64) (channelruntime.Message, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return channelruntime.Message{}, false, err
	}
	if err := n.ensureForeground(); err != nil {
		return channelruntime.Message{}, false, err
	}
	reader, ok := n.channels.(interface {
		ReadCommittedMessage(context.Context, channelruntime.ChannelID, uint64, uint64) (channelruntime.Message, bool, error)
	})
	if !ok {
		return channelruntime.Message{}, false, ErrNotStarted
	}
	return reader.ReadCommittedMessage(ctx, id, messageID, messageSeq)
}
