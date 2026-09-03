package cluster

import (
	"context"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
)

// ReadChannelCommittedHead captures a metadata-only history boundary through the
// current Channel Leader. It grants no permission to read message content.
func (n *Node) ReadChannelCommittedHead(ctx context.Context, id channelruntime.ChannelID) (uint64, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	if err := n.ensureForeground(); err != nil {
		return 0, err
	}
	reader, ok := n.channels.(interface {
		ReadCommittedHead(context.Context, channelruntime.ChannelID) (uint64, error)
	})
	if !ok {
		return 0, ErrNotStarted
	}
	return reader.ReadCommittedHead(ctx, id)
}
