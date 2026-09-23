package meta

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/engine"
)

// ChannelLargeRefreshResult is resolved against the current Channel row at
// batch commit, after earlier Slot commands in the same batch.
type ChannelLargeRefreshResult struct {
	Found   bool
	Channel Channel
}

// RefreshChannelLarge changes only Large using the durable subscriber count.
// In particular, a prior read of the Channel is never used as the write base.
func (b *WriteBatch) RefreshChannelLarge(hashSlot uint16, channelID string, channelType int64, threshold uint64) (*ChannelLargeRefreshResult, error) {
	if err := b.ensure(); err != nil {
		return nil, err
	}
	if err := validateKeyString(channelID); err != nil {
		return nil, err
	}
	hs := HashSlot(hashSlot)
	key := encodeChannelRowKey(hs, channelID, channelType, channelPrimaryFamilyID)
	result := &ChannelLargeRefreshResult{}
	b.batch.addOp(hs, func(ctx context.Context, state *batchCommitState, batch *engine.Batch) error {
		channel, exists, err := state.loadChannel(ctx, key, channelID, channelType)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		result.Found = true
		result.Channel = channel
		large := int64(0)
		if channel.SubscriberCount > threshold {
			large = 1
		}
		if channel.Large == large {
			return nil
		}
		channel.Large = large
		if err := (&Shard{db: state.db, hashSlot: hs}).stageChannel(batch, key, channel); err != nil {
			return err
		}
		state.channelPublishes[string(key)] = channel
		delete(state.channelDeletes, string(key))
		result.Channel = channel
		return nil
	})
	return result, nil
}
