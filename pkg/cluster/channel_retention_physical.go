package cluster

import (
	"context"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

// ChannelRetentionGCResult summarizes one bounded local physical retention pass.
type ChannelRetentionGCResult struct {
	// ScannedChannels is the number of local message catalog entries inspected.
	ScannedChannels int
	// AppliedChannels is the number of channels that received a retention apply request.
	AppliedChannels int
	// TrimmedChannels is the number of channels where at least one message row was removed.
	TrimmedChannels int
	// BlockedChannels is the number of channels whose logical boundary was adopted but physical trim was blocked.
	BlockedChannels int
	// DeletedMessages is the total number of message rows removed in this pass.
	DeletedMessages int
	// More reports whether the next pass should continue from the stored catalog cursor.
	More bool
	// Errors counts per-channel failures after the catalog page was read.
	Errors int
	// ActivatedChannels is the number of idle Channel runtimes loaded to apply a lagging physical boundary.
	ActivatedChannels int
	// DeferredChannels is the number of idle channels over the activation cap; they are retried first on the next pass.
	DeferredChannels int
}

// ChannelRetentionView returns local Channel retention state.
func (n *Node) ChannelRetentionView(ctx context.Context, id channelruntime.ChannelID) (channelruntime.RetentionView, error) {
	if err := ctxErr(ctx); err != nil {
		return channelruntime.RetentionView{}, err
	}
	if err := n.ensureForeground(); err != nil {
		return channelruntime.RetentionView{}, err
	}
	if n.channels == nil {
		return channelruntime.RetentionView{}, ErrNotStarted
	}
	return n.channels.RetentionView(ctx, id)
}

// ApplyChannelRetentionBoundary adopts a local logical boundary and attempts bounded physical cleanup.
func (n *Node) ApplyChannelRetentionBoundary(ctx context.Context, id channelruntime.ChannelID, throughSeq uint64, opts channelruntime.RetentionApplyOptions) (channelruntime.RetentionApplyResult, error) {
	if err := ctxErr(ctx); err != nil {
		return channelruntime.RetentionApplyResult{}, err
	}
	if err := n.ensureForeground(); err != nil {
		return channelruntime.RetentionApplyResult{}, err
	}
	if n.channels == nil {
		return channelruntime.RetentionApplyResult{}, ErrNotStarted
	}
	return n.channels.ApplyRetentionBoundary(ctx, channelruntime.RetentionApplyRequest{
		ChannelID:  id,
		ThroughSeq: throughSeq,
		Options:    opts,
	})
}

// RunChannelRetentionGCOnce scans one local channel catalog page and applies committed retention boundaries.
func (n *Node) RunChannelRetentionGCOnce(ctx context.Context) (ChannelRetentionGCResult, error) {
	if err := ctxErr(ctx); err != nil {
		return ChannelRetentionGCResult{}, err
	}
	if err := n.ensureForeground(); err != nil {
		return ChannelRetentionGCResult{}, err
	}
	if n.defaultChannelStore == nil || n.defaultSlotMetaDB == nil || n.channels == nil {
		return ChannelRetentionGCResult{}, ErrNotStarted
	}

	n.channelRetentionGCMu.Lock()
	defer n.channelRetentionGCMu.Unlock()

	page, cursor, more, err := n.defaultChannelStore.ListChannelsPage(ctx, n.channelRetentionCursor, n.cfg.ChannelRetention.ChannelBatchSize)
	if err != nil {
		return ChannelRetentionGCResult{}, err
	}
	// Channels deferred by the activation cap on the previous pass go first so a backlog drains at the cap per
	// pass instead of waiting a full catalog cycle; the carried list is bounded by the batch size.
	entries := append(append([]channelstore.ChannelCatalogEntry(nil), n.channelRetentionDeferred...), page...)
	n.channelRetentionDeferred = nil
	deferredSeen := make(map[channelruntime.ChannelKey]bool)
	result := ChannelRetentionGCResult{ScannedChannels: len(entries), More: more}
	opts := channelruntime.RetentionApplyOptions{
		MaxTrimMessages: n.cfg.ChannelRetention.MaxTrimMessages,
		MaxTrimBytes:    n.cfg.ChannelRetention.MaxTrimBytes,
	}
	for _, entry := range entries {
		meta, err := n.GetChannelRuntimeMeta(ctx, entry.ID.ID, int64(entry.ID.Type))
		if err != nil {
			result.Errors++
			continue
		}
		if meta.RetentionThroughSeq == 0 {
			continue
		}
		if n.channelPhysicalRetentionSettled(ctx, entry, meta.RetentionThroughSeq) {
			continue
		}
		applied, err := n.ApplyChannelRetentionBoundary(ctx, entry.ID, meta.RetentionThroughSeq, opts)
		if err != nil && channelruntime.ErrorMatches(err, channelruntime.ErrChannelNotFound) && n.canActivateForRetention(meta) {
			if result.ActivatedChannels >= n.cfg.ChannelRetention.MaxActivationsPerPass {
				// Over the cap: not a failure, retry first on the next pass.
				result.DeferredChannels++
				if len(n.channelRetentionDeferred) < n.cfg.ChannelRetention.ChannelBatchSize && !deferredSeen[entry.Key] {
					deferredSeen[entry.Key] = true
					n.channelRetentionDeferred = append(n.channelRetentionDeferred, entry)
				}
				continue
			}
			// An idle-evicted runtime cannot apply the boundary. Load it once from authoritative Slot metadata
			// (leader or follower role as the meta says); the cap bounds recovery probes and load per pass.
			result.ActivatedChannels++
			if err = n.applyChannelMigrationLocalRuntimeMeta(ctx, meta); err == nil {
				applied, err = n.ApplyChannelRetentionBoundary(ctx, entry.ID, meta.RetentionThroughSeq, opts)
			}
		}
		if err != nil {
			result.Errors++
			continue
		}
		result.AppliedChannels++
		if applied.Deleted > 0 {
			result.TrimmedChannels++
			result.DeletedMessages += applied.Deleted
		}
		if applied.BlockedReason != "" {
			result.BlockedChannels++
		}
	}
	if more {
		n.channelRetentionCursor = cursor
	} else {
		n.channelRetentionCursor = ""
	}
	return result, nil
}

// canActivateForRetention reports whether this node may load a runtime for meta: active metadata of a
// replica hosted here. Deleting or creating channels keep their lifecycle owners.
func (n *Node) canActivateForRetention(meta metadb.ChannelRuntimeMeta) bool {
	return channelruntime.Status(meta.Status) == channelruntime.StatusActive && containsNodeID(meta.Replicas, n.cfg.NodeID)
}

// channelPhysicalRetentionSettled reports whether the local store already physically reached the boundary,
// read without loading a Channel runtime so a settled idle channel never costs an activation.
func (n *Node) channelPhysicalRetentionSettled(ctx context.Context, entry channelstore.ChannelCatalogEntry, boundary uint64) bool {
	store, err := n.defaultChannelStore.ChannelStore(entry.Key, entry.ID)
	if err != nil || store == nil {
		return false
	}
	defer func() { _ = store.Close() }()
	state, err := store.LoadRetentionState(ctx)
	return err == nil && state.PhysicalRetentionThroughSeq >= boundary
}
