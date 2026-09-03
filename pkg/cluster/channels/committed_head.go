package channels

import (
	"context"
	"errors"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

// CommittedHeadRequest fences a metadata-only read to one exact Channel Leader.
// It deliberately contains no reader UID or message-content selection.
type CommittedHeadRequest struct {
	// ChannelID identifies the Channel-owned log without selecting content.
	ChannelID ch.ChannelID
	// ExpectedLeader fences the read to the origin's resolved leader.
	ExpectedLeader ch.NodeID
	// ExpectedChannelEpoch rejects another Channel membership generation.
	ExpectedChannelEpoch uint64
	// ExpectedLeaderEpoch rejects another Channel leader generation.
	ExpectedLeaderEpoch uint64
	// ExpectedMinISR selects the committed frontier rule from authoritative metadata.
	ExpectedMinISR int
}

type committedHeadForwarder interface {
	ForwardCommittedHead(context.Context, ch.NodeID, CommittedHeadRequest) (uint64, error)
}

// ReadCommittedHead reads the committed sequence, including after all messages
// have been retained away. It never reads message bodies or sender indexes.
// Only authoritative absence is empty; route and storage failures remain errors.
func (s *Service) ReadCommittedHead(ctx context.Context, id ch.ChannelID) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if id.ID == "" || id.Type == 0 {
		return 0, ch.ErrInvalidConfig
	}
	meta, ok, err := s.resolveReadMeta(ctx, id)
	if errors.Is(err, ch.ErrChannelNotFound) || errors.Is(err, metadb.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !ok || !validCommittedHeadMeta(id, meta) {
		return 0, ch.ErrNotReady
	}
	req := CommittedHeadRequest{ChannelID: id, ExpectedLeader: meta.Leader, ExpectedChannelEpoch: meta.Epoch, ExpectedLeaderEpoch: meta.LeaderEpoch, ExpectedMinISR: meta.MinISR}
	if meta.Leader != s.localNode {
		forward, ok := s.forward.(committedHeadForwarder)
		if !ok {
			return 0, ch.ErrNotReady
		}
		return forward.ForwardCommittedHead(ctx, meta.Leader, req)
	}
	return s.handleForwardCommittedHead(ctx, req)
}

// handleForwardCommittedHead validates routing before and after the scalar
// read. A peer's stale absence must not become a false zero on the origin.
func (s *Service) handleForwardCommittedHead(ctx context.Context, req CommittedHeadRequest) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.validateCommittedHeadRoute(ctx, req); err != nil {
		return 0, err
	}
	if s.store == nil {
		return 0, ch.ErrNotReady
	}
	store, err := s.store.ChannelStore(ch.ChannelKeyForID(req.ChannelID), req.ChannelID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = store.Close() }()
	committed, err := s.readCommittedFrontier(ctx, req, store)
	if err != nil {
		return 0, err
	}
	if err := s.validateCommittedHeadRoute(ctx, req); err != nil {
		return 0, err
	}
	return committed, nil
}

func (s *Service) readCommittedFrontier(ctx context.Context, req CommittedHeadRequest, store channelstore.ChannelStore) (uint64, error) {
	state, err := store.Load(ctx)
	if err != nil {
		return 0, err
	}
	committed := state.HW
	if req.ExpectedMinISR == 1 {
		committed = state.LEO
	} else {
		liveHW, loaded, checked, err := s.readCommittedHeadRuntime(ctx, req)
		if err != nil {
			return 0, err
		}
		if !checked {
			// A quorum Channel's durable HW may trail the loaded leader. Without
			// an exact runtime probe, returning it as the current committed head
			// would silently lower the authoritative frontier.
			return 0, ch.ErrNotReady
		}
		if loaded {
			committed = maxUint64Value(committed, liveHW)
		} else {
			// A runtime may have evicted after the first durable read. Reloading
			// observes the checkpoint flushed by that eviction without reading
			// any message row or secondary index.
			state, err = store.Load(ctx)
			if err != nil {
				return 0, err
			}
			committed = maxUint64Value(committed, state.HW)
		}
	}
	return committed, nil
}

// readCommittedHeadRuntime distinguishes a loaded leader from an unloaded
// channel so a coalesced durable HW checkpoint cannot lower the captured head.
func (s *Service) readCommittedHeadRuntime(ctx context.Context, req CommittedHeadRequest) (hw uint64, loaded bool, checked bool, err error) {
	probeRuntime, ok := s.runtime.(conversationRuntimeProbe)
	if !ok {
		return 0, false, false, nil
	}
	probe, err := probeRuntime.RuntimeProbe(ctx, ch.RuntimeSelector{ChannelIDs: []ch.ChannelID{req.ChannelID}})
	if err != nil {
		return 0, false, true, err
	}
	if len(probe.Channels) == 1 && len(probe.Missing) == 0 && probe.Channels[0].ChannelID == req.ChannelID {
		channel := probe.Channels[0]
		switch {
		case channel.Role != ch.RoleLeader:
			return 0, false, true, ch.ErrNotLeader
		case channel.Status != ch.StatusActive:
			return 0, false, true, ch.ErrNotReady
		case channel.ChannelEpoch != req.ExpectedChannelEpoch || channel.LeaderEpoch != req.ExpectedLeaderEpoch:
			return 0, false, true, ch.ErrStaleMeta
		case channel.HW > channel.LEO:
			return 0, false, true, ch.ErrNotReady
		default:
			return channel.HW, true, true, nil
		}
	}
	if len(probe.Channels) == 0 && len(probe.Missing) == 1 && probe.Missing[0] == req.ChannelID {
		return 0, false, true, nil
	}
	return 0, false, true, ch.ErrNotReady
}

func (s *Service) validateCommittedHeadRoute(ctx context.Context, req CommittedHeadRequest) error {
	if s == nil || req.ChannelID.ID == "" || req.ChannelID.Type == 0 || req.ExpectedLeader == 0 ||
		req.ExpectedChannelEpoch == 0 || req.ExpectedLeaderEpoch == 0 || req.ExpectedMinISR < 1 {
		return ch.ErrInvalidConfig
	}
	meta, ok, err := s.resolveReadMeta(ctx, req.ChannelID)
	if errors.Is(err, ch.ErrChannelNotFound) || errors.Is(err, metadb.ErrNotFound) {
		return ch.ErrNotReady
	}
	if err != nil {
		return err
	}
	if !ok || !validCommittedHeadMeta(req.ChannelID, meta) {
		return ch.ErrNotReady
	}
	if meta.Leader != s.localNode || req.ExpectedLeader != s.localNode {
		return ch.ErrNotLeader
	}
	if meta.Epoch != req.ExpectedChannelEpoch || meta.LeaderEpoch != req.ExpectedLeaderEpoch || meta.MinISR != req.ExpectedMinISR {
		return ch.ErrStaleMeta
	}
	return nil
}

// validCommittedHeadMeta narrows append-authority validation for a scalar
// committed-head read. The read must not trust a metadata source to have
// already checked the active topology or its epoch fences.
func validCommittedHeadMeta(id ch.ChannelID, meta ch.Meta) bool {
	if !cacheableAppendMeta(id, meta) || meta.Status != ch.StatusActive || meta.Epoch == 0 || meta.LeaderEpoch == 0 {
		return false
	}
	replicas := make(map[ch.NodeID]struct{}, len(meta.Replicas))
	for _, replica := range meta.Replicas {
		if replica == 0 {
			return false
		}
		if _, exists := replicas[replica]; exists {
			return false
		}
		replicas[replica] = struct{}{}
	}
	isr := make(map[ch.NodeID]struct{}, len(meta.ISR))
	for _, replica := range meta.ISR {
		if replica == 0 {
			return false
		}
		if _, exists := isr[replica]; exists {
			return false
		}
		isr[replica] = struct{}{}
	}
	return true
}
