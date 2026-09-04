package channels

import (
	"context"
	"errors"
	"math"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

// MaxCommittedMessagesPageLimit bounds one internal recovery page.
const MaxCommittedMessagesPageLimit = 100

// CommittedMessagesRequest fences one ordered scan page to an exact Channel Leader.
type CommittedMessagesRequest struct {
	ChannelID                   ch.ChannelID
	AfterMessageSeq             uint64
	Limit                       int
	ScanHead                    uint64
	ExpectedLeader              ch.NodeID
	ExpectedChannelEpoch        uint64
	ExpectedLeaderEpoch         uint64
	ExpectedMinISR              int
	ExpectedRetentionThroughSeq uint64
}

// CommittedMessagesResult is one page within a fixed committed-head snapshot.
type CommittedMessagesResult struct {
	Messages                 []ch.Message
	ScanHead                 uint64
	FirstAvailableMessageSeq uint64
	NextAfterMessageSeq      uint64
	RetentionGap             bool
	HasMore                  bool
}

type committedMessagesForwarder interface {
	ForwardCommittedMessages(context.Context, ch.NodeID, CommittedMessagesRequest) (CommittedMessagesResult, error)
}

// ReadCommittedMessages reads one service-only ordered Channel page. A zero
// scanHead captures a fresh committed frontier; callers echo the returned head
// on later pages to exclude concurrent appends.
func (s *Service) ReadCommittedMessages(ctx context.Context, id ch.ChannelID, afterMessageSeq uint64, limit int, scanHead uint64) (CommittedMessagesResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return CommittedMessagesResult{}, false, err
	}
	if id.ID == "" || id.Type == 0 || limit < 1 || limit > MaxCommittedMessagesPageLimit || (scanHead > 0 && afterMessageSeq > scanHead) {
		return CommittedMessagesResult{}, false, ch.ErrInvalidConfig
	}
	meta, ok, err := s.resolveReadMeta(ctx, id)
	if errors.Is(err, ch.ErrChannelNotFound) || errors.Is(err, metadb.ErrNotFound) {
		return CommittedMessagesResult{}, false, nil
	}
	if err != nil {
		return CommittedMessagesResult{}, false, err
	}
	if !ok || !validCommittedHeadMeta(id, meta) {
		return CommittedMessagesResult{}, false, ch.ErrNotReady
	}
	req := CommittedMessagesRequest{
		ChannelID: id, AfterMessageSeq: afterMessageSeq, Limit: limit, ScanHead: scanHead,
		ExpectedLeader: meta.Leader, ExpectedChannelEpoch: meta.Epoch,
		ExpectedLeaderEpoch: meta.LeaderEpoch, ExpectedMinISR: meta.MinISR,
		ExpectedRetentionThroughSeq: meta.RetentionThroughSeq,
	}
	var result CommittedMessagesResult
	if meta.Leader != s.localNode {
		forward, ok := s.forward.(committedMessagesForwarder)
		if !ok {
			return CommittedMessagesResult{}, false, ch.ErrNotReady
		}
		result, err = forward.ForwardCommittedMessages(ctx, meta.Leader, req)
	} else {
		result, err = s.handleForwardCommittedMessages(ctx, req)
	}
	if err != nil {
		return CommittedMessagesResult{}, false, err
	}
	if !validCommittedMessagesResult(id, req, result) {
		return CommittedMessagesResult{}, false, ch.ErrNotReady
	}
	return result, true, nil
}

func (s *Service) handleForwardCommittedMessages(ctx context.Context, req CommittedMessagesRequest) (CommittedMessagesResult, error) {
	meta, err := s.validateCommittedMessagesRoute(ctx, req)
	if err != nil {
		return CommittedMessagesResult{}, err
	}
	if s.store == nil {
		return CommittedMessagesResult{}, ch.ErrNotReady
	}
	store, err := s.store.ChannelStore(ch.ChannelKeyForID(req.ChannelID), req.ChannelID)
	if err != nil {
		return CommittedMessagesResult{}, err
	}
	defer func() { _ = store.Close() }()
	headReq := CommittedHeadRequest{
		ChannelID: req.ChannelID, ExpectedLeader: req.ExpectedLeader,
		ExpectedChannelEpoch: req.ExpectedChannelEpoch, ExpectedLeaderEpoch: req.ExpectedLeaderEpoch,
		ExpectedMinISR: req.ExpectedMinISR,
	}
	committed, err := s.readCommittedFrontier(ctx, headReq, meta, store)
	if err != nil {
		return CommittedMessagesResult{}, err
	}
	if meta.RetentionThroughSeq > committed || meta.RetentionThroughSeq == math.MaxUint64 {
		return CommittedMessagesResult{}, ch.ErrNotReady
	}
	scanHead := req.ScanHead
	if scanHead == 0 {
		scanHead = committed
	} else if scanHead > committed {
		return CommittedMessagesResult{}, ch.ErrNotReady
	}
	if req.AfterMessageSeq > scanHead {
		return CommittedMessagesResult{}, ch.ErrNotReady
	}
	firstAvailable := meta.RetentionThroughSeq + 1
	result := CommittedMessagesResult{
		ScanHead:                 scanHead,
		FirstAvailableMessageSeq: firstAvailable,
		RetentionGap:             req.AfterMessageSeq < meta.RetentionThroughSeq,
	}
	if req.AfterMessageSeq < scanHead {
		fromSeq := req.AfterMessageSeq + 1
		if result.RetentionGap {
			fromSeq = firstAvailable
		}
		if fromSeq <= scanHead {
			read, err := store.ReadCommitted(ctx, channelstore.ReadCommittedRequest{
				FromSeq: fromSeq, MaxSeq: scanHead, MinSeq: firstAvailable,
				Limit: req.Limit + 1, MaxBytes: maxInt(),
			})
			if err != nil {
				return CommittedMessagesResult{}, err
			}
			result.Messages = read.Messages
			if len(result.Messages) > req.Limit {
				result.Messages = result.Messages[:req.Limit]
				result.HasMore = true
			}
		}
	}
	if result.HasMore {
		result.NextAfterMessageSeq = result.Messages[len(result.Messages)-1].MessageSeq
	} else {
		result.NextAfterMessageSeq = scanHead
	}
	if _, err := s.validateCommittedMessagesRoute(ctx, req); err != nil {
		return CommittedMessagesResult{}, err
	}
	if !validCommittedMessagesResult(req.ChannelID, req, result) {
		return CommittedMessagesResult{}, ch.ErrNotReady
	}
	return result, nil
}

func (s *Service) validateCommittedMessagesRoute(ctx context.Context, req CommittedMessagesRequest) (ch.Meta, error) {
	if s == nil || req.ChannelID.ID == "" || req.ChannelID.Type == 0 || req.Limit < 1 || req.Limit > MaxCommittedMessagesPageLimit ||
		(req.ScanHead > 0 && req.AfterMessageSeq > req.ScanHead) || req.ExpectedLeader == 0 || req.ExpectedChannelEpoch == 0 ||
		req.ExpectedLeaderEpoch == 0 || req.ExpectedMinISR < 1 {
		return ch.Meta{}, ch.ErrInvalidConfig
	}
	meta, ok, err := s.resolveReadMeta(ctx, req.ChannelID)
	if errors.Is(err, ch.ErrChannelNotFound) || errors.Is(err, metadb.ErrNotFound) {
		return ch.Meta{}, ch.ErrNotReady
	}
	if err != nil {
		return ch.Meta{}, err
	}
	if !ok || !validCommittedHeadMeta(req.ChannelID, meta) {
		return ch.Meta{}, ch.ErrNotReady
	}
	if meta.Leader != s.localNode || req.ExpectedLeader != s.localNode {
		return ch.Meta{}, ch.ErrNotLeader
	}
	if meta.Epoch != req.ExpectedChannelEpoch || meta.LeaderEpoch != req.ExpectedLeaderEpoch || meta.MinISR != req.ExpectedMinISR ||
		meta.RetentionThroughSeq != req.ExpectedRetentionThroughSeq {
		return ch.Meta{}, ch.ErrStaleMeta
	}
	return meta, nil
}

func validCommittedMessagesResult(id ch.ChannelID, req CommittedMessagesRequest, result CommittedMessagesResult) bool {
	if req.ExpectedRetentionThroughSeq == math.MaxUint64 || result.FirstAvailableMessageSeq != req.ExpectedRetentionThroughSeq+1 ||
		(req.ScanHead > 0 && result.ScanHead != req.ScanHead) || result.ScanHead < req.AfterMessageSeq ||
		result.RetentionGap != (req.AfterMessageSeq < result.FirstAvailableMessageSeq-1) || len(result.Messages) > req.Limit {
		return false
	}
	if result.FirstAvailableMessageSeq-1 > result.ScanHead && (!result.RetentionGap || len(result.Messages) != 0 || result.HasMore) {
		return false
	}
	if result.HasMore {
		if len(result.Messages) == 0 || result.NextAfterMessageSeq != result.Messages[len(result.Messages)-1].MessageSeq || result.NextAfterMessageSeq >= result.ScanHead {
			return false
		}
	} else if result.NextAfterMessageSeq != result.ScanHead {
		return false
	}
	previous := req.AfterMessageSeq
	if result.RetentionGap {
		previous = result.FirstAvailableMessageSeq - 1
	}
	for _, message := range result.Messages {
		if message.ChannelID != id.ID || message.ChannelType != id.Type || message.MessageID == 0 || message.MessageID > math.MaxInt64 || message.MessageSeq <= previous ||
			message.MessageSeq < result.FirstAvailableMessageSeq || message.MessageSeq > result.ScanHead {
			return false
		}
		previous = message.MessageSeq
	}
	return true
}
