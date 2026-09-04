package channels

import (
	"context"
	"errors"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

// CommittedMessageRequest fences one exact immutable message proof to the
// current Channel Leader. MessageID and MessageSeq must identify the same row.
type CommittedMessageRequest struct {
	ChannelID            ch.ChannelID
	MessageID            uint64
	MessageSeq           uint64
	ExpectedLeader       ch.NodeID
	ExpectedChannelEpoch uint64
	ExpectedLeaderEpoch  uint64
	ExpectedMinISR       int
}

// CommittedMessageResult contains one exact committed message when Found is true.
type CommittedMessageResult struct {
	Message ch.Message
	Found   bool
}

type committedMessageForwarder interface {
	ForwardCommittedMessage(context.Context, ch.NodeID, CommittedMessageRequest) (CommittedMessageResult, error)
}

// ReadCommittedMessage returns one exact committed message without applying
// user membership. It never enumerates history or falls back to another read.
func (s *Service) ReadCommittedMessage(ctx context.Context, id ch.ChannelID, messageID, messageSeq uint64) (ch.Message, bool, error) {
	if err := ctx.Err(); err != nil {
		return ch.Message{}, false, err
	}
	if id.ID == "" || id.Type == 0 || messageID == 0 || messageSeq == 0 {
		return ch.Message{}, false, ch.ErrInvalidConfig
	}
	meta, ok, err := s.resolveReadMeta(ctx, id)
	if errors.Is(err, ch.ErrChannelNotFound) || errors.Is(err, metadb.ErrNotFound) {
		return ch.Message{}, false, nil
	}
	if err != nil {
		return ch.Message{}, false, err
	}
	if !ok || !validCommittedHeadMeta(id, meta) {
		return ch.Message{}, false, ch.ErrNotReady
	}
	if messageSeq <= meta.RetentionThroughSeq {
		return ch.Message{}, false, nil
	}
	req := CommittedMessageRequest{
		ChannelID: id, MessageID: messageID, MessageSeq: messageSeq,
		ExpectedLeader: meta.Leader, ExpectedChannelEpoch: meta.Epoch,
		ExpectedLeaderEpoch: meta.LeaderEpoch, ExpectedMinISR: meta.MinISR,
	}
	if meta.Leader != s.localNode {
		forward, ok := s.forward.(committedMessageForwarder)
		if !ok {
			return ch.Message{}, false, ch.ErrNotReady
		}
		result, err := forward.ForwardCommittedMessage(ctx, meta.Leader, req)
		if err == nil && result.Found && !committedMessageMatches(result.Message, req) {
			return ch.Message{}, false, ch.ErrNotReady
		}
		return result.Message, result.Found, err
	}
	result, err := s.handleForwardCommittedMessage(ctx, req)
	return result.Message, result.Found, err
}

func (s *Service) handleForwardCommittedMessage(ctx context.Context, req CommittedMessageRequest) (CommittedMessageResult, error) {
	meta, err := s.validateCommittedMessageRoute(ctx, req)
	if err != nil {
		return CommittedMessageResult{}, err
	}
	if req.MessageSeq <= meta.RetentionThroughSeq {
		return CommittedMessageResult{}, nil
	}
	if s.store == nil {
		return CommittedMessageResult{}, ch.ErrNotReady
	}
	store, err := s.store.ChannelStore(ch.ChannelKeyForID(req.ChannelID), req.ChannelID)
	if err != nil {
		return CommittedMessageResult{}, err
	}
	defer func() { _ = store.Close() }()
	headReq := CommittedHeadRequest{
		ChannelID: req.ChannelID, ExpectedLeader: req.ExpectedLeader,
		ExpectedChannelEpoch: req.ExpectedChannelEpoch, ExpectedLeaderEpoch: req.ExpectedLeaderEpoch,
		ExpectedMinISR: req.ExpectedMinISR,
	}
	committed, err := s.readCommittedFrontier(ctx, headReq, meta, store)
	if err != nil {
		return CommittedMessageResult{}, err
	}
	if req.MessageSeq > committed {
		if _, err := s.validateCommittedMessageRoute(ctx, req); err != nil {
			return CommittedMessageResult{}, err
		}
		return CommittedMessageResult{}, nil
	}
	read, err := store.ReadCommitted(ctx, channelstore.ReadCommittedRequest{
		FromSeq: req.MessageSeq, MaxSeq: req.MessageSeq, MinSeq: req.MessageSeq,
		Limit: 1, MaxBytes: maxInt(),
	})
	if err != nil {
		return CommittedMessageResult{}, err
	}
	current, err := s.validateCommittedMessageRoute(ctx, req)
	if err != nil {
		return CommittedMessageResult{}, err
	}
	if req.MessageSeq <= current.RetentionThroughSeq || len(read.Messages) != 1 {
		return CommittedMessageResult{}, nil
	}
	message := read.Messages[0]
	if !committedMessageMatches(message, req) {
		return CommittedMessageResult{}, nil
	}
	return CommittedMessageResult{Message: message, Found: true}, nil
}

func committedMessageMatches(message ch.Message, req CommittedMessageRequest) bool {
	return message.MessageID == req.MessageID && message.MessageSeq == req.MessageSeq &&
		message.ChannelID == req.ChannelID.ID && message.ChannelType == req.ChannelID.Type
}

func (s *Service) validateCommittedMessageRoute(ctx context.Context, req CommittedMessageRequest) (ch.Meta, error) {
	if s == nil || req.ChannelID.ID == "" || req.ChannelID.Type == 0 || req.MessageID == 0 || req.MessageSeq == 0 ||
		req.ExpectedLeader == 0 || req.ExpectedChannelEpoch == 0 || req.ExpectedLeaderEpoch == 0 || req.ExpectedMinISR < 1 {
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
	if meta.Epoch != req.ExpectedChannelEpoch || meta.LeaderEpoch != req.ExpectedLeaderEpoch || meta.MinISR != req.ExpectedMinISR {
		return ch.Meta{}, ch.ErrStaleMeta
	}
	return meta, nil
}
