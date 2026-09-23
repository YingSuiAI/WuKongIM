package cluster

import (
	"bytes"
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/internal/contracts/channelappend"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/WuKongIM/WuKongIM/pkg/cluster"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

// ChannelIdempotencyNode locates a durable key and proves its current committed visibility.
type ChannelIdempotencyNode interface {
	LookupChannelIdempotency(context.Context, channelruntime.ChannelID, string, string) (channelstore.IdempotencyHit, bool, error)
	ReadChannelCommittedBatch(context.Context, []clusterchannels.CommittedRead) ([]clusterchannels.CommittedReadResult, error)
}

// ChannelIdempotencyStore adapts cluster committed idempotency lookups to channelappend.
type ChannelIdempotencyStore struct {
	node                  ChannelIdempotencyNode
	applicationMessageIDs channelappend.ApplicationMessageIDReader
}

// NewChannelIdempotencyStore creates a ChannelIdempotencyStore.
func NewChannelIdempotencyStore(node ChannelIdempotencyNode, applicationMessageIDs channelappend.ApplicationMessageIDReader) *ChannelIdempotencyStore {
	return &ChannelIdempotencyStore{node: node, applicationMessageIDs: applicationMessageIDs}
}

// LookupSend returns a prior success only when the current Channel Leader exposes the indexed row.
func (s *ChannelIdempotencyStore) LookupSend(ctx context.Context, query channelappend.IdempotencyQuery) (channelappend.SendResult, bool, error) {
	if s == nil || s.node == nil || query.FromUID == "" || query.ClientMsgNo == "" || query.ChannelID == "" || query.ChannelType == 0 {
		return channelappend.SendResult{}, false, nil
	}
	hit, ok, err := s.node.LookupChannelIdempotency(ctx, channelruntime.ChannelID{ID: query.ChannelID, Type: query.ChannelType}, query.FromUID, query.ClientMsgNo)
	if err != nil || !ok {
		if channelIdempotencyLookupMissError(err) {
			return channelappend.SendResult{}, false, nil
		}
		return channelappend.SendResult{}, ok, mapAppendError(err)
	}
	if query.PayloadHash != 0 && hit.PayloadHash != query.PayloadHash {
		return channelappend.SendResult{}, false, nil
	}
	if hit.Message.MessageID == 0 || hit.Message.MessageSeq == 0 {
		return channelappend.SendResult{}, false, nil
	}
	// An uncommitted local proposal can have an exact durable index entry. The
	// routed read checks the current Leader's visible committed frontier.
	reads, err := s.node.ReadChannelCommittedBatch(ctx, []clusterchannels.CommittedRead{{
		ChannelID: channelruntime.ChannelID{ID: query.ChannelID, Type: query.ChannelType},
		Request: channelstore.ReadCommittedRequest{
			FromSeq:  hit.Message.MessageSeq,
			MinSeq:   hit.Message.MessageSeq,
			MaxSeq:   hit.Message.MessageSeq,
			Limit:    1,
			MaxBytes: max(1, len(hit.Message.Payload)),
		},
	}})
	if err != nil {
		return channelappend.SendResult{}, false, mapAppendError(err)
	}
	if len(reads) != 1 {
		return channelappend.SendResult{}, false, channelappend.ErrAppendFailed
	}
	if reads[0].Err != nil {
		return channelappend.SendResult{}, false, mapAppendError(reads[0].Err)
	}
	if len(reads[0].Read.Messages) != 1 {
		return channelappend.SendResult{}, false, nil
	}
	committed := reads[0].Read.Messages[0]
	if committed.MessageID != hit.Message.MessageID || committed.MessageSeq != hit.Message.MessageSeq ||
		committed.FromUID != query.FromUID || committed.ClientMsgNo != query.ClientMsgNo ||
		!bytes.Equal(committed.Payload, hit.Message.Payload) {
		return channelappend.SendResult{}, false, nil
	}
	var applicationID string
	if query.ApplicationAdmission {
		if committed.ServerTimestampMS <= 0 {
			return channelappend.SendResult{}, false, errors.New("cluster: committed server timestamp missing")
		}
		if s.applicationMessageIDs == nil {
			return channelappend.SendResult{}, false, errors.New("cluster: application message identity reader unavailable")
		}
		applicationID, err = s.applicationMessageIDs.ReadApplicationMessageID(committed.Payload)
		if err != nil {
			return channelappend.SendResult{}, false, err
		}
		if applicationID == "" {
			return channelappend.SendResult{}, false, errors.New("cluster: committed application message identity missing")
		}
	}
	return channelappend.SendResult{
		MessageID:            committed.MessageID,
		MessageSeq:           committed.MessageSeq,
		ApplicationMessageID: applicationID,
		ServerTimestampMS:    committed.ServerTimestampMS,
		Reason:               channelappend.ReasonSuccess,
	}, true, nil
}

func channelIdempotencyLookupMissError(err error) bool {
	return errors.Is(err, cluster.ErrNotStarted) ||
		errors.Is(err, cluster.ErrRouteNotReady) ||
		errors.Is(err, cluster.ErrNoSlotLeader) ||
		appendErrorMatches(err, channelruntime.ErrNotReady) ||
		appendErrorMatches(err, channelruntime.ErrChannelNotFound) ||
		appendErrorMatches(err, channelruntime.ErrInvalidConfig)
}
