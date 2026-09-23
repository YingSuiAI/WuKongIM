package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

const (
	messagePageScanChunk   = 1024
	messagePageScanWaves   = 64
	messagePageScanTimeout = 5 * time.Second
)

var (
	errMessagePageScanBudget  = errors.New("message page scan budget exhausted")
	errMessagePageScanInvalid = errors.New("invalid committed message page")
)

// ChannelMessageReadNode is the cluster committed message read surface used by internal.
type ChannelMessageReadNode interface {
	ReadChannelCommitted(context.Context, channelruntime.ChannelID, channelstore.ReadCommittedRequest) (channelstore.ReadCommittedResult, error)
}

type channelMessageBatchReadNode interface {
	ReadChannelCommittedBatch(context.Context, []clusterchannels.CommittedRead) ([]clusterchannels.CommittedReadResult, error)
}

// MessageMembershipNode exposes UID-owned pull authorization state.
type MessageMembershipNode interface {
	GetUserChannelMembership(context.Context, string, string, int64) (metadb.UserChannelMembership, bool, error)
}

// MessageMembershipStore adapts cluster membership reads to message sync.
type MessageMembershipStore struct{ node MessageMembershipNode }

func NewMessageMembershipStore(node MessageMembershipNode) *MessageMembershipStore {
	return &MessageMembershipStore{node: node}
}

func (s *MessageMembershipStore) GetUserChannelMembership(ctx context.Context, uid, channelID string, channelType int64) (metadb.UserChannelMembership, bool, error) {
	if s == nil || s.node == nil {
		return metadb.UserChannelMembership{}, false, message.ErrSyncMembershipRequired
	}
	return s.node.GetUserChannelMembership(ctx, uid, channelID, channelType)
}

// ChannelMessageReader adapts cluster committed reads to the message usecase sync port.
type ChannelMessageReader struct {
	node ChannelMessageReadNode
}

// NewChannelMessageReader creates a ChannelMessageReader.
func NewChannelMessageReader(node ChannelMessageReadNode) *ChannelMessageReader {
	return &ChannelMessageReader{node: node}
}

// SyncMessages returns one compatible channel message page.
func (r *ChannelMessageReader) SyncMessages(ctx context.Context, query message.ChannelMessageQuery) (message.ChannelMessagePage, error) {
	results, err := r.SyncMessagesBatch(ctx, []message.ChannelMessageQuery{query})
	if err != nil {
		return message.ChannelMessagePage{}, err
	}
	if results[0].Err != nil {
		return message.ChannelMessagePage{}, results[0].Err
	}
	return results[0].Page, nil
}

// SyncMessagesBatch runs bounded, aligned cluster read waves and preserves
// one item-scoped result for every query.
func (r *ChannelMessageReader) SyncMessagesBatch(ctx context.Context, queries []message.ChannelMessageQuery) ([]message.ChannelMessageReadResult, error) {
	if r == nil || r.node == nil {
		return nil, message.ErrMessageReaderRequired
	}
	batchNode, ok := r.node.(channelMessageBatchReadNode)
	if !ok {
		return nil, message.ErrSyncBatchReaderRequired
	}
	ctx, cancel := context.WithTimeout(ctx, messagePageScanTimeout)
	defer cancel()
	states := make([]messagePageScan, len(queries))
	pending := make([]int, len(queries))
	for index, query := range queries {
		limit := query.Limit
		if limit <= 0 {
			limit = 1
		}
		if limit == maxInt() {
			limit-- // keep the initial limit+1 read from overflowing
		}
		states[index] = messagePageScan{query: query, limit: limit, read: clusterchannels.CommittedRead{
			ChannelID: channelruntime.ChannelID{ID: query.ChannelID.ID, Type: query.ChannelID.Type},
			Request:   readCommittedRequest(query, limit),
		}}
		if states[index].read.Request.Limit > messagePageScanChunk {
			states[index].read.Request.Limit = messagePageScanChunk
		}
		pending[index] = index
	}
	results := make([]message.ChannelMessageReadResult, len(queries))
	for wave := 0; len(pending) > 0 && wave < messagePageScanWaves; wave++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reads := make([]clusterchannels.CommittedRead, len(pending))
		for i, index := range pending {
			reads[i] = states[index].read
		}
		readResults, err := batchNode.ReadChannelCommittedBatch(ctx, reads)
		if err != nil {
			return nil, mapAppendError(err)
		}
		if len(readResults) != len(reads) {
			return nil, message.ErrSyncBatchResultMismatch
		}
		remaining := pending[:0]
		for i, index := range pending {
			if readResults[i].Err != nil {
				results[index].Err = mapAppendError(readResults[i].Err)
				continue
			}
			state := &states[index]
			done, err := state.consume(readResults[i].Read.Messages)
			if err != nil {
				results[index].Err = err
				continue
			}
			if !done {
				remaining = append(remaining, index)
				continue
			}
			current, err := currentMessagePayloads(ctx, r.node, state.kept)
			if err != nil {
				results[index].Err = err
				continue
			}
			results[index].Page = channelMessagePageFromRead(state.query, state.limit, channelstore.ReadCommittedResult{Messages: current})
		}
		pending = remaining
	}
	for _, index := range pending {
		results[index].Err = errMessagePageScanBudget
	}
	return results, nil
}

// messagePageScan keeps a bounded visible page while raw control rows advance the cursor.
type messagePageScan struct {
	query message.ChannelMessageQuery
	limit int
	read  clusterchannels.CommittedRead
	kept  []channelruntime.Message
}

func (s *messagePageScan) consume(rows []channelruntime.Message) (bool, error) {
	req := s.read.Request
	if len(rows) > req.Limit {
		return false, errMessagePageScanInvalid
	}
	if len(rows) == 0 {
		return true, nil
	}
	var previous uint64
	for i, row := range rows {
		seq := row.MessageSeq
		if seq == 0 || seq < req.MinSeq || (req.MaxSeq > 0 && seq > req.MaxSeq) ||
			(req.Reverse && (seq > req.FromSeq || (i > 0 && seq >= previous))) ||
			(!req.Reverse && (seq < req.FromSeq || (i > 0 && seq <= previous))) {
			return false, errMessagePageScanInvalid
		}
		previous = seq
		if (s.query.PullMode == message.PullModeDown && s.query.EndSeq > 0 && seq <= s.query.EndSeq) ||
			(s.query.PullMode == message.PullModeUp && s.query.EndSeq > 0 && seq >= s.query.EndSeq) {
			return true, nil
		}
		if !row.SyncOnce {
			s.kept = append(s.kept, row)
		}
		if len(s.kept) > s.limit {
			return true, nil
		}
	}
	if len(rows) < req.Limit {
		return true, nil
	}
	last := rows[len(rows)-1].MessageSeq
	if req.Reverse {
		if last <= 1 || last <= req.MinSeq || (s.query.EndSeq > 0 && last-1 <= s.query.EndSeq) {
			return true, nil
		}
		s.read.Request.FromSeq = last - 1
		s.read.Request.MaxSeq = last - 1
	} else {
		if last == maxUint64() || (req.MaxSeq > 0 && last >= req.MaxSeq) {
			return true, nil
		}
		s.read.Request.FromSeq = last + 1
	}
	s.read.Request.Limit = messagePageScanChunk
	return false, nil
}

func channelMessagePageFromRead(query message.ChannelMessageQuery, limit int, read channelstore.ReadCommittedResult) message.ChannelMessagePage {
	messages := syncedMessagesFromChannel(read.Messages)
	messages = filterSyncedMessages(query, messages)
	reverse := query.PullMode == message.PullModeDown || (query.StartSeq == 0 && query.EndSeq == 0)
	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	if reverse {
		reverseSyncedMessages(messages)
	}
	return message.ChannelMessagePage{Messages: messages, HasMore: hasMore}
}

func readCommittedRequest(query message.ChannelMessageQuery, limit int) channelstore.ReadCommittedRequest {
	req := channelstore.ReadCommittedRequest{
		FromSeq:  query.StartSeq,
		MaxSeq:   queryMaxSeq(query),
		MinSeq:   query.MinSeq,
		Limit:    limit + 1,
		MaxBytes: maxInt(),
	}
	if query.PullMode == message.PullModeDown || (query.StartSeq == 0 && query.EndSeq == 0) {
		req.Reverse = true
		if req.FromSeq == 0 {
			req.FromSeq = maxUint64()
			req.MaxSeq = maxUint64()
		}
	}
	if req.FromSeq == 0 && !req.Reverse {
		req.FromSeq = 1
	}
	return req
}

func queryMaxSeq(query message.ChannelMessageQuery) uint64 {
	if query.PullMode == message.PullModeUp && query.EndSeq > 0 {
		return query.EndSeq - 1
	}
	if query.PullMode == message.PullModeDown && query.StartSeq > 0 {
		return query.StartSeq
	}
	return maxUint64()
}

func syncedMessagesFromChannel(in []channelruntime.Message) []message.SyncedMessage {
	out := make([]message.SyncedMessage, 0, len(in))
	for _, msg := range in {
		if msg.SyncOnce {
			continue
		}
		out = append(out, message.SyncedMessage{
			MessageID:         msg.MessageID,
			MessageSeq:        msg.MessageSeq,
			ChannelID:         msg.ChannelID,
			ChannelType:       msg.ChannelType,
			Setting:           msg.Setting,
			FromUID:           msg.FromUID,
			ClientMsgNo:       msg.ClientMsgNo,
			Timestamp:         int32(msg.ServerTimestampMS / 1000),
			Payload:           append([]byte(nil), msg.Payload...),
			PayloadCorrection: msg.PayloadCorrection.Clone(),
		})
	}
	return out
}

func filterSyncedMessages(query message.ChannelMessageQuery, messages []message.SyncedMessage) []message.SyncedMessage {
	if query.PullMode == message.PullModeDown && query.EndSeq > 0 {
		kept := messages[:0]
		for _, msg := range messages {
			if msg.MessageSeq <= query.EndSeq {
				continue
			}
			kept = append(kept, msg)
		}
		return kept
	}
	if query.PullMode == message.PullModeUp && query.EndSeq > 0 {
		kept := messages[:0]
		for _, msg := range messages {
			if msg.MessageSeq >= query.EndSeq {
				continue
			}
			kept = append(kept, msg)
		}
		return kept
	}
	return messages
}

func reverseSyncedMessages(messages []message.SyncedMessage) {
	for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
		messages[left], messages[right] = messages[right], messages[left]
	}
}

func maxUint64() uint64 {
	return ^uint64(0)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
