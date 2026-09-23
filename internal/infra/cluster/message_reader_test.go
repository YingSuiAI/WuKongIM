package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

func TestChannelMessageReaderMapsPullUpRequestAndTrimsHasMore(t *testing.T) {
	node := &recordingReadNode{
		batchResults: []clusterchannels.CommittedReadResult{{Read: channelstore.ReadCommittedResult{Messages: []channelruntime.Message{
			{MessageID: 10, MessageSeq: 2, ChannelID: "g1", ChannelType: 2, Setting: 2, FromUID: "u1", ClientMsgNo: "c1", Payload: []byte("a")},
			{MessageID: 11, MessageSeq: 3, ChannelID: "g1", ChannelType: 2, FromUID: "u1", ClientMsgNo: "c2", Payload: []byte("b")},
			{MessageID: 12, MessageSeq: 4, ChannelID: "g1", ChannelType: 2, FromUID: "u1", ClientMsgNo: "c3", Payload: []byte("c")},
		}}}},
	}
	reader := NewChannelMessageReader(node)

	page, err := reader.SyncMessages(context.Background(), message.ChannelMessageQuery{
		ChannelID: message.ChannelID{ID: "g1", Type: 2},
		StartSeq:  2,
		EndSeq:    5,
		Limit:     2,
		PullMode:  message.PullModeUp,
	})

	if err != nil {
		t.Fatalf("SyncMessages() error = %v", err)
	}
	if node.batchCalls != 1 || len(node.batchReads) != 1 || node.batchReads[0].ChannelID != (channelruntime.ChannelID{ID: "g1", Type: 2}) {
		t.Fatalf("batch calls=%d reads=%#v, want one g1/2 read", node.batchCalls, node.batchReads)
	}
	readReq := node.batchReads[0].Request
	if readReq.FromSeq != 2 || readReq.MaxSeq != 4 || readReq.Limit != 3 || readReq.Reverse {
		t.Fatalf("read request = %#v, want forward 2..4 limit+1", readReq)
	}
	if !page.HasMore || len(page.Messages) != 2 {
		t.Fatalf("page = %#v, want two messages with hasMore", page)
	}
	if page.Messages[0].MessageID != 10 || page.Messages[1].MessageID != 11 || string(page.Messages[0].Payload) != "a" {
		t.Fatalf("messages = %#v, want mapped first two messages", page.Messages)
	}
	if page.Messages[0].Setting != 2 {
		t.Fatalf("message setting = %d, want 2", page.Messages[0].Setting)
	}
}

func TestChannelMessageReaderMapsUnboundedPullUpRange(t *testing.T) {
	node := &recordingReadNode{batchResults: []clusterchannels.CommittedReadResult{{}}}
	reader := NewChannelMessageReader(node)

	_, err := reader.SyncMessages(context.Background(), message.ChannelMessageQuery{
		ChannelID: message.ChannelID{ID: "g1", Type: 2},
		StartSeq:  2,
		Limit:     10,
		PullMode:  message.PullModeUp,
	})
	if err != nil {
		t.Fatalf("SyncMessages() error = %v", err)
	}
	request := node.batchReads[0].Request
	if request.FromSeq != 2 || request.MaxSeq != maxUint64() || request.Reverse {
		t.Fatalf("read request = %#v, want forward [2,+inf)", request)
	}
}

func TestChannelMessageReaderMapsPullDownAndReturnsAscending(t *testing.T) {
	node := &recordingReadNode{
		batchResults: []clusterchannels.CommittedReadResult{{Read: channelstore.ReadCommittedResult{Messages: []channelruntime.Message{
			{MessageID: 15, MessageSeq: 5, ChannelID: "g1", ChannelType: 2},
			{MessageID: 14, MessageSeq: 4, ChannelID: "g1", ChannelType: 2},
			{MessageID: 13, MessageSeq: 3, ChannelID: "g1", ChannelType: 2},
		}}}},
	}
	reader := NewChannelMessageReader(node)

	page, err := reader.SyncMessages(context.Background(), message.ChannelMessageQuery{
		ChannelID: message.ChannelID{ID: "g1", Type: 2},
		StartSeq:  5,
		EndSeq:    2,
		Limit:     2,
		PullMode:  message.PullModeDown,
	})

	if err != nil {
		t.Fatalf("SyncMessages() error = %v", err)
	}
	readReq := node.batchReads[0].Request
	if readReq.FromSeq != 5 || readReq.Limit != 3 || !readReq.Reverse {
		t.Fatalf("read request = %#v, want reverse from 5 limit+1", readReq)
	}
	if !page.HasMore || len(page.Messages) != 2 {
		t.Fatalf("page = %#v, want two messages with hasMore", page)
	}
	if page.Messages[0].MessageSeq != 4 || page.Messages[1].MessageSeq != 5 {
		t.Fatalf("messages = %#v, want ascending seq 4,5", page.Messages)
	}
}

func TestChannelMessageReaderPreservesMessageTimestamp(t *testing.T) {
	node := &recordingReadNode{
		batchResults: []clusterchannels.CommittedReadResult{{
			Read: channelstore.ReadCommittedResult{Messages: []channelruntime.Message{{
				MessageID: 1, MessageSeq: 1, ChannelID: "g1", ChannelType: 2,
				ServerTimestampMS: 1_700_000_000_123,
			}}},
		}},
	}
	reader := NewChannelMessageReader(node)

	page, err := reader.SyncMessages(context.Background(), message.ChannelMessageQuery{
		ChannelID: message.ChannelID{ID: "g1", Type: 2}, Limit: 1, PullMode: message.PullModeDown,
	})
	if err != nil {
		t.Fatalf("SyncMessages(): %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].Timestamp != 1_700_000_000 {
		t.Fatalf("messages = %#v, want durable server timestamp in seconds", page.Messages)
	}
}

func TestChannelMessageReaderSingleUsesRoutedOneItemBatch(t *testing.T) {
	node := &recordingReadNode{batchResults: []clusterchannels.CommittedReadResult{{
		Read: channelstore.ReadCommittedResult{Messages: []channelruntime.Message{{
			MessageID: 10, MessageSeq: 1, ChannelID: "g1", ChannelType: 2, ClientMsgNo: "routed",
		}},
		}}}}
	reader := NewChannelMessageReader(node)

	page, err := reader.SyncMessages(context.Background(), message.ChannelMessageQuery{
		ChannelID: message.ChannelID{ID: "g1", Type: 2},
		StartSeq:  1,
		Limit:     10,
		PullMode:  message.PullModeDown,
	})
	if err != nil {
		t.Fatalf("SyncMessages() error = %v", err)
	}
	if node.batchCalls != 1 || len(node.batchReads) != 1 {
		t.Fatalf("batch calls=%d reads=%+v, want one routed item", node.batchCalls, node.batchReads)
	}
	if node.lastID != (channelruntime.ChannelID{}) {
		t.Fatalf("local ReadChannelCommitted called with %v", node.lastID)
	}
	if len(page.Messages) != 1 || page.Messages[0].ClientMsgNo != "routed" {
		t.Fatalf("page=%+v, want routed message", page)
	}
}

func TestChannelMessageReaderPreservesMissingChannelRuntimeError(t *testing.T) {
	reader := NewChannelMessageReader(&recordingReadNode{batchResults: []clusterchannels.CommittedReadResult{{
		Err: channelruntime.ErrChannelNotFound,
	}}})

	_, err := reader.SyncMessages(context.Background(), message.ChannelMessageQuery{
		ChannelID: message.ChannelID{ID: "new-empty-channel", Type: 2},
		Limit:     10,
		PullMode:  message.PullModeDown,
	})

	if !errors.Is(err, message.ErrChannelNotFound) {
		t.Fatalf("SyncMessages() error = %v, want %v", err, message.ErrChannelNotFound)
	}
}

func TestChannelMessageReaderBatchUsesOneAlignedClusterRead(t *testing.T) {
	node := &recordingReadNode{batchResults: []clusterchannels.CommittedReadResult{
		{Read: channelstore.ReadCommittedResult{Messages: []channelruntime.Message{{MessageSeq: 2}, {MessageSeq: 3}}}},
		{Err: channelruntime.ErrNotReady},
	}}
	reader := NewChannelMessageReader(node)

	results, err := reader.SyncMessagesBatch(context.Background(), []message.ChannelMessageQuery{
		{ChannelID: message.ChannelID{ID: "g1", Type: 2}, StartSeq: 1, Limit: 1, PullMode: message.PullModeUp},
		{ChannelID: message.ChannelID{ID: "g2", Type: 2}, StartSeq: 4, Limit: 2, PullMode: message.PullModeUp},
	})
	if err != nil {
		t.Fatalf("SyncMessagesBatch() error=%v", err)
	}
	if node.batchCalls != 1 || len(node.batchReads) != 2 {
		t.Fatalf("batch calls=%d reads=%+v", node.batchCalls, node.batchReads)
	}
	if !results[0].Page.HasMore || len(results[0].Page.Messages) != 1 {
		t.Fatalf("first result=%+v, want trimmed page", results[0])
	}
	if results[1].Err == nil {
		t.Fatalf("second result=%+v, want item error", results[1])
	}
}

func TestChannelMessageReaderBatchPreservesMissingChannelRuntimeError(t *testing.T) {
	reader := NewChannelMessageReader(&recordingReadNode{batchResults: []clusterchannels.CommittedReadResult{{
		Err: channelruntime.ErrChannelNotFound,
	}}})

	results, err := reader.SyncMessagesBatch(context.Background(), []message.ChannelMessageQuery{{
		ChannelID: message.ChannelID{ID: "new-empty-channel", Type: 2},
		Limit:     10,
		PullMode:  message.PullModeDown,
	}})

	if err != nil {
		t.Fatalf("SyncMessagesBatch() error = %v", err)
	}
	if len(results) != 1 || !errors.Is(results[0].Err, message.ErrChannelNotFound) {
		t.Fatalf("results = %#v, want one channel-not-found error", results)
	}
}

func TestChannelMessageReaderContinuesPastHiddenRawPages(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query message.ChannelMessageQuery
		want  uint64
	}{
		{name: "forward", query: message.ChannelMessageQuery{StartSeq: 1, Limit: 1, PullMode: message.PullModeUp}, want: 4},
		{name: "latest", query: message.ChannelMessageQuery{Limit: 1, PullMode: message.PullModeDown}, want: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []channelruntime.Message{
				{MessageSeq: 1, SyncOnce: tc.name == "forward"},
				{MessageSeq: 2, SyncOnce: tc.name == "forward"},
				{MessageSeq: 3, SyncOnce: tc.name == "forward"},
				{MessageSeq: 4},
				{MessageSeq: 5, SyncOnce: tc.name == "latest"},
				{MessageSeq: 6, SyncOnce: tc.name == "latest"},
				{MessageSeq: 7, SyncOnce: tc.name == "latest"},
			}
			node := &historyReadNode{rows: rows, correctPayloads: true}
			reader := NewChannelMessageReader(node)
			page, err := reader.SyncMessages(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("SyncMessages() error = %v", err)
			}
			if len(page.Messages) != 1 || page.Messages[0].MessageSeq != tc.want || !page.HasMore || string(page.Messages[0].Payload) != "corrected" {
				t.Fatalf("page = %+v, want visible seq %d and more", page, tc.want)
			}
			if node.calls < 2 {
				t.Fatalf("read calls = %d, want continuation past hidden raw page", node.calls)
			}
		})
	}
}

func TestChannelMessageReaderBatchContinuesOnlyPendingItems(t *testing.T) {
	node := &historyReadNode{rows: []channelruntime.Message{
		{MessageSeq: 1, SyncOnce: true}, {MessageSeq: 2, SyncOnce: true},
		{MessageSeq: 3, SyncOnce: true}, {MessageSeq: 4}, {MessageSeq: 5}, {MessageSeq: 6},
	}}
	results, err := NewChannelMessageReader(node).SyncMessagesBatch(context.Background(), []message.ChannelMessageQuery{
		{ChannelID: message.ChannelID{ID: "controls", Type: 2}, StartSeq: 1, Limit: 1, PullMode: message.PullModeUp},
		{ChannelID: message.ChannelID{ID: "ordinary", Type: 2}, StartSeq: 4, Limit: 1, PullMode: message.PullModeUp},
	})
	if err != nil || len(results) != 2 || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("SyncMessagesBatch() results = %+v, err = %v", results, err)
	}
	for i, result := range results {
		if len(result.Page.Messages) != 1 || result.Page.Messages[0].MessageSeq != 4 || !result.Page.HasMore {
			t.Fatalf("result %d = %+v, want seq 4 with more", i, result)
		}
	}
	if node.calls < 2 || node.readCounts[0] != 2 || node.readCounts[1] != 1 {
		t.Fatalf("read waves = %v, want first two items and then pending item only", node.readCounts)
	}
}

func TestChannelMessageReaderHiddenRowsRespectExclusiveAndVisibilityBounds(t *testing.T) {
	node := &historyReadNode{rows: []channelruntime.Message{
		{MessageSeq: 1}, {MessageSeq: 2}, {MessageSeq: 3},
		{MessageSeq: 4, SyncOnce: true}, {MessageSeq: 5, SyncOnce: true}, {MessageSeq: 6, SyncOnce: true},
		{MessageSeq: 7},
	}}
	for _, query := range []message.ChannelMessageQuery{
		{StartSeq: 4, EndSeq: 7, MinSeq: 4, Limit: 1, PullMode: message.PullModeUp},
		{StartSeq: 6, EndSeq: 3, MinSeq: 4, Limit: 1, PullMode: message.PullModeDown},
	} {
		page, err := NewChannelMessageReader(node).SyncMessages(context.Background(), query)
		if err != nil || len(page.Messages) != 0 || page.HasMore {
			t.Fatalf("query = %+v, page = %+v, err = %v; want empty bounded page", query, page, err)
		}
	}
}

func TestChannelMessageReaderScanBudgetFailsInsteadOfReturningFalseEnd(t *testing.T) {
	node := &historyReadNode{endlessControls: true}
	page, err := NewChannelMessageReader(node).SyncMessages(context.Background(), message.ChannelMessageQuery{StartSeq: 1, Limit: 1, PullMode: message.PullModeUp})
	if !errors.Is(err, errMessagePageScanBudget) || len(page.Messages) != 0 || node.calls != messagePageScanWaves {
		t.Fatalf("page = %+v, err = %v, calls = %d; want bounded error after %d waves", page, err, node.calls, messagePageScanWaves)
	}
}

// historyReadNode models committed storage where hidden rows count toward a raw read limit.
type historyReadNode struct {
	rows            []channelruntime.Message
	calls           int
	readCounts      []int
	endlessControls bool
	correctPayloads bool
}

func (n *historyReadNode) ApplyMessagePayloadCorrections(_ context.Context, messages []channelruntime.Message) ([]channelruntime.Message, error) {
	if n.correctPayloads {
		for i := range messages {
			messages[i].Payload = []byte("corrected")
		}
	}
	return messages, nil
}

func (n *historyReadNode) ReadChannelCommitted(context.Context, channelruntime.ChannelID, channelstore.ReadCommittedRequest) (channelstore.ReadCommittedResult, error) {
	return channelstore.ReadCommittedResult{}, nil
}

func (n *historyReadNode) ReadChannelCommittedBatch(_ context.Context, reads []clusterchannels.CommittedRead) ([]clusterchannels.CommittedReadResult, error) {
	n.calls++
	n.readCounts = append(n.readCounts, len(reads))
	results := make([]clusterchannels.CommittedReadResult, len(reads))
	for i, read := range reads {
		req := read.Request
		if n.endlessControls {
			results[i].Read.Messages = make([]channelruntime.Message, req.Limit)
			for j := range results[i].Read.Messages {
				results[i].Read.Messages[j] = channelruntime.Message{MessageSeq: req.FromSeq + uint64(j), SyncOnce: true}
			}
			continue
		}
		appendRow := func(row channelruntime.Message) {
			if len(results[i].Read.Messages) >= req.Limit || row.MessageSeq < req.MinSeq || (req.MaxSeq > 0 && row.MessageSeq > req.MaxSeq) || (req.Reverse && row.MessageSeq > req.FromSeq) || (!req.Reverse && row.MessageSeq < req.FromSeq) {
				return
			}
			results[i].Read.Messages = append(results[i].Read.Messages, row)
		}
		if req.Reverse {
			for j := len(n.rows) - 1; j >= 0; j-- {
				appendRow(n.rows[j])
			}
		} else {
			for _, row := range n.rows {
				appendRow(row)
			}
		}
	}
	return results, nil
}

type recordingReadNode struct {
	lastID       channelruntime.ChannelID
	lastReq      channelstore.ReadCommittedRequest
	result       channelstore.ReadCommittedResult
	err          error
	batchCalls   int
	batchReads   []clusterchannels.CommittedRead
	batchResults []clusterchannels.CommittedReadResult
}

func (n *recordingReadNode) ReadChannelCommittedBatch(_ context.Context, reads []clusterchannels.CommittedRead) ([]clusterchannels.CommittedReadResult, error) {
	n.batchCalls++
	n.batchReads = append([]clusterchannels.CommittedRead(nil), reads...)
	return n.batchResults, n.err
}

func (n *recordingReadNode) ReadChannelCommitted(_ context.Context, id channelruntime.ChannelID, req channelstore.ReadCommittedRequest) (channelstore.ReadCommittedResult, error) {
	n.lastID = id
	n.lastReq = req
	return n.result, n.err
}
