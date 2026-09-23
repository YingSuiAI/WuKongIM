package cluster

import (
	"context"
	"errors"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/WuKongIM/WuKongIM/internal/contracts/channelappend"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/WuKongIM/WuKongIM/pkg/cluster"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

type idempotencyApplicationIDReader func([]byte) (string, error)

func (f idempotencyApplicationIDReader) ReadApplicationMessageID(payload []byte) (string, error) {
	return f(payload)
}

func TestChannelIdempotencyReadsApplicationIdentityFromVerifiedCommittedRow(t *testing.T) {
	canonical := []byte("actual committed canonical payload")
	node := &recordingIdempotencyNode{ok: true, hit: channelstore.IdempotencyHit{
		Message:     channelruntime.Message{MessageID: 42, MessageSeq: 7, Payload: canonical},
		PayloadHash: idempotencyTestHash(canonical),
	}, committed: []channelruntime.Message{{MessageID: 42, MessageSeq: 7, FromUID: "sender", ClientMsgNo: "client-message-0001", Payload: canonical, ServerTimestampMS: 1788364800123}}}
	reader := idempotencyApplicationIDReader(func(payload []byte) (string, error) {
		require.Equal(t, canonical, payload)
		return "original-committed-application-id", nil
	})
	query := channelappend.IdempotencyQuery{FromUID: "sender", ChannelID: "room", ChannelType: 2,
		ClientMsgNo: "client-message-0001", PayloadHash: idempotencyTestHash(canonical), ApplicationAdmission: true}
	result, found, err := NewChannelIdempotencyStore(node, reader).LookupSend(context.Background(), query)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "original-committed-application-id", result.ApplicationMessageID)
	require.Equal(t, int64(1788364800123), result.ServerTimestampMS)
	_, _, err = NewChannelIdempotencyStore(node, nil).LookupSend(context.Background(), query)
	require.Error(t, err)
}

func TestChannelIdempotencyStoreLookupSendMapsCommittedHit(t *testing.T) {
	node := &recordingIdempotencyNode{
		hit: channelstore.IdempotencyHit{
			Message:     channelruntime.Message{MessageID: 42, MessageSeq: 7, Payload: []byte("payload")},
			PayloadHash: idempotencyTestHash([]byte("payload")),
		},
		ok: true,
	}
	store := NewChannelIdempotencyStore(node, nil)

	result, ok, err := store.LookupSend(context.Background(), channelappend.IdempotencyQuery{
		FromUID:     "u1",
		ClientMsgNo: "client-1",
		ChannelID:   "room",
		ChannelType: 2,
		PayloadHash: idempotencyTestHash([]byte("payload")),
	})
	if err != nil {
		t.Fatalf("LookupSend() error = %v", err)
	}
	if !ok {
		t.Fatal("LookupSend() ok = false, want true")
	}
	if result.MessageID != 42 || result.MessageSeq != 7 || result.Reason != channelappend.ReasonSuccess {
		t.Fatalf("LookupSend() result = %#v, want committed success", result)
	}
	if node.id != (channelruntime.ChannelID{ID: "room", Type: 2}) || node.fromUID != "u1" || node.clientMsgNo != "client-1" {
		t.Fatalf("lookup request = id:%#v from:%q client:%q", node.id, node.fromUID, node.clientMsgNo)
	}
	if len(node.reads) != 1 || node.reads[0].ChannelID != node.id || node.reads[0].Request != (channelstore.ReadCommittedRequest{FromSeq: 7, MinSeq: 7, MaxSeq: 7, Limit: 1, MaxBytes: len("payload")}) {
		t.Fatalf("committed proof request = %+v, want bounded point read", node.reads)
	}
}

func TestChannelIdempotencyStoreDoesNotAckUncommittedIndexHit(t *testing.T) {
	node := &recordingIdempotencyNode{
		hit:       channelstore.IdempotencyHit{Message: channelruntime.Message{MessageID: 42, MessageSeq: 7, Payload: []byte("payload")}},
		ok:        true,
		committed: []channelruntime.Message{},
	}
	result, ok, err := NewChannelIdempotencyStore(node, nil).LookupSend(context.Background(), channelappend.IdempotencyQuery{
		FromUID: "u1", ClientMsgNo: "client-1", ChannelID: "room", ChannelType: 2,
	})
	if err != nil || ok || result.MessageID != 0 {
		t.Fatalf("uncommitted index hit became success: result=%+v ok=%v err=%v", result, ok, err)
	}
}

func TestChannelIdempotencyStoreRequiresMatchingCommittedRow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*channelruntime.Message)
	}{
		{"message_id", func(m *channelruntime.Message) { m.MessageID++ }},
		{"sequence", func(m *channelruntime.Message) { m.MessageSeq++ }},
		{"sender", func(m *channelruntime.Message) { m.FromUID = "other" }},
		{"client_key", func(m *channelruntime.Message) { m.ClientMsgNo = "other" }},
		{"payload", func(m *channelruntime.Message) { m.Payload = []byte("other") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := channelruntime.Message{MessageID: 42, MessageSeq: 7, FromUID: "u1", ClientMsgNo: "client-1", Payload: []byte("payload")}
			changed := original
			tc.change(&changed)
			node := &recordingIdempotencyNode{hit: channelstore.IdempotencyHit{Message: original}, ok: true, committed: []channelruntime.Message{changed}}
			result, ok, err := NewChannelIdempotencyStore(node, nil).LookupSend(context.Background(), channelappend.IdempotencyQuery{
				FromUID: "u1", ClientMsgNo: "client-1", ChannelID: "room", ChannelType: 2,
			})
			if err != nil || ok || result.MessageID != 0 {
				t.Fatalf("mismatched row became success: result=%+v ok=%v err=%v", result, ok, err)
			}
		})
	}
}

func TestChannelIdempotencyStoreDoesNotAckFailedCommittedRead(t *testing.T) {
	proofErr := errors.New("current leader unavailable")
	for _, itemError := range []bool{false, true} {
		node := &recordingIdempotencyNode{hit: channelstore.IdempotencyHit{Message: channelruntime.Message{MessageID: 42, MessageSeq: 7}}, ok: true}
		if itemError {
			node.itemErr = proofErr
		} else {
			node.readErr = proofErr
		}
		result, ok, err := NewChannelIdempotencyStore(node, nil).LookupSend(context.Background(), channelappend.IdempotencyQuery{
			FromUID: "u1", ClientMsgNo: "client-1", ChannelID: "room", ChannelType: 2,
		})
		if !errors.Is(err, proofErr) || ok || result.MessageID != 0 {
			t.Fatalf("failed proof became success: result=%+v ok=%v err=%v", result, ok, err)
		}
	}
}

func TestChannelIdempotencyStoreLookupSendRejectsPayloadHashMismatch(t *testing.T) {
	node := &recordingIdempotencyNode{
		hit: channelstore.IdempotencyHit{
			Message:     channelruntime.Message{MessageID: 42, MessageSeq: 7},
			PayloadHash: idempotencyTestHash([]byte("old")),
		},
		ok: true,
	}
	store := NewChannelIdempotencyStore(node, nil)

	_, ok, err := store.LookupSend(context.Background(), channelappend.IdempotencyQuery{
		FromUID:     "u1",
		ClientMsgNo: "client-1",
		ChannelID:   "room",
		ChannelType: 2,
		PayloadHash: idempotencyTestHash([]byte("new")),
	})
	if err != nil {
		t.Fatalf("LookupSend() error = %v", err)
	}
	if ok {
		t.Fatal("LookupSend() ok = true, want false for payload hash mismatch")
	}
}

func TestChannelIdempotencyStoreLookupSendTreatsReadinessErrorsAsMiss(t *testing.T) {
	store := NewChannelIdempotencyStore(&recordingIdempotencyNode{err: cluster.ErrNotStarted}, nil)

	_, ok, err := store.LookupSend(context.Background(), channelappend.IdempotencyQuery{
		FromUID:     "u1",
		ClientMsgNo: "client-1",
		ChannelID:   "room",
		ChannelType: 2,
		PayloadHash: idempotencyTestHash([]byte("payload")),
	})
	if err != nil {
		t.Fatalf("LookupSend() error = %v, want nil readiness miss", err)
	}
	if ok {
		t.Fatal("LookupSend() ok = true, want false readiness miss")
	}
}

type recordingIdempotencyNode struct {
	id          channelruntime.ChannelID
	fromUID     string
	clientMsgNo string
	hit         channelstore.IdempotencyHit
	ok          bool
	err         error
	reads       []clusterchannels.CommittedRead
	committed   []channelruntime.Message
	readErr     error
	itemErr     error
}

func (n *recordingIdempotencyNode) LookupChannelIdempotency(_ context.Context, id channelruntime.ChannelID, fromUID string, clientMsgNo string) (channelstore.IdempotencyHit, bool, error) {
	n.id = id
	n.fromUID = fromUID
	n.clientMsgNo = clientMsgNo
	return n.hit, n.ok, n.err
}

func (n *recordingIdempotencyNode) ReadChannelCommittedBatch(_ context.Context, reads []clusterchannels.CommittedRead) ([]clusterchannels.CommittedReadResult, error) {
	n.reads = reads
	if n.readErr != nil {
		return nil, n.readErr
	}
	if n.itemErr != nil {
		return []clusterchannels.CommittedReadResult{{Err: n.itemErr}}, nil
	}
	if n.committed != nil {
		return []clusterchannels.CommittedReadResult{{Read: channelstore.ReadCommittedResult{Messages: n.committed}}}, nil
	}
	msg := n.hit.Message
	msg.FromUID = n.fromUID
	msg.ClientMsgNo = n.clientMsgNo
	return []clusterchannels.CommittedReadResult{{Read: channelstore.ReadCommittedResult{Messages: []channelruntime.Message{msg}}}}, nil
}

func idempotencyTestHash(payload []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(payload)
	return h.Sum64()
}
