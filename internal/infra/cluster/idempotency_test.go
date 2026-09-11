package cluster

import (
	"context"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/WuKongIM/WuKongIM/internal/contracts/channelappend"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/WuKongIM/WuKongIM/pkg/cluster"
)

type idempotencyApplicationIDReader func([]byte) (string, error)

func (f idempotencyApplicationIDReader) ReadApplicationMessageID(payload []byte) (string, error) {
	return f(payload)
}

func TestChannelIdempotencyReadsApplicationIdentityFromCommittedHit(t *testing.T) {
	canonical := []byte("actual committed canonical payload")
	node := &recordingIdempotencyNode{ok: true, hit: channelstore.IdempotencyHit{
		Message:     channelruntime.Message{MessageID: 42, MessageSeq: 7, Payload: canonical, ServerTimestampMS: 1788364800123},
		PayloadHash: idempotencyTestHash(canonical),
	}}
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
			Message:     channelruntime.Message{MessageID: 42, MessageSeq: 7},
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
}

func (n *recordingIdempotencyNode) LookupChannelIdempotency(_ context.Context, id channelruntime.ChannelID, fromUID string, clientMsgNo string) (channelstore.IdempotencyHit, bool, error) {
	n.id = id
	n.fromUID = fromUID
	n.clientMsgNo = clientMsgNo
	return n.hit, n.ok, n.err
}

func idempotencyTestHash(payload []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(payload)
	return h.Sum64()
}
