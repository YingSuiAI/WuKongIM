//go:build integration

package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	clusterinfra "github.com/WuKongIM/WuKongIM/internal/infra/cluster"
	"github.com/WuKongIM/WuKongIM/internal/runtime/channelappend"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/WuKongIM/WuKongIM/pkg/cluster"
)

type auditSendIDs struct{ next atomic.Uint64 }

func (ids *auditSendIDs) Next() uint64 { return ids.next.Add(1) }

func TestAuditThreeNodeSingleSenderRecoversThroughRealSendConsumer(t *testing.T) {
	leader, id, _ := cluster.AuditAmbiguousBatchFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	meta, err := leader.GetChannelRuntimeMeta(ctx, id.ID, int64(id.Type))
	if err != nil {
		t.Fatal(err)
	}
	ids := &auditSendIDs{}
	ids.next.Store(5000)
	group := channelappend.New(channelappend.Options{
		LocalNodeID: leader.NodeID(), MessageID: ids,
		Appender:    clusterinfra.NewChannelAppender(leader),
		Idempotency: clusterinfra.NewChannelIdempotencyStore(leader, nil),
	})
	if err := group.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		if err := group.Stop(stopCtx); err != nil {
			t.Errorf("send consumer stop: %v", err)
		}
	})
	target := channelappend.AuthorityTarget{
		ChannelID: channelappend.ChannelID{ID: id.ID, Type: id.Type}, LeaderNodeID: leader.NodeID(),
		Epoch: meta.ChannelEpoch, LeaderEpoch: meta.LeaderEpoch, RouteGeneration: meta.RouteGeneration,
	}
	send := func(uid, key string, payload []byte) channelappend.SendResult {
		t.Helper()
		future, err := group.SubmitLocal(ctx, target, []channelappend.SendBatchItem{{Context: ctx, Command: channelappend.SendCommand{
			FromUID: uid, ClientMsgNo: key, ChannelID: id.ID, ChannelType: id.Type, Payload: payload,
		}}})
		if err != nil {
			t.Fatal(err)
		}
		items, err := future.Wait(ctx)
		if err != nil || len(items) != 1 {
			t.Fatalf("send consumer wait: %+v, %v", items, err)
		}
		if items[0].Err != nil || items[0].Result.Reason != channelappend.ReasonSuccess {
			t.Fatalf("send consumer result: %+v", items[0])
		}
		return items[0].Result
	}
	first := send("sender-a", "a", []byte("a"))
	if first.MessageID != 1001 || first.MessageSeq != 2 {
		t.Fatalf("single sender got a new or incorrect ACK: %+v", first)
	}
	second := send("sender-b", "b", []byte("b"))
	if second.MessageID != 1002 || second.MessageSeq != 3 {
		t.Fatalf("batch sibling got a new or incorrect ACK: %+v", second)
	}
	newSend := send("new-sender", "next", []byte("next"))
	if newSend.MessageID != 5003 || newSend.MessageSeq != 4 {
		t.Fatalf("new SEND sequence after recovered batch: %+v", newSend)
	}
	read, err := leader.ReadChannelCommitted(ctx, id, channelstore.ReadCommittedRequest{FromSeq: 1, MaxSeq: 100, Limit: 100, MaxBytes: 1 << 20})
	if err != nil || len(read.Messages) != 4 {
		t.Fatalf("real consumer durable rows: %+v, %v", read.Messages, err)
	}
	wantIDs := []uint64{1000, 1001, 1002, 5003}
	for index, message := range read.Messages {
		if message.MessageID != wantIDs[index] || message.MessageSeq != uint64(index+1) {
			t.Fatalf("unexpected durable row: %+v", message)
		}
	}
}
