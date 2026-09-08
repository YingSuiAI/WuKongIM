//go:build integration

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replication"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

// auditLostQuorumResponse runs the real follower commit and only discards its
// response, reproducing an ambiguous network outcome without fake storage.
type auditLostQuorumResponse struct {
	server  channels.QuorumExchangeServer
	drop    atomic.Bool
	dropped atomic.Int32
}

func (s *auditLostQuorumResponse) Handle(ctx context.Context, from ch.NodeID, batch replication.ExchangeBatch) (replication.ExchangeBatchResult, error) {
	result, err := s.server.Handle(ctx, from, batch)
	if err == nil && s.drop.Load() {
		for _, item := range batch.Items {
			if item.Kind == replication.ExchangeReplicate {
				s.dropped.Add(1)
				return replication.ExchangeBatchResult{}, errors.New("audit: follower durable response lost")
			}
		}
	}
	return result, err
}

// AuditAmbiguousBatchFixture returns a real cluster leader after an internal
// two-sender batch has lost its durable responses and the network is restored.
// It is exported only by the test binary for the external actual-consumer test.
func AuditAmbiguousBatchFixture(t *testing.T) (*Node, ch.ChannelID, ch.AppendBatchRequest) {
	t.Helper()
	addrs := []string{freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)}
	voters := []ControlVoter{{NodeID: 1, Addr: addrs[0]}, {NodeID: 2, Addr: addrs[1]}, {NodeID: 3, Addr: addrs[2]}}
	nodes := make([]*Node, 0, 3)
	for _, voter := range voters {
		cfg := Config{NodeID: voter.NodeID, ListenAddr: voter.Addr, DataDir: t.TempDir()}
		cfg.Control.ClusterID = "audit-ambiguous-batch"
		cfg.Control.Voters = voters
		cfg.Control.AllowBootstrap = true
		cfg.Slots.InitialSlotCount = 10
		cfg.Slots.HashSlotCount = 256
		cfg.Slots.ReplicaCount = 3
		cfg.Channel.TickInterval = time.Millisecond
		node, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	startNodes(t, nodes...)
	t.Cleanup(func() { stopNodes(t, nodes...) })
	waitClusterReady(t, nodes...)
	for _, node := range nodes {
		waitNodeWriteReady(t, node)
	}
	id := ch.ChannelID{ID: "audit-ambiguous-room", Type: 2}
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := nodes[0].AppendChannel(warmCtx, ch.AppendRequest{ChannelID: id, CommitMode: ch.CommitModeQuorum, Message: ch.Message{MessageID: 1000, FromUID: "sender", ClientMsgNo: "warm", Payload: []byte("warm")}})
	warmCancel()
	if err != nil {
		t.Fatal(err)
	}
	meta, err := nodes[0].GetChannelRuntimeMeta(context.Background(), id.ID, int64(id.Type))
	if err != nil {
		t.Fatal(err)
	}
	var leader *Node
	var faults []*auditLostQuorumResponse
	for _, node := range nodes {
		if node.NodeID() == meta.Leader {
			leader = node
			continue
		}
		fault := &auditLostQuorumResponse{server: node.defaultChannelReplication.ExchangeServer()}
		fault.drop.Store(true)
		node.channelQuorumGateway.Replace(fault)
		faults = append(faults, fault)
	}
	if leader == nil || len(faults) != 2 {
		t.Fatalf("unexpected authority: %+v", meta)
	}
	original := ch.AppendBatchRequest{ChannelID: id, CommitMode: ch.CommitModeQuorum, ServerAllocatedMessageIDs: true, Messages: []ch.Message{
		{MessageID: 1001, FromUID: "sender-a", ClientMsgNo: "a", Payload: []byte("a"), ServerTimestampMS: 1001},
		{MessageID: 1002, FromUID: "sender-b", ClientMsgNo: "b", Payload: []byte("b"), ServerTimestampMS: 1002},
	}}
	failedCtx, failedCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, originalErr := leader.AppendChannelBatch(failedCtx, original)
	failedCancel()
	for _, fault := range faults {
		fault.drop.Store(false)
	}
	if originalErr == nil {
		t.Fatal("fault failed to create ambiguous append")
	}
	for _, fault := range faults {
		if fault.dropped.Load() == 0 {
			t.Fatal("follower fault was not exercised")
		}
	}
	t.Logf("original batch outcome=%v; both real follower responses discarded after storage", originalErr)
	return leader, id, original
}

func TestAuditThreeNodeNewSendRecoversAfterAmbiguousBatch(t *testing.T) {
	leader, id, original := AuditAmbiguousBatchFixture(t)
	// Real clients retry their own SEND, not the server-created batch that also
	// included another sender. The original caller has gone away here.
	partial := original
	partial.Messages = append([]ch.Message(nil), original.Messages[:1]...)
	partial.Messages[0].MessageID = 2001
	partial.Messages[0].ServerTimestampMS = 2001
	partialCtx, partialCancel := context.WithTimeout(context.Background(), time.Second)
	_, partialErr := leader.AppendChannelBatch(partialCtx, partial)
	partialCancel()
	t.Logf("one original sender retry after network restored=%v", partialErr)
	// The real durable duplicate index rejects a second append. The SEND
	// consumer's committed-idempotency lookup can recover the exact old ACK.
	if partialErr == nil {
		t.Fatal("individual retry appended a duplicate original message")
	}
	hit, found, lookupErr := leader.LookupChannelIdempotency(context.Background(), id, "sender-a", "a")
	if lookupErr != nil || !found || hit.Message.MessageID != 1001 || hit.Message.MessageSeq != 2 {
		t.Fatalf("individual retry cannot recover original committed identity: %+v, found=%v err=%v", hit, found, lookupErr)
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, lastErr = leader.AppendChannel(ctx, ch.AppendRequest{ChannelID: id, CommitMode: ch.CommitModeQuorum, Message: ch.Message{MessageID: uint64(3000 + attempt), FromUID: "new-sender", ClientMsgNo: fmt.Sprintf("new-%d", attempt), Payload: []byte("new")}})
		cancel()
		if lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		t.Errorf("healthy quorum cannot accept a new SEND after lost-response batch: %v", lastErr)
	}
	// Control: retaining and replaying the exact internal batch unlocks it.
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
	recovered, retryErr := leader.AppendChannelBatch(retryCtx, original)
	retryCancel()
	if retryErr != nil {
		t.Fatalf("exact internal batch control retry: %v", retryErr)
	}
	t.Logf("exact internal batch succeeds with original durable identities: %+v", recovered)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.AppendChannel(ctx, ch.AppendRequest{ChannelID: id, CommitMode: ch.CommitModeQuorum, Message: ch.Message{MessageID: 4000, FromUID: "new-sender", ClientMsgNo: "after-control", Payload: []byte("new")}}); err != nil {
		t.Fatalf("new SEND after exact batch replay: %v", err)
	}
	read, err := leader.ReadChannelCommitted(ctx, id, channelstore.ReadCommittedRequest{FromSeq: 1, MaxSeq: 100, Limit: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []uint64{1000, 1001, 1002, 3000, 4000}
	if len(read.Messages) != len(wantIDs) {
		t.Fatalf("committed rows=%+v, want five exact messages without duplicates", read.Messages)
	}
	for index, message := range read.Messages {
		if message.MessageID != wantIDs[index] || message.MessageSeq != uint64(index+1) {
			t.Fatalf("committed row %d=%+v, want id=%d seq=%d", index, message, wantIDs[index], index+1)
		}
	}
}
