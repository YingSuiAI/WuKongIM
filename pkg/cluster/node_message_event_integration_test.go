//go:build integration

package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestClusterGetMessageEventStatesBatchReadsLeaderCacheFromFollower(t *testing.T) {
	nodes := newDefaultThreeNodeCluster(t)
	startNodes(t, nodes...)
	t.Cleanup(func() { stopNodes(t, nodes...) })
	waitClusterReady(t, nodes...)

	channelID := "message-event-remote-cache"
	route := waitRouteKeyLeaderConverged(t, nodes, channelID)
	follower := firstNonLeaderNode(t, nodes, route.Leader)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := follower.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        "cmn-remote-cache",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-remote-delta",
		EventKey:           "main",
		EventType:          metadb.EventTypeDelta,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1000,
		Payload:            messageEventDeltaForIntegrationTest("remote", 1),
		UpdatedAt:          1001,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(follower delta) error = %v", err)
	}

	key := metadb.MessageEventMessageKey{ChannelID: channelID, ChannelType: 2, ClientMsgNo: "cmn-remote-cache"}
	got, err := follower.GetMessageEventStatesBatch(ctx, []metadb.MessageEventMessageKey{key}, 10)
	if err != nil {
		t.Fatalf("GetMessageEventStatesBatch(follower) error = %v", err)
	}
	if len(got[key]) != 1 || got[key][0].Status != metadb.EventStatusOpen || got[key][0].LastMsgEventSeq != 1 {
		t.Fatalf("follower state batch = %#v, want leader cached open state", got[key])
	}
	if !strings.Contains(string(got[key][0].SnapshotPayload), "remote") {
		t.Fatalf("follower snapshot = %s, want leader cached text", got[key][0].SnapshotPayload)
	}
}

func TestClusterMessageEventRecoversSnapshotAndFinishesAfterLeaderStops(t *testing.T) {
	nodes := newDefaultThreeNodeCluster(t)
	startNodes(t, nodes...)
	stoppedNodeID := uint64(0)
	t.Cleanup(func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			if nodes[i].NodeID() == stoppedNodeID {
				continue
			}
			if err := nodes[i].Stop(context.Background()); err != nil {
				t.Errorf("Stop(node=%d) error = %v", nodes[i].NodeID(), err)
			}
		}
	})
	waitClusterReady(t, nodes...)

	const (
		channelID   = "message-event-leader-failover"
		clientMsgNo = "cmn-message-event-leader-failover"
		runID       = "run-message-event-leader-failover"
	)
	route := waitRouteKeyLeaderConverged(t, nodes, channelID)
	initialLeader := route.Leader
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := firstNonLeaderNode(t, nodes, initialLeader).AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        clientMsgNo,
		RunID:              runID,
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-before-failover",
		EventKey:           "main",
		EventType:          metadb.EventTypeDelta,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1000,
		Payload:            messageEventDeltaForIntegrationTest("before", 1),
		UpdatedAt:          1001,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(before failover) error = %v", err)
	}

	stoppedNodeID = initialLeader
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := nodes[initialLeader-1].Stop(stopCtx); err != nil {
		stopCancel()
		t.Fatalf("Stop(initial leader=%d) error = %v", initialLeader, err)
	}
	stopCancel()

	active := make([]*Node, 0, 2)
	for _, node := range nodes {
		if node.NodeID() != stoppedNodeID {
			active = append(active, node)
		}
	}
	var newRoute Route
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		candidate, err := active[0].RouteKey(channelID)
		if err == nil && candidate.Leader != 0 && candidate.Leader != stoppedNodeID {
			observed, observedErr := active[1].RouteKey(channelID)
			if observedErr == nil && observed.Leader == candidate.Leader && observed.LeaderTerm == candidate.LeaderTerm {
				newRoute = candidate
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newRoute.Leader == 0 {
		t.Fatalf("message EVENT Slot leader did not recover after node %d stopped", stoppedNodeID)
	}

	writer := firstNonLeaderNode(t, active, newRoute.Leader)
	result, err := writer.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        clientMsgNo,
		RunID:              runID,
		AuthorizationFence: "fence-1",
		AuthoritySequence:  2,
		EventID:            "evt-recovery-snapshot",
		EventKey:           "main",
		EventType:          metadb.EventTypeSnapshot,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1002,
		Payload:            []byte(`{"snapshot":{"state":"running","complete":false,"text":"before","authority_sequence":2}}`),
		UpdatedAt:          1003,
	})
	if err != nil || result.MsgEventSeq != 2 {
		t.Fatalf("AppendMessageEvent(recovery snapshot) = (%#v, %v), want seq=2", result, err)
	}

	result, err = writer.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        clientMsgNo,
		RunID:              runID,
		AuthorizationFence: "fence-1",
		AuthoritySequence:  3,
		EventID:            "evt-after-failover",
		EventKey:           "main",
		EventType:          metadb.EventTypeDelta,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1004,
		Payload:            messageEventDeltaForIntegrationTest(" after", 3),
		UpdatedAt:          1005,
	})
	if err != nil || result.MsgEventSeq != 3 {
		t.Fatalf("AppendMessageEvent(delta after failover) = (%#v, %v), want seq=3", result, err)
	}

	result, err = writer.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        clientMsgNo,
		RunID:              runID,
		AuthorizationFence: "fence-1",
		AuthoritySequence:  4,
		EventID:            "evt-finish-after-failover",
		EventKey:           "main",
		EventType:          metadb.EventTypeFinish,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1006,
		Payload:            messageEventFinishForIntegrationTest("before after", 4),
		UpdatedAt:          1007,
	})
	if err != nil || result.MsgEventSeq != 4 || result.Status != metadb.EventStatusClosed {
		t.Fatalf("AppendMessageEvent(finish after failover) = (%#v, %v), want seq=4 closed", result, err)
	}

	key := metadb.MessageEventMessageKey{ChannelID: channelID, ChannelType: 2, ClientMsgNo: clientMsgNo}
	states, err := writer.GetMessageEventStatesBatch(ctx, []metadb.MessageEventMessageKey{key}, 10)
	if err != nil {
		t.Fatalf("GetMessageEventStatesBatch(after failover) error = %v", err)
	}
	if len(states[key]) != 1 || states[key][0].Status != metadb.EventStatusClosed || states[key][0].LastMsgEventSeq != 4 || states[key][0].LastAuthoritySequence != 4 {
		t.Fatalf("states after failover = %#v, want one closed lane at authority/transport seq 4", states[key])
	}
}
