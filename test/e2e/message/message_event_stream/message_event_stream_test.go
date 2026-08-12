//go:build e2e

package message_event_stream

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/test/e2e/suite"
	"github.com/stretchr/testify/require"
)

const (
	messageEventServiceToken = "message-event-e2e-service"
	messageEventRunID        = "run-e2e-1"
	messageEventFence        = uint64(1)
	messageEventOccurredAt   = int64(1700000000000)
)

type messageEventEnvelope struct {
	Status int                 `json:"status"`
	Data   messageEventPayload `json:"data"`
}

type messageEventPayload struct {
	ClientMsgNo string `json:"client_msg_no"`
	EventKey    string `json:"event_key"`
	EventID     string `json:"event_id"`
	MsgEventSeq uint64 `json:"msg_event_seq"`
	EventStatus string `json:"event_status"`
}

type channelMessageSyncPage struct {
	Messages []channelMessageSyncItem `json:"messages"`
}

type channelMessageSyncItem struct {
	ClientMsgNo string            `json:"client_msg_no"`
	EventMeta   *messageEventMeta `json:"event_meta"`
}

type messageEventMeta struct {
	HasEvents       bool                  `json:"has_events"`
	Completed       bool                  `json:"completed"`
	EventVersion    uint64                `json:"event_version"`
	LastMsgEventSeq uint64                `json:"last_msg_event_seq"`
	EventCount      int                   `json:"event_count"`
	OpenEventCount  int                   `json:"open_event_count"`
	Events          []messageEventKeyMeta `json:"events"`
}

type messageEventKeyMeta struct {
	EventKey        string         `json:"event_key"`
	Status          string         `json:"status"`
	LastMsgEventSeq uint64         `json:"last_msg_event_seq"`
	Snapshot        map[string]any `json:"snapshot"`
}

type messageEventAnchor struct {
	ChannelID   string
	FromUID     string
	ClientMsgNo string
	MessageID   int64
}

type messageEventSlotLeaderTransferResponse struct {
	SlotID       uint32             `json:"slot_id"`
	TargetNode   uint64             `json:"target_node"`
	ActualLeader uint64             `json:"actual_leader"`
	Created      bool               `json:"created"`
	Task         *suite.SlotTaskDTO `json:"task,omitempty"`
	Message      string             `json:"message"`
}

func TestWukongIMMessageEventStreamBuffersUntilFinishAndExposesMetrics(t *testing.T) {
	node := suite.New(t).StartSingleNodeCluster(suite.WithNodeConfigOverrides(1, map[string]string{"WK_API_SERVICE_TOKEN": messageEventServiceToken}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		channelID   = "e2e-message-event-stream-room"
		aliceUID    = "message-event-alice"
		clientMsgNo = "message-event-stream-base-1"
	)
	require.NoError(t, suite.PostChannel(ctx, node.APIAddr(), map[string]any{
		"channel_id":   channelID,
		"channel_type": frame.ChannelTypeGroup,
		"reset":        1,
		"subscribers":  []string{aliceUID},
	}), node.DumpDiagnostics())

	send, err := suite.PostMessageSend(ctx, node.APIAddr(), map[string]any{
		"from_uid":      aliceUID,
		"channel_id":    channelID,
		"channel_type":  frame.ChannelTypeGroup,
		"client_msg_no": clientMsgNo,
		"payload":       base64.StdEncoding.EncodeToString([]byte(`{"type":1,"run_id":"` + messageEventRunID + `","authorization_fence":1}`)),
	})
	require.NoError(t, err, node.DumpDiagnostics())
	require.Equal(t, uint8(frame.ReasonSuccess), send.Reason, node.DumpDiagnostics())
	require.NotZero(t, send.MessageSeq, node.DumpDiagnostics())
	anchor := messageEventAnchor{ChannelID: channelID, FromUID: aliceUID, ClientMsgNo: clientMsgNo, MessageID: send.MessageID}

	mainDelta := postMessageEvent(t, ctx, *node, anchor, "evt-main-delta", "main", "delta", 1, "hello ", "running")
	require.Equal(t, uint64(1), mainDelta.Data.MsgEventSeq, node.DumpDiagnostics())
	require.Equal(t, "open", mainDelta.Data.EventStatus, node.DumpDiagnostics())
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_append_total", map[string]string{
		"path":       "cache",
		"event_type": "delta",
		"result":     "ok",
	}, 1)
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_stream_cache_sessions", nil, 1)
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_stream_cache_open_lanes", nil, 1)

	toolDelta := postMessageEvent(t, ctx, *node, anchor, "evt-tool-delta", "tool", "delta", 2, "lookup", "running")
	require.Equal(t, uint64(2), toolDelta.Data.MsgEventSeq, node.DumpDiagnostics())
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_stream_cache_open_lanes", nil, 2)

	finish := postMessageEvent(t, ctx, *node, anchor, "evt-finish", "main", "finish", 3, "hello ", "succeeded")
	require.NotZero(t, finish.Data.MsgEventSeq, node.DumpDiagnostics())
	require.Equal(t, "closed", finish.Data.EventStatus, node.DumpDiagnostics())
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_append_total", map[string]string{
		"path":       "finish_batch",
		"event_type": "finish",
		"result":     "ok",
	}, 1)
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_propose_total", map[string]string{
		"path":   "finish_batch",
		"result": "ok",
	}, 1)
	suite.RequireMetricAtLeastEventually(t, *node, "wukongim_message_event_propose_batch_events_sum", map[string]string{
		"path":   "finish_batch",
		"result": "ok",
	}, 2)
	requireMetricEqualsEventually(t, *node, "wukongim_message_event_stream_cache_sessions", nil, 1)
	requireMetricEqualsEventually(t, *node, "wukongim_message_event_stream_cache_open_lanes", nil, 0)

	restartSingleNodeCluster(t, node)
	msg := requireStreamMessageEventMetaEventually(t, *node, aliceUID, channelID, clientMsgNo, send.MessageSeq, 10*time.Second)
	require.NotNil(t, msg.EventMeta, node.DumpDiagnostics())
	require.True(t, msg.EventMeta.HasEvents, node.DumpDiagnostics())
	require.True(t, msg.EventMeta.Completed, node.DumpDiagnostics())
	require.Equal(t, 2, msg.EventMeta.EventCount, node.DumpDiagnostics())
	require.Equal(t, 0, msg.EventMeta.OpenEventCount, node.DumpDiagnostics())
	require.GreaterOrEqual(t, msg.EventMeta.LastMsgEventSeq, uint64(2), node.DumpDiagnostics())
	requireEventLane(t, msg.EventMeta, "main", "closed", "hello ")
	requireEventLane(t, msg.EventMeta, "tool", "closed", "lookup")
}

func TestWukongIMMessageEventStreamFollowerForwardAndLeaderChangeSnapshotRecovery(t *testing.T) {
	cluster := suite.New(t).StartThreeNodeCluster(
		suite.WithManagerHTTP(),
		suite.WithNodeConfigOverrides(1, map[string]string{"WK_API_SERVICE_TOKEN": messageEventServiceToken}),
		suite.WithNodeConfigOverrides(2, map[string]string{"WK_API_SERVICE_TOKEN": messageEventServiceToken}),
		suite.WithNodeConfigOverrides(3, map[string]string{"WK_API_SERVICE_TOKEN": messageEventServiceToken}),
	)
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelReady()
	require.NoError(t, cluster.WaitClusterReady(readyCtx), cluster.DumpDiagnostics())

	managerNode := cluster.MustNode(1)
	initialSlots := managerSlotsForNodeEventually(t, cluster, managerNode, managerNode.Spec.ID)
	slot, initialLeader, targetLeader := chooseTransferableMessageEventSlot(t, initialSlots)
	channelID, ok := channelIDForMessageEventSlot(initialSlots, slot, fmt.Sprintf("e2e-message-event-forward-%d", time.Now().UnixNano()))
	require.True(t, ok, "channel id for slot %d not found in slots=%+v", slot.SlotID, initialSlots)
	ingress := firstNodeExcept(t, cluster, initialLeader)
	leaderNode := cluster.MustNode(initialLeader)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const (
		aliceUID    = "message-event-forward-alice"
		clientMsgNo = "message-event-forward-base-1"
	)
	require.NoError(t, suite.PostChannel(ctx, ingress.APIAddr(), map[string]any{
		"channel_id":   channelID,
		"channel_type": frame.ChannelTypeGroup,
		"reset":        1,
		"subscribers":  []string{aliceUID},
	}), cluster.DumpDiagnostics())

	send, err := suite.PostMessageSend(ctx, ingress.APIAddr(), map[string]any{
		"from_uid":      aliceUID,
		"channel_id":    channelID,
		"channel_type":  frame.ChannelTypeGroup,
		"client_msg_no": clientMsgNo,
		"payload":       base64.StdEncoding.EncodeToString([]byte(`{"type":1,"run_id":"` + messageEventRunID + `","authorization_fence":1}`)),
	})
	require.NoError(t, err, cluster.DumpDiagnostics())
	require.Equal(t, uint8(frame.ReasonSuccess), send.Reason, cluster.DumpDiagnostics())
	anchor := messageEventAnchor{ChannelID: channelID, FromUID: aliceUID, ClientMsgNo: clientMsgNo, MessageID: send.MessageID}

	postStreamDeltas(t, ctx, *ingress, anchor, "evt-main-delta", "evt-tool-delta", 1)
	suite.RequireMetricAtLeastEventually(t, *ingress, "wukongim_message_event_append_total", map[string]string{
		"path":       "forward",
		"event_type": "delta",
		"result":     "ok",
	}, 2)
	suite.RequireMetricAtLeastEventually(t, *leaderNode, "wukongim_message_event_append_total", map[string]string{
		"path":       "cache",
		"event_type": "delta",
		"result":     "ok",
	}, 2)
	suite.RequireMetricAtLeastEventually(t, *leaderNode, "wukongim_message_event_stream_cache_open_lanes", nil, 2)

	transferCtx, cancelTransfer := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelTransfer()
	accepted := postSlotLeaderTransfer(t, transferCtx, cluster, slot.SlotID, targetLeader)
	require.Equal(t, slot.SlotID, accepted.SlotID)
	require.NotZero(t, accepted.ActualLeader)
	moved := requireSlotLeaderMoved(t, transferCtx, cluster, managerNode, slot.SlotID, initialLeader)
	newLeader := moved.NodeLog.LeaderID
	require.NotEqual(t, initialLeader, newLeader)
	newLeaderNode := cluster.MustNode(newLeader)
	finishIngress := firstNodeExcept(t, cluster, newLeader)

	requireMetricEqualsEventually(t, *leaderNode, "wukongim_message_event_stream_cache_sessions", nil, 0)
	requireMetricEqualsEventually(t, *leaderNode, "wukongim_message_event_stream_cache_open_lanes", nil, 0)

	postMessageEvent(t, ctx, *finishIngress, anchor, "evt-main-snapshot-replay", "main", "snapshot", 3, "hello ", "running")
	postMessageEvent(t, ctx, *finishIngress, anchor, "evt-tool-snapshot-replay", "tool", "snapshot", 4, "lookup", "running")
	finish := postMessageEvent(t, ctx, *finishIngress, anchor, "evt-finish", "main", "finish", 5, "hello ", "succeeded")
	require.NotZero(t, finish.Data.MsgEventSeq, cluster.DumpDiagnostics())
	require.Equal(t, "closed", finish.Data.EventStatus, cluster.DumpDiagnostics())
	suite.RequireMetricAtLeastEventually(t, *newLeaderNode, "wukongim_message_event_propose_total", map[string]string{
		"path":   "finish_batch",
		"result": "ok",
	}, 1)
	suite.RequireMetricAtLeastEventually(t, *newLeaderNode, "wukongim_message_event_propose_batch_events_sum", map[string]string{
		"path":   "finish_batch",
		"result": "ok",
	}, 2)
	requireMetricEqualsEventually(t, *newLeaderNode, "wukongim_message_event_stream_cache_open_lanes", nil, 0)

	duplicate := postMessageEvent(t, ctx, *finishIngress, anchor, "evt-finish", "main", "finish", 5, "hello ", "succeeded")
	require.Equal(t, finish.Data.MsgEventSeq, duplicate.Data.MsgEventSeq, cluster.DumpDiagnostics())
	require.Equal(t, "closed", duplicate.Data.EventStatus, cluster.DumpDiagnostics())
}

func postMessageEvent(t *testing.T, ctx context.Context, node suite.StartedNode, anchor messageEventAnchor, eventID, eventKey, eventType string, authoritySequence uint64, text, state string) messageEventEnvelope {
	t.Helper()

	payload := map[string]any{
		"event_id": eventID, "run_id": messageEventRunID,
		"base_message": map[string]any{
			"conversation_id": "019c0000-0000-7000-8000-000000000001",
			"message_id":      "019c0000-0000-7000-8000-000000000002",
			"client_msg_id":   anchor.ClientMsgNo, "committed_message_ref": "agent-run.e2e",
			"message_sequence": 1, "source_principal_id": anchor.FromUID,
		},
		"event_key": eventKey, "event_type": eventType, "authority_sequence": authoritySequence,
		"authorization_fence": messageEventFence,
		"occurred_at":         "2023-11-14T22:13:20Z",
	}
	if eventType == "delta" {
		payload["text_delta"] = text
	} else {
		payload["snapshot"] = map[string]any{
			"state": state, "authority_sequence": authoritySequence, "text": text, "complete": eventType == "finish",
		}
	}
	body := map[string]any{
		"channel_id": anchor.ChannelID, "channel_type": frame.ChannelTypeGroup, "from_uid": anchor.FromUID,
		"message_id": anchor.MessageID, "client_msg_no": anchor.ClientMsgNo, "run_id": messageEventRunID,
		"authorization_fence": messageEventFence, "authority_sequence": authoritySequence,
		"event_id": eventID, "event_key": eventKey, "event_type": eventType, "visibility": "public",
		"occurred_at": messageEventOccurredAt, "payload": payload,
	}
	var out messageEventEnvelope
	err := postServiceJSON(ctx, "http://"+node.APIAddr()+"/message/events:append", body, &out)
	require.NoError(t, err, node.DumpDiagnostics())
	require.Equal(t, 200, out.Status, node.DumpDiagnostics())
	return out
}

func postMessageEventError(ctx context.Context, node suite.StartedNode, anchor messageEventAnchor, eventID, eventType string, authoritySequence uint64) error {
	var out messageEventEnvelope
	payload := map[string]any{
		"event_id": eventID, "run_id": messageEventRunID,
		"base_message": map[string]any{
			"conversation_id": "019c0000-0000-7000-8000-000000000001", "message_id": "019c0000-0000-7000-8000-000000000002",
			"client_msg_id": anchor.ClientMsgNo, "committed_message_ref": "agent-run.e2e", "message_sequence": 1, "source_principal_id": anchor.FromUID,
		},
		"event_key": "main", "event_type": eventType, "authority_sequence": authoritySequence, "authorization_fence": messageEventFence, "occurred_at": "2023-11-14T22:13:20Z",
		"snapshot": map[string]any{"state": "succeeded", "authority_sequence": authoritySequence, "text": "hello ", "complete": true},
	}
	body := map[string]any{
		"channel_id": anchor.ChannelID, "channel_type": frame.ChannelTypeGroup, "from_uid": anchor.FromUID,
		"message_id": anchor.MessageID, "client_msg_no": anchor.ClientMsgNo, "run_id": messageEventRunID,
		"authorization_fence": messageEventFence, "authority_sequence": authoritySequence,
		"event_id": eventID, "event_key": "main", "event_type": eventType, "visibility": "public",
		"occurred_at": messageEventOccurredAt, "payload": payload,
	}
	return postServiceJSON(ctx, "http://"+node.APIAddr()+"/message/events:append", body, &out)
}

func postStreamDeltas(t *testing.T, ctx context.Context, node suite.StartedNode, anchor messageEventAnchor, mainEventID, toolEventID string, startSequence uint64) {
	t.Helper()

	mainDelta := postMessageEvent(t, ctx, node, anchor, mainEventID, "main", "delta", startSequence, "hello ", "running")
	require.NotZero(t, mainDelta.Data.MsgEventSeq, node.DumpDiagnostics())
	require.Equal(t, "open", mainDelta.Data.EventStatus, node.DumpDiagnostics())

	toolDelta := postMessageEvent(t, ctx, node, anchor, toolEventID, "tool", "delta", startSequence+1, "lookup", "running")
	require.NotZero(t, toolDelta.Data.MsgEventSeq, node.DumpDiagnostics())
}

func postServiceJSON(ctx context.Context, url string, body, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+messageEventServiceToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return json.Unmarshal(responseBody, out)
}

func restartSingleNodeCluster(t *testing.T, node *suite.StartedNode) {
	t.Helper()

	require.NotNil(t, node)
	require.NotNil(t, node.Process)
	require.NoError(t, node.Restart(node.Process.BinaryPath))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, suite.WaitHTTPReady(ctx, node.APIAddr(), "/readyz"), node.DumpDiagnostics())
	require.NoError(t, suite.WaitWKProtoReady(ctx, node.GatewayAddr()), node.DumpDiagnostics())
}

func requireMetricEqualsEventually(t *testing.T, node suite.StartedNode, name string, labels map[string]string, want float64) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var last float64
	var lastErr error
	for {
		last, lastErr = suite.FetchMetricValue(ctx, node.APIAddr(), name, labels)
		if lastErr == nil && last == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("metric %s%v = %v err=%v, want %v\n%s", name, labels, last, lastErr, want, node.DumpDiagnostics())
		case <-ticker.C:
		}
	}
}

func requireStreamMessageEventMetaEventually(t *testing.T, node suite.StartedNode, loginUID, channelID, clientMsgNo string, startSeq uint64, timeout time.Duration) channelMessageSyncItem {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastPage channelMessageSyncPage
	var lastErr error
	for {
		var page channelMessageSyncPage
		_, err := suite.PostJSON(ctx, "http://"+node.APIAddr()+"/channel/messagesync", map[string]any{
			"login_uid":          loginUID,
			"channel_id":         channelID,
			"channel_type":       frame.ChannelTypeGroup,
			"start_message_seq":  startSeq,
			"limit":              10,
			"include_event_meta": 1,
		}, &page)
		if err == nil {
			lastPage = page
			for _, msg := range page.Messages {
				if msg.ClientMsgNo == clientMsgNo && msg.EventMeta != nil && msg.EventMeta.Completed {
					return msg
				}
			}
			lastErr = fmt.Errorf("message %s with completed event meta not found", clientMsgNo)
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			t.Fatalf("message event meta timed out: lastPage=%#v lastErr=%v\n%s", lastPage, lastErr, node.DumpDiagnostics())
		case <-ticker.C:
		}
	}
}

func requireEventLane(t *testing.T, meta *messageEventMeta, eventKey, status, text string) {
	t.Helper()

	for _, lane := range meta.Events {
		if lane.EventKey != eventKey {
			continue
		}
		require.Equal(t, status, lane.Status)
		require.NotZero(t, lane.LastMsgEventSeq)
		require.Contains(t, fmt.Sprint(lane.Snapshot), text)
		return
	}
	t.Fatalf("event lane %s not found in %#v", eventKey, meta.Events)
}

type managerSlotsPage struct {
	Items []suite.SlotDTO `json:"items"`
}

func managerSlotsForNodeEventually(t *testing.T, cluster *suite.StartedCluster, managerNode *suite.StartedNode, nodeID uint64) []suite.SlotDTO {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastItems []suite.SlotDTO
	var lastErr error
	for {
		items, err := managerSlotsForNode(ctx, managerNode, nodeID)
		if err == nil {
			lastItems = items
			if slotsHaveNodeLogLeaders(items) {
				return items
			}
			lastErr = fmt.Errorf("slot node logs = %+v", items)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("manager slots for node %d did not expose slot leaders: lastItems=%+v lastErr=%v\n%s", nodeID, lastItems, lastErr, cluster.DumpDiagnostics())
		case <-ticker.C:
		}
	}
}

func managerSlotsForNode(ctx context.Context, managerNode *suite.StartedNode, nodeID uint64) ([]suite.SlotDTO, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var page managerSlotsPage
	query := url.Values{}
	query.Set("node_id", fmt.Sprintf("%d", nodeID))
	_, err := suite.GetJSON(reqCtx, "http://"+managerNode.ManagerAddr()+"/manager/slots?"+query.Encode(), &page)
	if err != nil {
		return nil, err
	}
	sort.Slice(page.Items, func(i, j int) bool { return page.Items[i].SlotID < page.Items[j].SlotID })
	return page.Items, nil
}

func slotsHaveNodeLogLeaders(items []suite.SlotDTO) bool {
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		if item.Task != nil || item.NodeLog == nil || item.NodeLog.LeaderID == 0 {
			return false
		}
	}
	return true
}

func chooseTransferableMessageEventSlot(t *testing.T, slots []suite.SlotDTO) (suite.SlotDTO, uint64, uint64) {
	t.Helper()

	for _, slot := range slots {
		if slot.NodeLog == nil || slot.NodeLog.LeaderID == 0 {
			continue
		}
		source := slot.NodeLog.LeaderID
		for _, peer := range slot.Assignment.DesiredPeers {
			if peer != source {
				return slot, source, peer
			}
		}
	}
	t.Fatalf("no transferable message event slot found in slots=%+v", slots)
	return suite.SlotDTO{}, 0, 0
}

func channelIDForMessageEventSlot(slots []suite.SlotDTO, slot suite.SlotDTO, prefix string) (string, bool) {
	hashSlotCount, ok := hashSlotCountFromSlots(slots)
	if !ok || slot.HashSlots == nil {
		return "", false
	}
	targets := make(map[uint16]struct{}, len(slot.HashSlots.Items))
	for _, item := range slot.HashSlots.Items {
		targets[item] = struct{}{}
	}
	for i := 0; i < 10000; i++ {
		channelID := fmt.Sprintf("%s-%04d", prefix, i)
		if _, ok := targets[routing.HashSlotForKey(channelID, hashSlotCount)]; ok {
			return channelID, true
		}
	}
	return "", false
}

func hashSlotCountFromSlots(slots []suite.SlotDTO) (uint16, bool) {
	var max uint16
	found := false
	for _, slot := range slots {
		if slot.HashSlots == nil {
			continue
		}
		for _, item := range slot.HashSlots.Items {
			if !found || item > max {
				max = item
			}
			found = true
		}
	}
	if !found || max == ^uint16(0) {
		return 0, false
	}
	return max + 1, true
}

func firstNodeExcept(t *testing.T, cluster *suite.StartedCluster, excluded uint64) *suite.StartedNode {
	t.Helper()

	for i := range cluster.Nodes {
		if cluster.Nodes[i].Spec.ID != excluded {
			return &cluster.Nodes[i]
		}
	}
	t.Fatalf("no node found outside excluded node %d", excluded)
	return nil
}

func postSlotLeaderTransfer(t *testing.T, ctx context.Context, cluster *suite.StartedCluster, slotID uint32, target uint64) messageEventSlotLeaderTransferResponse {
	t.Helper()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	nodeErrors := make(map[string]string)
	for {
		for _, node := range cluster.Nodes {
			var out messageEventSlotLeaderTransferResponse
			_, err := suite.PostJSON(ctx, fmt.Sprintf("http://%s/manager/slots/%d/leader-transfer", node.ManagerAddr(), slotID), map[string]any{
				"target_node": target,
			}, &out)
			if err == nil {
				return out
			}
			nodeErrors[fmt.Sprintf("node %d (%s)", node.Spec.ID, node.ManagerAddr())] = err.Error()
		}

		select {
		case <-ctx.Done():
			t.Fatalf("no manager node accepted slot leader transfer slot=%d target=%d errors=%s\n%s", slotID, target, formatNodeErrors(nodeErrors), cluster.DumpDiagnostics())
		case <-ticker.C:
		}
	}
}

func requireSlotLeaderMoved(t *testing.T, ctx context.Context, cluster *suite.StartedCluster, managerNode *suite.StartedNode, slotID uint32, source uint64) suite.SlotDTO {
	t.Helper()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var last suite.SlotDTO
	var lastErr error
	for {
		slots, err := managerSlotsForNode(ctx, managerNode, managerNode.Spec.ID)
		if err == nil {
			if slot, ok := findSlot(slots, slotID); ok {
				last = slot
				if slot.Task == nil && slot.NodeLog != nil && slot.NodeLog.LeaderID != 0 && slot.NodeLog.LeaderID != source {
					return slot
				}
				lastErr = fmt.Errorf("slot %d leader/task = %+v/%+v", slotID, slot.NodeLog, slot.Task)
			} else {
				lastErr = fmt.Errorf("slot %d missing from slots %+v", slotID, slots)
			}
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			t.Fatalf("slot leader did not move from source %d: last=%+v lastErr=%v\n%s", source, last, lastErr, cluster.DumpDiagnostics())
		case <-ticker.C:
		}
	}
}

func findSlot(items []suite.SlotDTO, slotID uint32) (suite.SlotDTO, bool) {
	for _, item := range items {
		if item.SlotID == slotID {
			return item, true
		}
	}
	return suite.SlotDTO{}, false
}

func formatNodeErrors(nodeErrors map[string]string) string {
	if len(nodeErrors) == 0 {
		return "<none>"
	}
	keys := make([]string, 0, len(nodeErrors))
	for key := range nodeErrors {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("%s: %s", key, nodeErrors[key]))
	}
	return strings.Join(lines, "; ")
}
