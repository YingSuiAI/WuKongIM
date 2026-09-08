//go:build e2e

package payload_correction

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	"github.com/WuKongIM/WuKongIM/test/e2e/suite"
	"github.com/stretchr/testify/require"
)

const serviceToken = "isolated-payload-correction-e2e"

type message struct {
	MessageID  uint64 `json:"message_id"`
	MessageSeq uint64 `json:"message_seq"`
	Payload    []byte `json:"payload"`
	Proof      *struct {
		OperationID string `json:"operation_id"`
		Original    string `json:"original_payload_sha256"`
		Corrected   string `json:"corrected_payload_sha256"`
	} `json:"payload_correction"`
}

func TestThreeNodeCorrectionReadViewsLeaderChangeRestartAndOriginalSEND(t *testing.T) {
	cluster := suite.New(t).StartThreeNodeCluster(suite.WithManagerHTTP(),
		suite.WithNodeConfigOverrides(1, map[string]string{"WK_API_SERVICE_TOKEN": serviceToken, "WK_CLUSTER_HASH_SLOT_COUNT": "256"}),
		suite.WithNodeConfigOverrides(2, map[string]string{"WK_API_SERVICE_TOKEN": serviceToken, "WK_CLUSTER_HASH_SLOT_COUNT": "256"}),
		suite.WithNodeConfigOverrides(3, map[string]string{"WK_API_SERVICE_TOKEN": serviceToken, "WK_CLUSTER_HASH_SLOT_COUNT": "256"}))
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	require.NoError(t, cluster.WaitClusterReady(ctx), cluster.DumpDiagnostics())
	_, err := cluster.WaitSlotLeadersStable(ctx, 200*time.Millisecond)
	require.NoError(t, err)
	slots := nodeSlots(t, ctx, cluster)
	const channelID, sender, clientNo = "correction-e2e-group", "correction-human", "stable-original-send"
	var slot suite.SlotDTO
	for _, candidate := range slots {
		if candidate.HashSlots != nil {
			for _, hash := range candidate.HashSlots.Items {
				if hash == routing.HashSlotForKey(channelID, 256) {
					slot = candidate
				}
			}
		}
	}
	require.NotNil(t, slot.NodeLog)
	require.NotZero(t, slot.NodeLog.LeaderID)
	initialLeader := slot.NodeLog.LeaderID
	var ingress *suite.StartedNode
	var target uint64
	for _, node := range cluster.Nodes {
		if node.Spec.ID != initialLeader {
			ingress = cluster.MustNode(node.Spec.ID)
			target = node.Spec.ID
			break
		}
	}
	require.NotNil(t, ingress)
	require.NoError(t, suite.PostChannel(ctx, ingress.APIAddr(), map[string]any{"channel_id": channelID, "channel_type": 2, "subscribers": []string{sender}}))
	original, corrected := []byte(`{"type":1,"content":"old-forward-shape"}`), []byte(`{"type":1,"content":"canonical-forward-shape"}`)
	sendBody := map[string]any{"from_uid": sender, "channel_id": channelID, "channel_type": 2, "client_msg_no": clientNo, "payload": original}
	sent, err := suite.PostMessageSendEventually(ctx, ingress.APIAddr(), sendBody)
	require.NoError(t, err, cluster.DumpDiagnostics())
	require.Equal(t, uint8(1), sent.Reason)
	correction := map[string]any{"operation_id": "correction-operation", "channel_id": channelID, "channel_type": 2,
		"message_id": sent.MessageID, "message_seq": sent.MessageSeq, "from_uid": sender, "client_msg_no": clientNo,
		"expected_original_payload_sha256": sha(original), "corrected_payload_sha256": sha(corrected), "corrected_payload": corrected}
	claim := map[string]any{"channel_id": channelID, "channel_type": 2, "from_uid": sender, "client_msg_no": clientNo, "expected_original_payload_sha256": sha(original)}
	point := map[string]any{"channel_id": channelID, "channel_type": 2, "message_id": sent.MessageID, "message_seq": sent.MessageSeq}
	var receipt struct {
		Applied     bool   `json:"applied"`
		OperationID string `json:"operation_id"`
	}
	require.Equal(t, 401, post(t, ctx, ingress.APIAddr(), "/channel/message-payload-correction", "", correction, nil))
	require.Equal(t, 200, post(t, ctx, ingress.APIAddr(), "/channel/message-payload-correction", serviceToken, correction, &receipt))
	require.True(t, receipt.Applied)
	require.Equal(t, "correction-operation", receipt.OperationID)
	require.Equal(t, 200, post(t, ctx, ingress.APIAddr(), "/channel/message-payload-correction", serviceToken, correction, &receipt))
	require.False(t, receipt.Applied)
	correction["operation_id"] = "conflicting-operation"
	require.Equal(t, 409, post(t, ctx, ingress.APIAddr(), "/channel/message-payload-correction", serviceToken, correction, nil))
	correction["operation_id"] = "correction-operation"
	checkMessage := func(got message) {
		t.Helper()
		require.Equal(t, uint64(sent.MessageID), got.MessageID)
		require.Equal(t, sent.MessageSeq, got.MessageSeq)
		require.Equal(t, corrected, got.Payload)
		require.NotNil(t, got.Proof)
		require.Equal(t, "correction-operation", got.Proof.OperationID)
		require.Equal(t, sha(original), got.Proof.Original)
		require.Equal(t, sha(corrected), got.Proof.Corrected)
	}
	verify := func() {
		t.Helper()
		for _, node := range cluster.Nodes {
			var got message
			require.Equal(t, 200, post(t, ctx, node.APIAddr(), "/channel/committed-message", serviceToken, point, &got))
			checkMessage(got)
			var proof struct {
				State   string  `json:"state"`
				Message message `json:"message"`
			}
			require.Equal(t, 200, post(t, ctx, node.APIAddr(), "/channel/committed-message-claim", serviceToken, claim, &proof))
			require.Equal(t, "committed", proof.State)
			checkMessage(proof.Message)
			var page struct {
				Messages []message `json:"messages"`
				ScanHead uint64    `json:"scan_head"`
			}
			require.Equal(t, 200, post(t, ctx, node.APIAddr(), "/channel/committed-messages", serviceToken, map[string]any{"channel_id": channelID, "channel_type": 2, "limit": 10}, &page))
			require.Equal(t, sent.MessageSeq, page.ScanHead)
			require.Len(t, page.Messages, 1)
			checkMessage(page.Messages[0])
			page.Messages = nil
			require.Equal(t, 200, post(t, ctx, node.APIAddr(), "/channel/messagesync", "", map[string]any{"login_uid": sender, "channel_id": channelID, "channel_type": 2, "start_message_seq": sent.MessageSeq, "limit": 10}, &page))
			require.Len(t, page.Messages, 1)
			checkMessage(page.Messages[0])
		}
	}
	verify()
	claim["client_msg_no"] = "never-sent"
	var absent struct {
		State string `json:"state"`
	}
	require.Equal(t, 200, post(t, ctx, ingress.APIAddr(), "/channel/committed-message-claim", serviceToken, claim, &absent))
	require.Equal(t, "not_committed", absent.State)
	claim["client_msg_no"] = clientNo
	// Transfer through the actual Controller leader; never retry an unknown mutation.
	var accepted any
	_, err = suite.PostJSON(ctx, fmt.Sprintf("http://%s/manager/slots/%d/leader-transfer", cluster.MustNode(1).ManagerAddr(), slot.SlotID), map[string]any{"target_node": target}, &accepted)
	require.NoError(t, err, cluster.DumpDiagnostics())
	require.Eventually(t, func() bool {
		inventory := nodeSlots(t, ctx, cluster)
		for _, current := range inventory {
			if current.SlotID == slot.SlotID {
				return current.NodeLog != nil && current.NodeLog.LeaderID == target
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)
	verify()
	require.NoError(t, cluster.RestartNode(initialLeader), cluster.DumpDiagnostics())
	require.NoError(t, cluster.WaitClusterReady(ctx), cluster.DumpDiagnostics())
	verify()
	replayed, err := suite.PostMessageSendEventually(ctx, cluster.MustNode(target).APIAddr(), sendBody)
	require.NoError(t, err)
	require.Equal(t, sent, replayed)
	verify()
	require.Equal(t, 200, post(t, ctx, cluster.MustNode(target).APIAddr(), "/channel/message-payload-correction", serviceToken, correction, &receipt))
	require.False(t, receipt.Applied)
	// Terminal deletion removes corrected content and cannot expose the raw
	// original through service point/claim or recreate a correction on replay.
	require.Equal(t, 200, post(t, ctx, cluster.MustNode(target).APIAddr(), "/channel/delete", "", map[string]any{"channel_id": channelID, "channel_type": 2}, nil))
	for _, node := range cluster.Nodes {
		require.Equal(t, 404, post(t, ctx, node.APIAddr(), "/channel/committed-message", serviceToken, point, nil))
		require.Equal(t, 200, post(t, ctx, node.APIAddr(), "/channel/committed-message-claim", serviceToken, claim, &absent))
		require.Equal(t, "not_committed", absent.State)
		require.Equal(t, 404, post(t, ctx, node.APIAddr(), "/channel/message-payload-correction", serviceToken, correction, nil))
	}
}

func sha(payload []byte) string { sum := sha256.Sum256(payload); return hex.EncodeToString(sum[:]) }

func nodeSlots(t *testing.T, ctx context.Context, cluster *suite.StartedCluster) []suite.SlotDTO {
	t.Helper()
	var page struct {
		Items []suite.SlotDTO `json:"items"`
	}
	_, err := suite.GetJSON(ctx, "http://"+cluster.MustNode(1).ManagerAddr()+"/manager/slots?node_id=1", &page)
	require.NoError(t, err)
	return page.Items
}

func post(t *testing.T, ctx context.Context, addr, path, token string, body, out any) int {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024))
	require.NoError(t, err)
	if response.StatusCode == http.StatusOK && out != nil {
		require.NoError(t, json.Unmarshal(data, out))
	}
	if response.StatusCode >= 500 {
		t.Logf("%s status=%d", path, response.StatusCode)
	}
	return response.StatusCode
}
