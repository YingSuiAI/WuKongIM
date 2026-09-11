//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	coregateway "github.com/WuKongIM/WuKongIM/pkg/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	imadmission "github.com/YingSuiAI/centerim-contracts/generated/imadmission-go"
	"github.com/stretchr/testify/require"
)

func TestMessageAdmissionLoopbackCommitsCanonicalBytesAndRecoversSameSend(t *testing.T) {
	const sessionID = "019c0000-0000-7000-8000-000000000001"
	const publicID = "019c0000-0000-7000-8000-000000000002"
	canonical := []byte(`{"id":"` + publicID + `","type":"message.committed","payload":{"text":"safe admitted text"}}`)
	const original = `{"id":"forged-id","origin":"service","im_session_id":"forged","body":"original text"}`
	var unavailable atomic.Bool
	var requestMu sync.Mutex
	var requests []imadmission.ImMessageAdmissionRequest
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(503)
			return
		}
		digest := sha256.Sum256(body)
		mac := hmac.New(sha256.New, []byte("admission-test-secret"))
		_, _ = mac.Write([]byte("im.message.admission.v1\n" + r.Header.Get("X-IM-Timestamp") + "\n" + r.Header.Get("X-IM-Request-ID") + "\n" + hex.EncodeToString(digest[:])))
		if r.Method != http.MethodPost || r.URL.Path != "/internal/im/message-admissions" || !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(r.Header.Get("X-IM-Signature"))) {
			t.Error("admission request failed independent route/signature verification")
			w.WriteHeader(401)
			return
		}
		var input imadmission.ImMessageAdmissionRequest
		if json.Unmarshal(body, &input) != nil {
			w.WriteHeader(400)
			return
		}
		requestMu.Lock()
		requests = append(requests, input)
		requestMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"ADMISSION_UNAVAILABLE"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(imadmission.ImMessageAdmissionResponse{Payload: canonical})
	}))
	t.Cleanup(backend.Close)
	config := singleNodeClusterAppConfig(t)
	config.Message.AdmissionURL = backend.URL + "/internal/im/message-admissions"
	config.Webhook.SigningSecret = "admission-test-secret"
	config.API = APIConfig{ListenAddr: "127.0.0.1:0", ServiceToken: "service-test-secret"}
	config.Gateway.SendTimeout = 2 * time.Second
	application, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, application.Stop(ctx))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, application.Start(ctx))
	node := application.cluster.(*cluster.Node)
	channelID := channelruntime.ChannelID{ID: "room-admitted", Type: frame.ChannelTypeGroup}
	waitSingleNodeClusterRouteLeader(t, node, channelID.ID, config.NodeID)
	waitSingleNodeClusterNodeSchedulable(t, node, config.NodeID)
	seedGroupSendPermission(t, node, channelID, "u1")
	require.NoError(t, node.UpsertDeviceMetadata(ctx, metadb.Device{
		UID: "u1", DeviceID: "device-1", AppInstanceID: "app-1", DeviceFlag: int64(frame.APP),
		DeviceSessionID: "device-session-1", IMSessionID: sessionID, InstallationGeneration: 1,
		SessionGeneration: 1, AuthorizationFence: 1, Token: "device-test-token", DeviceLevel: int64(frame.DeviceLevelMaster),
	}))
	auth := coregateway.NewWKProtoAuthenticator(coregateway.WKProtoAuthOptions{
		TokenAuthOn: true, DisableEncryption: true, VerifyToken: application.verifyWKProtoToken,
	})
	verified, err := auth.Authenticate(nil, &frame.ConnectPacket{
		Version: frame.LatestVersion, UID: "u1", DeviceID: "device-1", AppInstanceID: "app-1", DeviceFlag: frame.APP,
		InstallationGeneration: 1, SessionGeneration: 1, Token: "device-test-token",
	})
	require.NoError(t, err)
	require.Equal(t, frame.ReasonSuccess, verified.Connack.ReasonCode)
	require.Equal(t, sessionID, verified.SessionValues[coregateway.SessionValueIMSessionID])
	send := func(clientNo string, noPersist, syncOnce bool) *frame.SendackPacket {
		t.Helper()
		writes := &sendackSmokeSessionWrites{}
		sess := newSendackSmokeSession(writes)
		for key, value := range verified.SessionValues {
			sess.SetValue(key, value)
		}
		packet := &frame.SendPacket{Framer: frame.Framer{NoPersist: noPersist, SyncOnce: syncOnce},
			ClientSeq: 11, ClientMsgNo: clientNo, ChannelID: channelID.ID, ChannelType: channelID.Type, Payload: []byte(original)}
		require.NoError(t, application.Handler().OnFrame(coregateway.Context{Session: sess, RequestContext: ctx}, packet))
		return writes.requireOnlySendack(t)
	}
	first := send("client-message-0001", false, false)
	require.Equal(t, frame.ReasonSuccess, first.ReasonCode)
	require.Equal(t, publicID, first.ApplicationMessageID)
	second := send("client-message-0001", false, false)
	require.Equal(t, first.MessageID, second.MessageID)
	require.Equal(t, first.MessageSeq, second.MessageSeq)
	require.Equal(t, first.ApplicationMessageID, second.ApplicationMessageID)
	stored, found, err := node.ReadChannelCommittedMessage(ctx, channelID, uint64(first.MessageID), first.MessageSeq)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, canonical, stored.Payload)
	require.Equal(t, "u1", stored.FromUID)
	require.Equal(t, "client-message-0001", stored.ClientMsgNo)
	requestMu.Lock()
	observed := append([]imadmission.ImMessageAdmissionRequest(nil), requests...)
	requestMu.Unlock()
	require.Len(t, observed, 2)
	for _, input := range observed {
		require.Equal(t, "device", string(input.Origin))
		require.Equal(t, "u1", input.FromUid)
		require.NotNil(t, input.ImSessionId)
		require.Equal(t, sessionID, input.ImSessionId.String())
		require.Equal(t, []byte(original), input.Payload)
	}
	// Reuse the real service claim endpoint, not Manager or uncommitted storage inspection.
	canonicalDigest := sha256.Sum256(canonical)
	claimBody, err := json.Marshal(map[string]any{"channel_id": channelID.ID, "channel_type": channelID.Type,
		"from_uid": "u1", "client_msg_no": "client-message-0001", "expected_original_payload_sha256": hex.EncodeToString(canonicalDigest[:])})
	require.NoError(t, err)
	apiServer := application.api.(*accessapi.Server)
	claimRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiServer.Addr()+"/channel/committed-message-claim", bytes.NewReader(claimBody))
	require.NoError(t, err)
	claimRequest.Header.Set("Content-Type", "application/json")
	claimRequest.Header.Set("Authorization", "Bearer service-test-secret")
	claimResponse, err := http.DefaultClient.Do(claimRequest)
	require.NoError(t, err)
	claimResult, err := io.ReadAll(claimResponse.Body)
	claimResponse.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, claimResponse.StatusCode)
	require.Contains(t, string(claimResult), `"state":"committed"`)
	// A service retry obtains the actual old commit identity and clock in one SEND response.
	// The fixed service token, not any JSON origin/session field, establishes its origin.
	serviceBody, err := json.Marshal(map[string]any{"from_uid": "u1", "channel_id": channelID.ID, "channel_type": channelID.Type,
		"client_msg_no": "client-message-0001", "payload": base64.StdEncoding.EncodeToString([]byte(original)),
		"origin": "device", "im_session_id": "forged"})
	require.NoError(t, err)
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiServer.Addr()+"/message/send", bytes.NewReader(serviceBody))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer service-test-secret")
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		var receipt struct {
			MessageID            int64  `json:"message_id"`
			MessageSeq           uint64 `json:"message_seq"`
			ApplicationMessageID string `json:"application_message_id"`
			Timestamp            int64  `json:"timestamp"`
		}
		err = json.NewDecoder(response.Body).Decode(&receipt)
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, first.MessageID, receipt.MessageID)
		require.Equal(t, first.MessageSeq, receipt.MessageSeq)
		require.Equal(t, publicID, receipt.ApplicationMessageID)
		require.Equal(t, stored.ServerTimestampMS/1000, receipt.Timestamp)
		require.Positive(t, receipt.Timestamp)
	}
	requestMu.Lock()
	serviceRequests := append([]imadmission.ImMessageAdmissionRequest(nil), requests[2:]...)
	requestMu.Unlock()
	require.Len(t, serviceRequests, 2)
	for _, request := range serviceRequests {
		require.Equal(t, "service", string(request.Origin))
		require.Nil(t, request.ImSessionId)
	}
	unavailable.Store(true)
	failed := send("client-message-failed", false, false)
	require.NotEqual(t, frame.ReasonSuccess, failed.ReasonCode)
	require.Empty(t, failed.ApplicationMessageID)
	require.NotEqual(t, frame.ReasonSuccess, send("client-transient-0001", true, false).ReasonCode)
	require.NotEqual(t, frame.ReasonSuccess, send("client-command-0001", false, true).ReasonCode)
	head, err := node.ReadChannelCommittedHead(ctx, channelID)
	require.NoError(t, err)
	require.Equal(t, first.MessageSeq, head)
	requestMu.Lock()
	count := len(requests)
	requestMu.Unlock()
	require.Equal(t, 5, count, "device flags must fail before admission callback, and callback failure must not append")
}
