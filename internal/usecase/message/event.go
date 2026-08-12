package message

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/pkg/protocol/channelid"
)

// AppendMessageEvent validates and persists one message-scoped event projection update.
func (a *App) AppendMessageEvent(ctx context.Context, event MessageEventAppend) (MessageEventAppendResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	event, err := normalizeMessageEventAppendCommand(event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if a == nil || a.eventStore == nil {
		return MessageEventAppendResult{}, ErrMessageEventStoreRequired
	}
	anchor, found, err := a.eventStore.LookupMessageEventAnchor(ctx, event.ChannelID, event.ChannelType, event.MessageID)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if !found {
		return MessageEventAppendResult{}, ErrMessageEventTargetNotFound
	}
	if anchor.ChannelID != event.ChannelID || anchor.ChannelType != event.ChannelType || anchor.MessageID != event.MessageID || anchor.ClientMsgNo != event.ClientMsgNo || anchor.FromUID != event.FromUID || anchor.RunID != event.RunID || anchor.AuthorizationFence != event.AuthorizationFence {
		return MessageEventAppendResult{}, ErrMessageEventAnchorMismatch
	}
	if err := validateAgentRunMessageEventPayload(event); err != nil {
		return MessageEventAppendResult{}, err
	}
	if event.UpdatedAt == 0 {
		now := time.Now
		if a.now != nil {
			now = a.now
		}
		event.UpdatedAt = now().UnixMilli()
	}
	result, err := a.eventStore.AppendMessageEvent(ctx, event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	result.FromUID = event.FromUID
	result.MessageID = event.MessageID
	result.State.SnapshotPayload = cloneBytes(result.State.SnapshotPayload)
	return result, nil
}

func normalizeMessageEventAppendCommand(event MessageEventAppend) (MessageEventAppend, error) {
	event.ChannelID = strings.TrimSpace(event.ChannelID)
	event.FromUID = strings.TrimSpace(event.FromUID)
	event.ClientMsgNo = strings.TrimSpace(event.ClientMsgNo)
	event.RunID = strings.TrimSpace(event.RunID)
	event.AuthorizationFence = strings.TrimSpace(event.AuthorizationFence)
	event.EventID = strings.TrimSpace(event.EventID)
	event.EventKey = strings.TrimSpace(event.EventKey)
	event.EventType = strings.ToLower(strings.TrimSpace(event.EventType))
	event.Visibility = strings.TrimSpace(event.Visibility)
	event.Payload = cloneBytes(event.Payload)

	if event.ChannelID == "" {
		return MessageEventAppend{}, ErrMessageEventChannelIDRequired
	}
	if event.ChannelType <= 0 {
		return MessageEventAppend{}, ErrMessageEventChannelTypeRequired
	}
	if event.FromUID == "" {
		return MessageEventAppend{}, ErrMessageEventFromUIDRequired
	}
	if event.MessageID == 0 {
		return MessageEventAppend{}, ErrMessageEventMessageIDRequired
	}
	if event.ClientMsgNo == "" {
		return MessageEventAppend{}, ErrMessageEventClientMsgNoRequired
	}
	if event.RunID == "" {
		return MessageEventAppend{}, ErrMessageEventRunIDRequired
	}
	if event.AuthorizationFence == "" {
		return MessageEventAppend{}, ErrMessageEventAuthorizationFenceRequired
	}
	if _, err := strconv.ParseUint(event.AuthorizationFence, 10, 64); err != nil {
		return MessageEventAppend{}, ErrMessageEventAuthorizationFenceRequired
	}
	if event.EventID == "" {
		return MessageEventAppend{}, ErrMessageEventIDRequired
	}
	if event.EventType == "" {
		return MessageEventAppend{}, ErrMessageEventTypeRequired
	}
	if event.AuthoritySequence == 0 {
		return MessageEventAppend{}, ErrMessageEventAuthoritySequenceRequired
	}
	if event.Visibility == "" {
		event.Visibility = VisibilityPublic
	}
	if event.Visibility != VisibilityPublic {
		return MessageEventAppend{}, ErrMessageEventPublicOnly
	}
	if event.EventKey == "" {
		event.EventKey = EventKeyDefault
	}
	if event.EventType == EventTypeDelta && len(event.Payload) > 16*1024 {
		return MessageEventAppend{}, ErrMessageEventDeltaTooLarge
	}
	if (event.EventType == EventTypeSnapshot || event.EventType == EventTypeFinish) && messageEventSnapshotBytes(event.Payload) > 256*1024 {
		return MessageEventAppend{}, ErrMessageEventSnapshotTooLarge
	}
	channelID, err := normalizeMessageEventChannelID(event)
	if err != nil {
		return MessageEventAppend{}, err
	}
	event.ChannelID = channelID
	return event, nil
}

type agentRunMessageEventPayload struct {
	EventID            string          `json:"event_id"`
	RunID              string          `json:"run_id"`
	EventKey           string          `json:"event_key"`
	EventType          string          `json:"event_type"`
	AuthoritySequence  uint64          `json:"authority_sequence"`
	AuthorizationFence uint64          `json:"authorization_fence"`
	OccurredAt         string          `json:"occurred_at"`
	TextDelta          *string         `json:"text_delta"`
	Snapshot           json.RawMessage `json:"snapshot"`
	BaseMessage        struct {
		ClientMsgID       string `json:"client_msg_id"`
		SourcePrincipalID string `json:"source_principal_id"`
		CommittedRef      string `json:"committed_message_ref"`
		ConversationID    string `json:"conversation_id"`
		MessageID         string `json:"message_id"`
		MessageSequence   uint64 `json:"message_sequence"`
	} `json:"base_message"`
}

type agentRunMessageEventSnapshot struct {
	State             string `json:"state"`
	AuthoritySequence uint64 `json:"authority_sequence"`
	Text              string `json:"text"`
	Complete          bool   `json:"complete"`
	PublicErrorCode   string `json:"public_error_code"`
}

func validateAgentRunMessageEventPayload(event MessageEventAppend) error {
	var payload agentRunMessageEventPayload
	if len(event.Payload) == 0 {
		return ErrMessageEventPayloadRequired
	}
	if json.Unmarshal(event.Payload, &payload) != nil {
		return ErrMessageEventAnchorMismatch
	}
	fence, err := strconv.ParseUint(event.AuthorizationFence, 10, 64)
	if err != nil || fence == 0 {
		return ErrMessageEventAuthorizationFenceRequired
	}
	if payload.EventID != event.EventID || payload.RunID != event.RunID ||
		payload.EventKey != event.EventKey || payload.EventType != event.EventType ||
		payload.AuthoritySequence != event.AuthoritySequence || payload.AuthorizationFence != fence ||
		payload.BaseMessage.ClientMsgID != event.ClientMsgNo || payload.BaseMessage.SourcePrincipalID != event.FromUID {
		return ErrMessageEventAnchorMismatch
	}
	if payload.BaseMessage.ConversationID == "" || payload.BaseMessage.MessageID == "" || payload.BaseMessage.SourcePrincipalID == "" ||
		payload.OccurredAt == "" ||
		payload.BaseMessage.CommittedRef == "" || payload.BaseMessage.MessageSequence == 0 {
		return ErrMessageEventAnchorMismatch
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, payload.OccurredAt)
	if err != nil || (event.OccurredAt != 0 && occurredAt.UnixMilli() != event.OccurredAt) {
		return ErrMessageEventAnchorMismatch
	}
	eventHasSnapshot := len(payload.Snapshot) > 0 && string(payload.Snapshot) != "null"
	if event.EventType == EventTypeDelta {
		if payload.TextDelta == nil || *payload.TextDelta == "" || eventHasSnapshot {
			return ErrMessageEventAnchorMismatch
		}
		return nil
	}
	if payload.TextDelta != nil || !eventHasSnapshot {
		return ErrMessageEventAnchorMismatch
	}
	var snapshot agentRunMessageEventSnapshot
	if json.Unmarshal(payload.Snapshot, &snapshot) != nil || snapshot.State == "" ||
		snapshot.AuthoritySequence != event.AuthoritySequence {
		return ErrMessageEventAnchorMismatch
	}
	terminal := snapshot.State == "succeeded" || snapshot.State == "failed" || snapshot.State == "cancelled" || snapshot.State == "timed_out"
	if snapshot.Complete != terminal || (event.EventType == EventTypeFinish && !terminal) ||
		(snapshot.State == "failed" && snapshot.PublicErrorCode == "") {
		return ErrMessageEventAnchorMismatch
	}
	return nil
}

func messageEventSnapshotBytes(payload []byte) int {
	var body struct {
		Snapshot json.RawMessage `json:"snapshot"`
	}
	if json.Unmarshal(payload, &body) == nil && len(body.Snapshot) > 0 && string(body.Snapshot) != "null" {
		return len(body.Snapshot)
	}
	return len(payload)
}

func normalizeMessageEventChannelID(event MessageEventAppend) (string, error) {
	switch event.ChannelType {
	case int64(channelTypePerson):
		return runtimechannelid.NormalizePersonChannel(event.FromUID, event.ChannelID)
	case int64(channelTypeAgent):
		if strings.Contains(event.ChannelID, "@") {
			uid, agentUID, err := runtimechannelid.DecodeAgentChannel(event.ChannelID)
			if err != nil {
				return "", err
			}
			if event.FromUID != "" && uid != event.FromUID {
				return "", runtimechannelid.ErrInvalidAgentChannel
			}
			return runtimechannelid.EncodeAgentChannel(uid, agentUID), nil
		}
		if event.FromUID == "" {
			return "", runtimechannelid.ErrInvalidAgentChannel
		}
		return runtimechannelid.EncodeAgentChannel(event.FromUID, event.ChannelID), nil
	default:
		return event.ChannelID, nil
	}
}
