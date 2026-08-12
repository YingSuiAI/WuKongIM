package message

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/pkg/protocol/channelid"
)

func TestAppendMessageEventNormalizesPersonChannelAndClonesPayload(t *testing.T) {
	store := &recordingMessageEventStore{
		anchor: MessageEventAnchor{ChannelID: runtimechannelid.EncodePersonChannel("u1", "u2"), ChannelType: int64(channelTypePerson), FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1"},
		appendResult: MessageEventAppendResult{
			ChannelID:   runtimechannelid.EncodePersonChannel("u1", "u2"),
			ChannelType: int64(channelTypePerson),
			ClientMsgNo: "cmn-1",
			EventID:     "evt-1",
			EventKey:    EventKeyDefault,
			MsgEventSeq: 7,
			Status:      EventStatusOpen,
			State: MessageEventState{
				SnapshotPayload: []byte(`{"kind":"text","text":"hi"}`),
			},
		},
	}
	app := New(Options{
		EventStore: store,
		Now:        func() time.Time { return time.UnixMilli(1700000000123) },
	})
	payload := []byte(`{"event_id":"evt-1","run_id":"run-1","base_message":{"conversation_id":"019c0000-0000-7000-8000-000000000001","message_id":"019c0000-0000-7000-8000-000000000002","client_msg_id":"cmn-1","committed_message_ref":"agent-run.019c0000-0000-7000-8000-000000000003","message_sequence":1,"source_principal_id":"u1"},"event_key":"main","event_type":"delta","authority_sequence":1,"text_delta":"hi","authorization_fence":1,"occurred_at":"2023-11-14T22:13:20Z"}`)

	result, err := app.AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID:   " u2 ",
		ChannelType: int64(channelTypePerson),
		FromUID:     " u1 ",
		MessageID:   99,
		ClientMsgNo: " cmn-1 ",
		RunID:       " run-1 ", AuthorizationFence: " 1 ", AuthoritySequence: 1,
		EventID:   " evt-1 ",
		EventType: " delta ",
		Payload:   payload,
	})

	if err != nil {
		t.Fatalf("AppendMessageEvent() error = %v", err)
	}
	if len(store.appendCalls) != 1 {
		t.Fatalf("append calls = %#v, want one call", store.appendCalls)
	}
	call := store.appendCalls[0]
	if call.ChannelID != runtimechannelid.EncodePersonChannel("u1", "u2") || call.ChannelType != int64(channelTypePerson) || call.FromUID != "u1" {
		t.Fatalf("append call channel = %#v, want normalized person channel", call)
	}
	if call.ClientMsgNo != "cmn-1" || call.RunID != "run-1" || call.AuthorizationFence != "1" || call.EventID != "evt-1" || call.EventKey != "main" || call.EventType != EventTypeDelta {
		t.Fatalf("append call event fields = %#v, want normalized event fields", call)
	}
	if call.UpdatedAt != 1700000000123 {
		t.Fatalf("UpdatedAt = %d, want fixed now", call.UpdatedAt)
	}
	payload[0] = '['
	if !json.Valid(call.Payload) {
		t.Fatalf("stored payload mutated to %q", call.Payload)
	}
	result.State.SnapshotPayload[0] = '['
	if string(store.appendResult.State.SnapshotPayload) != `{"kind":"text","text":"hi"}` {
		t.Fatalf("result state was not cloned")
	}
	if result.FromUID != "u1" || result.MessageID != 99 {
		t.Fatalf("result sender/message = %q/%d, want u1/99", result.FromUID, result.MessageID)
	}
}

func TestAppendMessageEventNormalizesAgentChannel(t *testing.T) {
	store := &recordingMessageEventStore{anchor: MessageEventAnchor{ChannelID: runtimechannelid.EncodeAgentChannel("u1", "agent-a"), ChannelType: int64(channelTypeAgent), FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1"}, appendResult: MessageEventAppendResult{Status: EventStatusOpen}}
	app := New(Options{EventStore: store})

	_, err := app.AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID:   "agent-a",
		ChannelType: int64(channelTypeAgent),
		FromUID:     "u1",
		MessageID:   99,
		ClientMsgNo: "cmn-1",
		RunID:       "run-1", AuthorizationFence: "1", AuthoritySequence: 1,
		EventID:   "evt-1",
		EventType: EventTypeOpen,
		Payload:   validAgentRunEventPayload("evt-1", "run-1", "cmn-1", "u1", EventTypeOpen, EventKeyDefault, 1),
	})

	if err != nil {
		t.Fatalf("AppendMessageEvent() error = %v", err)
	}
	if got, want := store.appendCalls[0].ChannelID, runtimechannelid.EncodeAgentChannel("u1", "agent-a"); got != want {
		t.Fatalf("agent channel = %q, want %q", got, want)
	}
}

func TestAppendMessageEventKeepsRunAndLaneKeysIndependent(t *testing.T) {
	store := &recordingMessageEventStore{anchor: MessageEventAnchor{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "run:1", AuthorizationFence: "1"}}
	app := New(Options{EventStore: store})
	_, err := app.AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1",
		RunID: "run:1", AuthorizationFence: "1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "tool:search", EventType: EventTypeDelta,
		Payload: validAgentRunEventPayload("evt-1", "run:1", "cmn-1", "u1", EventTypeDelta, "tool:search", 1),
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent() error = %v", err)
	}
	if got := store.appendCalls[0]; got.RunID != "run:1" || got.EventKey != "tool:search" {
		t.Fatalf("append keys = run %q lane %q, want independent original values", got.RunID, got.EventKey)
	}
}

func validAgentRunEventPayload(eventID, runID, clientMsgNo, fromUID, eventType, eventKey string, sequence uint64) []byte {
	if eventKey == "" {
		eventKey = EventKeyDefault
	}
	body := map[string]any{
		"event_id": eventID, "run_id": runID,
		"base_message": map[string]any{
			"conversation_id": "019c0000-0000-7000-8000-000000000001",
			"message_id":      "019c0000-0000-7000-8000-000000000002",
			"client_msg_id":   clientMsgNo, "committed_message_ref": "agent-run.test",
			"message_sequence": 1, "source_principal_id": fromUID,
		},
		"event_key": eventKey, "event_type": eventType, "authority_sequence": sequence,
		"authorization_fence": 1, "occurred_at": "2023-11-14T22:13:20Z",
	}
	if eventType == EventTypeDelta {
		body["text_delta"] = "hi"
	} else {
		body["snapshot"] = map[string]any{
			"state": "running", "authority_sequence": sequence, "text": "", "complete": false,
		}
	}
	out, _ := json.Marshal(body)
	return out
}

func TestAppendMessageEventRejectsMismatchedEncodedAgentChannel(t *testing.T) {
	app := New(Options{EventStore: &recordingMessageEventStore{}})

	_, err := app.AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID:   runtimechannelid.EncodeAgentChannel("u2", "agent-a"),
		ChannelType: int64(channelTypeAgent),
		FromUID:     "u1",
		MessageID:   99,
		ClientMsgNo: "cmn-1",
		RunID:       "run-1", AuthorizationFence: "1", AuthoritySequence: 1,
		EventID:   "evt-1",
		EventType: EventTypeOpen,
	})

	if !errors.Is(err, runtimechannelid.ErrInvalidAgentChannel) {
		t.Fatalf("error = %v, want invalid agent channel", err)
	}
}

func TestAppendMessageEventRejectsMissingFields(t *testing.T) {
	app := New(Options{EventStore: &recordingMessageEventStore{}})
	cases := []struct {
		name  string
		event MessageEventAppend
		err   error
	}{
		{name: "channel id", event: MessageEventAppend{ChannelType: 2, ClientMsgNo: "cmn", EventID: "evt", EventType: EventTypeOpen}, err: ErrMessageEventChannelIDRequired},
		{name: "channel type", event: MessageEventAppend{ChannelID: "g1", ClientMsgNo: "cmn", EventID: "evt", EventType: EventTypeOpen}, err: ErrMessageEventChannelTypeRequired},
		{name: "client msg no", event: MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, EventID: "evt", EventType: EventTypeOpen}, err: ErrMessageEventClientMsgNoRequired},
		{name: "run id", event: MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn", EventID: "evt", EventType: EventTypeOpen}, err: ErrMessageEventRunIDRequired},
		{name: "fence", event: MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn", RunID: "run", EventID: "evt", EventType: EventTypeOpen}, err: ErrMessageEventAuthorizationFenceRequired},
		{name: "event id", event: MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn", RunID: "run", AuthorizationFence: "1", AuthoritySequence: 1, EventType: EventTypeOpen}, err: ErrMessageEventIDRequired},
		{name: "event type", event: MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn", RunID: "run", AuthorizationFence: "1", AuthoritySequence: 1, EventID: "evt"}, err: ErrMessageEventTypeRequired},
		{name: "authority sequence", event: MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn", RunID: "run", AuthorizationFence: "1", EventID: "evt", EventType: EventTypeOpen}, err: ErrMessageEventAuthoritySequenceRequired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := app.AppendMessageEvent(context.Background(), tc.event)
			if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want %v", err, tc.err)
			}
		})
	}
}

func TestAppendMessageEventRequiresStore(t *testing.T) {
	app := New(Options{})

	_, err := app.AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID:   "g1",
		ChannelType: 2,
		FromUID:     "u1",
		MessageID:   99,
		ClientMsgNo: "cmn-1",
		RunID:       "run-1", AuthorizationFence: "1", AuthoritySequence: 1,
		EventID:   "evt-1",
		EventType: EventTypeOpen,
	})

	if !errors.Is(err, ErrMessageEventStoreRequired) {
		t.Fatalf("error = %v, want store required", err)
	}
}

func TestAppendMessageEventRequiresTypedPayload(t *testing.T) {
	store := &recordingMessageEventStore{anchor: MessageEventAnchor{
		ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99,
		ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1",
	}}
	_, err := New(Options{EventStore: store}).AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99,
		ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1",
		AuthoritySequence: 1, EventID: "evt-1", EventType: EventTypeOpen,
	})
	if !errors.Is(err, ErrMessageEventPayloadRequired) {
		t.Fatalf("error = %v, want payload required", err)
	}
	if len(store.appendCalls) != 0 {
		t.Fatalf("append calls = %d, want zero", len(store.appendCalls))
	}
}

func TestAppendMessageEventAcceptsFailedFinishErrorInsideSnapshot(t *testing.T) {
	store := &recordingMessageEventStore{anchor: MessageEventAnchor{
		ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99,
		ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1",
	}}
	payload := validAgentRunEventPayload("evt-failed", "run-1", "cmn-1", "u1", EventTypeFinish, "main", 1)
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatal(err)
	}
	snapshot := body["snapshot"].(map[string]any)
	snapshot["state"] = "failed"
	snapshot["complete"] = true
	snapshot["public_error_code"] = "model_unavailable"
	payload, _ = json.Marshal(body)

	_, err := New(Options{EventStore: store}).AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99,
		ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1",
		AuthoritySequence: 1, EventID: "evt-failed", EventKey: "main", EventType: EventTypeFinish,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(failed finish) error = %v", err)
	}
	if len(store.appendCalls) != 1 {
		t.Fatalf("append calls = %d, want one", len(store.appendCalls))
	}
}

func TestAppendMessageEventLimitsUTF8PayloadByBytes(t *testing.T) {
	base := MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99,
		ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1",
		AuthoritySequence: 1, EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
	}
	base.Payload = []byte(strings.Repeat("界", 5461))
	if _, err := normalizeMessageEventAppendCommand(base); err != nil {
		t.Fatalf("16383-byte payload unexpectedly rejected: %v", err)
	}
	base.Payload = []byte(strings.Repeat("界", 5462))
	if _, err := normalizeMessageEventAppendCommand(base); !errors.Is(err, ErrMessageEventDeltaTooLarge) {
		t.Fatalf("16386-byte payload error = %v, want byte limit", err)
	}

	base.EventType = EventTypeSnapshot
	base.Payload = []byte(strings.Repeat("界", (256*1024)/3))
	if _, err := normalizeMessageEventAppendCommand(base); err != nil {
		t.Fatalf("snapshot below 256 KiB unexpectedly rejected: %v", err)
	}
	base.Payload = []byte(strings.Repeat("界", (256*1024)/3+1))
	if _, err := normalizeMessageEventAppendCommand(base); !errors.Is(err, ErrMessageEventSnapshotTooLarge) {
		t.Fatalf("snapshot above 256 KiB error = %v, want byte limit", err)
	}
}

func TestAppendMessageEventRequiresExactCommittedPublicAnchor(t *testing.T) {
	base := MessageEventAppend{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1", AuthoritySequence: 1, EventID: "evt-1", EventType: EventTypeOpen, Visibility: VisibilityPublic}
	tests := []struct {
		name  string
		store *recordingMessageEventStore
		event MessageEventAppend
		want  error
	}{
		{name: "missing", store: &recordingMessageEventStore{anchorMissing: true}, event: base, want: ErrMessageEventTargetNotFound},
		{name: "sender mismatch", store: &recordingMessageEventStore{anchorFound: true, anchor: MessageEventAnchor{ChannelID: "g1", ChannelType: 2, FromUID: "other", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1"}}, event: base, want: ErrMessageEventAnchorMismatch},
		{name: "client number mismatch", store: &recordingMessageEventStore{anchorFound: true, anchor: MessageEventAnchor{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "other"}}, event: base, want: ErrMessageEventAnchorMismatch},
		{name: "run mismatch", store: &recordingMessageEventStore{anchorFound: true, anchor: MessageEventAnchor{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "other", AuthorizationFence: "1"}}, event: base, want: ErrMessageEventAnchorMismatch},
		{name: "fence mismatch", store: &recordingMessageEventStore{anchorFound: true, anchor: MessageEventAnchor{ChannelID: "g1", ChannelType: 2, FromUID: "u1", MessageID: 99, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "other"}}, event: base, want: ErrMessageEventAnchorMismatch},
		{name: "private", store: &recordingMessageEventStore{}, event: func() MessageEventAppend { value := base; value.Visibility = VisibilityPrivate; return value }(), want: ErrMessageEventPublicOnly},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(Options{EventStore: test.store}).AppendMessageEvent(context.Background(), test.event)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if len(test.store.appendCalls) != 0 {
				t.Fatalf("append calls = %d, want zero", len(test.store.appendCalls))
			}
		})
	}
}

type recordingMessageEventStore struct {
	anchor        MessageEventAnchor
	anchorFound   bool
	anchorMissing bool
	anchorErr     error
	appendCalls   []MessageEventAppend
	appendResult  MessageEventAppendResult
	appendErr     error

	stateCalls []messageEventStateCall
	stateRows  map[MessageEventMessageKey][]MessageEventState
	stateErr   error
}

func (r *recordingMessageEventStore) LookupMessageEventAnchor(_ context.Context, channelID string, channelType int64, messageID uint64) (MessageEventAnchor, bool, error) {
	if r.anchorErr != nil {
		return MessageEventAnchor{}, false, r.anchorErr
	}
	if r.anchorMissing {
		return MessageEventAnchor{}, false, nil
	}
	anchor := r.anchor
	if anchor.MessageID == 0 && !r.anchorFound {
		anchor = MessageEventAnchor{ChannelID: channelID, ChannelType: channelType, FromUID: "u1", MessageID: messageID, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1"}
	}
	return anchor, r.anchorFound || anchor.MessageID != 0, nil
}

type messageEventStateCall struct {
	keys  []MessageEventMessageKey
	limit int
}

func (r *recordingMessageEventStore) AppendMessageEvent(_ context.Context, event MessageEventAppend) (MessageEventAppendResult, error) {
	r.appendCalls = append(r.appendCalls, event)
	return r.appendResult, r.appendErr
}

func (r *recordingMessageEventStore) GetMessageEventStatesBatch(_ context.Context, keys []MessageEventMessageKey, limit int) (map[MessageEventMessageKey][]MessageEventState, error) {
	cp := append([]MessageEventMessageKey(nil), keys...)
	r.stateCalls = append(r.stateCalls, messageEventStateCall{keys: cp, limit: limit})
	if r.stateErr != nil {
		return nil, r.stateErr
	}
	out := make(map[MessageEventMessageKey][]MessageEventState, len(r.stateRows))
	for key, rows := range r.stateRows {
		out[key] = cloneMessageEventStates(rows)
	}
	return out, nil
}
