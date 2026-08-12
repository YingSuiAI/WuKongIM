package fsm

import (
	"context"
	"strings"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestStateMachineAppliesMessageEventAppend(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	resultBytes, err := sm.Apply(ctx, multiraft.Command{
		SlotID:   11,
		Index:    1,
		Term:     1,
		HashSlot: 11,
		Data: EncodeAppendMessageEventCommand(metadb.MessageEventAppend{
			ChannelID:          "g1",
			ChannelType:        2,
			ClientMsgNo:        "cmn-1",
			RunID:              "run-1",
			AuthorizationFence: "fence-1",
			AuthoritySequence:  1,
			EventID:            "evt-1",
			EventKey:           "main",
			EventType:          metadb.EventTypeDelta,
			Visibility:         metadb.VisibilityPublic,
			OccurredAt:         1000,
			Payload:            []byte(`{"kind":"text","delta":"hi"}`),
			UpdatedAt:          1001,
		}),
	})
	if err != nil {
		t.Fatalf("Apply(message event) error = %v", err)
	}
	result, err := DecodeAppendMessageEventResult(resultBytes)
	if err != nil {
		t.Fatalf("DecodeAppendMessageEventResult() error = %v", err)
	}
	if result.MsgEventSeq != 1 || result.Status != metadb.EventStatusOpen || result.EventKey != "main" {
		t.Fatalf("result = %#v, want seq=1 open main", result)
	}
	if !strings.Contains(string(result.State.SnapshotPayload), "hi") {
		t.Fatalf("snapshot payload = %s, want text delta applied", result.State.SnapshotPayload)
	}

	states, err := db.ForHashSlot(11).ListMessageEventStates(ctx, "g1", 2, "cmn-1", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates() error = %v", err)
	}
	if len(states) != 1 || states[0].LastMsgEventSeq != 1 || states[0].LastEventID != "evt-1" {
		t.Fatalf("states = %#v, want one persisted event state", states)
	}
}

func TestStateMachineMessageEventAppendNormalizesOpenAndDefaultKey(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	resultBytes, err := sm.Apply(ctx, multiraft.Command{
		SlotID:   11,
		Index:    1,
		Term:     1,
		HashSlot: 11,
		Data: EncodeAppendMessageEventCommand(metadb.MessageEventAppend{
			ChannelID:          "g1",
			ChannelType:        2,
			ClientMsgNo:        "cmn-open",
			RunID:              "run-1",
			AuthorizationFence: "fence-1",
			AuthoritySequence:  1,
			EventID:            "evt-open",
			EventKey:           "main",
			EventType:          metadb.EventTypeOpen,
			OccurredAt:         1000,
			UpdatedAt:          1001,
		}),
	})
	if err != nil {
		t.Fatalf("Apply(message event open) error = %v", err)
	}
	result, err := DecodeAppendMessageEventResult(resultBytes)
	if err != nil {
		t.Fatalf("DecodeAppendMessageEventResult() error = %v", err)
	}
	if result.EventKey != "main" || result.Status != metadb.EventStatusOpen || result.MsgEventSeq != 1 {
		t.Fatalf("result = %#v, want default key open seq=1", result)
	}
	if result.State.LastVisibility != metadb.VisibilityPublic {
		t.Fatalf("visibility = %q, want public default", result.State.LastVisibility)
	}
}

func TestStateMachineAppliesMessageEventAppendBatch(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	resultBytes, err := sm.Apply(ctx, multiraft.Command{
		SlotID:   11,
		Index:    1,
		Term:     1,
		HashSlot: 11,
		Data: EncodeAppendMessageEventsCommand([]metadb.MessageEventAppend{
			{
				ChannelID:          "g1",
				ChannelType:        2,
				ClientMsgNo:        "cmn-batch",
				RunID:              "run-1",
				AuthorizationFence: "fence-1",
				AuthoritySequence:  1,
				EventID:            "evt-close-main",
				EventKey:           "main",
				EventType:          metadb.EventTypeFinish,
				Visibility:         metadb.VisibilityPublic,
				OccurredAt:         1000,
				Payload:            []byte(`{"snapshot":{"kind":"text","text":"hi"}}`),
				UpdatedAt:          1001,
			},
			{
				ChannelID:          "g1",
				ChannelType:        2,
				ClientMsgNo:        "cmn-batch",
				RunID:              "run-2",
				AuthorizationFence: "fence-1",
				AuthoritySequence:  1,
				EventID:            "evt-finish",
				EventKey:           "main",
				EventType:          metadb.EventTypeFinish,
				Visibility:         metadb.VisibilityPublic,
				OccurredAt:         1002,
				UpdatedAt:          1003,
			},
		}),
	})
	if err != nil {
		t.Fatalf("Apply(message event batch) error = %v", err)
	}
	result, err := DecodeAppendMessageEventResult(resultBytes)
	if err != nil {
		t.Fatalf("DecodeAppendMessageEventResult() error = %v", err)
	}
	if result.EventKey != "main" || result.MsgEventSeq != 1 || result.Status != metadb.EventStatusClosed {
		t.Fatalf("batch result = %#v, want second run finish seq=1 closed", result)
	}
	states, err := db.ForHashSlot(11).ListMessageEventStates(ctx, "g1", 2, "cmn-batch", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates() error = %v", err)
	}
	byKey := make(map[string]metadb.MessageEventState, len(states))
	for _, state := range states {
		byKey[state.RunID+"/"+state.EventKey] = state
	}
	if byKey["run-1/main"].LastMsgEventSeq != 1 || byKey["run-1/main"].Status != metadb.EventStatusClosed {
		t.Fatalf("first state = %#v, want closed seq=1", byKey["run-1/main"])
	}
	if byKey["run-2/main"].LastMsgEventSeq != 1 || byKey["run-2/main"].Status != metadb.EventStatusClosed {
		t.Fatalf("second state = %#v, want closed seq=1", byKey["run-2/main"])
	}
}

func TestStateMachineAppliesMessageEventAppendBatchAcrossMessages(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	resultBytes, err := sm.Apply(ctx, multiraft.Command{
		SlotID:   11,
		Index:    1,
		Term:     1,
		HashSlot: 11,
		Data: EncodeAppendMessageEventsCommand([]metadb.MessageEventAppend{
			{
				ChannelID:          "g1",
				ChannelType:        2,
				ClientMsgNo:        "cmn-a",
				RunID:              "run-a",
				AuthorizationFence: "fence-1",
				AuthoritySequence:  1,
				EventID:            "evt-finish-a",
				EventKey:           "run-a:main",
				EventType:          metadb.EventTypeFinish,
				Visibility:         metadb.VisibilityPublic,
				OccurredAt:         1000,
				Payload:            []byte(`{"snapshot":{"kind":"text","text":"a"}}`),
				UpdatedAt:          1001,
			},
			{
				ChannelID:          "g1",
				ChannelType:        2,
				ClientMsgNo:        "cmn-b",
				RunID:              "run-b",
				AuthorizationFence: "fence-1",
				AuthoritySequence:  1,
				EventID:            "evt-finish-b",
				EventKey:           "run-b:main",
				EventType:          metadb.EventTypeFinish,
				Visibility:         metadb.VisibilityPublic,
				OccurredAt:         1002,
				Payload:            []byte(`{"snapshot":{"kind":"text","text":"b"}}`),
				UpdatedAt:          1003,
			},
		}),
	})
	if err != nil {
		t.Fatalf("Apply(message event mixed-message batch) error = %v", err)
	}
	results, err := DecodeAppendMessageEventResults(resultBytes)
	if err != nil {
		t.Fatalf("DecodeAppendMessageEventResults() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("batch results = %#v, want two per-message results", results)
	}
	if results[0].ClientMsgNo != "cmn-a" || results[0].EventID != "evt-finish-a" || results[0].MsgEventSeq != 1 {
		t.Fatalf("first result = %#v, want cmn-a finish seq=1", results[0])
	}
	if results[1].ClientMsgNo != "cmn-b" || results[1].EventID != "evt-finish-b" || results[1].MsgEventSeq != 1 {
		t.Fatalf("second result = %#v, want cmn-b finish seq=1", results[1])
	}
	for _, clientMsgNo := range []string{"cmn-a", "cmn-b"} {
		states, err := db.ForHashSlot(11).ListMessageEventStates(ctx, "g1", 2, clientMsgNo, 10)
		if err != nil {
			t.Fatalf("ListMessageEventStates(%s) error = %v", clientMsgNo, err)
		}
		if len(states) != 1 || states[0].LastEventType != metadb.EventTypeFinish || states[0].LastMsgEventSeq != 1 {
			t.Fatalf("states for %s = %#v, want one finish marker at seq=1", clientMsgNo, states)
		}
	}
}

func TestEncodeAppendMessageEventCommandCheckedRejectsInvalidEvent(t *testing.T) {
	_, err := EncodeAppendMessageEventCommandChecked(metadb.MessageEventAppend{
		ChannelID:   "g1",
		ChannelType: 0,
		ClientMsgNo: "cmn-1",
		EventID:     "evt-1",
		EventType:   metadb.EventTypeDelta,
	})
	if err == nil {
		t.Fatal("EncodeAppendMessageEventCommandChecked() error = nil, want invalid argument")
	}
}

func TestSingleMessageEventCommandRejectsProjectionOnly(t *testing.T) {
	event := metadb.MessageEventAppend{
		ChannelID:          "g1",
		ChannelType:        2,
		ClientMsgNo:        "cmn-1",
		RunID:              "run-1",
		AuthoritySequence:  1,
		EventID:            "projection-main",
		EventKey:           "main",
		EventType:          metadb.EventTypeFinish,
		ProjectionOnly:     true,
		AuthorizationFence: "fence-1",
	}
	if _, err := EncodeAppendMessageEventCommandChecked(event); err == nil {
		t.Fatal("checked single command accepted projection-only event")
	}
	if _, err := decodeCommand(EncodeAppendMessageEventCommand(event)); err == nil {
		t.Fatal("decoded single command accepted projection-only event")
	}
}

func TestMessageEventBatchPreservesProjectionOnly(t *testing.T) {
	events := []metadb.MessageEventAppend{
		{
			ChannelID:          "g1",
			ChannelType:        2,
			ClientMsgNo:        "cmn-1",
			RunID:              "run-1",
			AuthoritySequence:  3,
			EventID:            "projection-tool",
			EventKey:           "tool",
			EventType:          metadb.EventTypeFinish,
			ProjectionOnly:     true,
			AuthorizationFence: "fence-1",
		},
		{
			ChannelID:          "g1",
			ChannelType:        2,
			ClientMsgNo:        "cmn-1",
			RunID:              "run-1",
			AuthoritySequence:  7,
			EventID:            "finish-main",
			EventKey:           "main",
			EventType:          metadb.EventTypeFinish,
			AuthorizationFence: "fence-1",
		},
	}
	encoded, err := EncodeAppendMessageEventsCommandChecked(events)
	if err != nil {
		t.Fatalf("EncodeAppendMessageEventsCommandChecked() error = %v", err)
	}
	cmd, err := decodeCommand(encoded)
	if err != nil {
		t.Fatalf("decodeCommand() error = %v", err)
	}
	batch := cmd.(*appendMessageEventsBatchCmd)
	if !batch.events[0].ProjectionOnly || batch.events[1].ProjectionOnly {
		t.Fatalf("decoded projection flags = %v/%v, want true/false", batch.events[0].ProjectionOnly, batch.events[1].ProjectionOnly)
	}
}

func TestStateMachineReplaysFinishBatchIdempotently(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	events := []metadb.MessageEventAppend{
		{
			ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-replay", RunID: "run-1",
			AuthorizationFence: "1", AuthoritySequence: 101, MsgEventSeq: 2,
			EventID: "finish/flush/tool", EventKey: "tool", EventType: metadb.EventTypeSnapshot,
			ProjectionOnly: true, Visibility: metadb.VisibilityPublic, Payload: []byte(`{"text":"tool"}`),
		},
		{
			ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-replay", RunID: "run-1",
			AuthorizationFence: "1", AuthoritySequence: 102, MsgEventSeq: 3,
			EventID: "finish", EventKey: "main", EventType: metadb.EventTypeFinish,
			Visibility: metadb.VisibilityPublic, Payload: []byte(`{"snapshot":{"text":"done"}}`),
		},
	}
	command := multiraft.Command{SlotID: 11, Index: 1, Term: 1, HashSlot: 11, Data: EncodeAppendMessageEventsCommand(events)}
	firstBytes, err := sm.Apply(ctx, command)
	if err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	command.Index = 2
	secondBytes, err := sm.Apply(ctx, command)
	if err != nil {
		t.Fatalf("replay Apply() error = %v", err)
	}
	first, err := DecodeAppendMessageEventResults(firstBytes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DecodeAppendMessageEventResults(secondBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || len(second) != 2 || second[1].Applied || second[1].MsgEventSeq != first[1].MsgEventSeq {
		t.Fatalf("first=%+v replay=%+v, want idempotent finish replay", first, second)
	}
	cursor, found, err := db.ForHashSlot(11).GetMessageEventCursor(ctx, "g1", 2, "cmn-replay", "run-1")
	if err != nil || !found || cursor.LastMsgEventSeq != 3 || cursor.LastAuthoritySequence != 102 || !cursor.Terminal {
		t.Fatalf("cursor=%+v found=%v err=%v", cursor, found, err)
	}
}

func TestEncodeAppendMessageEventsCommandCheckedRejectsMixedChannel(t *testing.T) {
	_, err := EncodeAppendMessageEventsCommandChecked([]metadb.MessageEventAppend{
		{
			ChannelID:   "g1",
			ChannelType: 2,
			ClientMsgNo: "cmn-1",
			EventID:     "evt-1",
			EventType:   metadb.EventTypeFinish,
		},
		{
			ChannelID:   "g2",
			ChannelType: 2,
			ClientMsgNo: "cmn-2",
			EventID:     "evt-2",
			EventType:   metadb.EventTypeFinish,
		},
	})
	if err == nil {
		t.Fatal("EncodeAppendMessageEventsCommandChecked() error = nil, want invalid argument for mixed channel keys")
	}
}
