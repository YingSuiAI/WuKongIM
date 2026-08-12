//go:build integration

package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
)

func TestMessageEventAppendTextLifecycle(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	first, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-text", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("hello", 1), OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(first): %v", err)
	}
	if first.MsgEventSeq != 1 || first.Status != EventStatusOpen {
		t.Fatalf("first result = %#v, want seq=1 status=open", first)
	}

	second, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-text", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
		EventID: "evt-2", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest(" world", 2), OccurredAt: 12, UpdatedAt: 13,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(second): %v", err)
	}
	if second.MsgEventSeq != 2 {
		t.Fatalf("second seq = %d, want 2", second.MsgEventSeq)
	}

	closed, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-text", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 3,
		EventID: "evt-3", EventKey: "main", EventType: EventTypeFinish,
		Payload: messageEventFinishPayloadForTest("hello world", 3), OccurredAt: 14, UpdatedAt: 15,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(close): %v", err)
	}
	if closed.MsgEventSeq != 3 || closed.State.Status != EventStatusClosed {
		t.Fatalf("closed result = %#v, want seq=3 closed", closed)
	}

	state, ok, err := shard.GetMessageEventState(ctx, "g1", 2, "cmn-text", "run-1", "main")
	if err != nil {
		t.Fatalf("GetMessageEventState(): %v", err)
	}
	if !ok {
		t.Fatal("GetMessageEventState(): missing state")
	}
	if state.Status != EventStatusClosed || state.LastMsgEventSeq != 3 {
		t.Fatalf("state = %#v, want closed seq=3", state)
	}
	if got, want := string(state.SnapshotPayload), string(messageEventSnapshotForTest("hello world", 3, true)); !jsonEqualForTest(got, want) {
		t.Fatalf("snapshot = %s, want %s", got, want)
	}
}

func TestMessageEventSeparatesAuthorityAndTransportSequences(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)
	for index, authority := range []uint64{100, 101} {
		result, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
			ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-dual-seq", RunID: "run-1",
			AuthorizationFence: "7", AuthoritySequence: authority,
			EventID: fmt.Sprintf("evt-%d", authority), EventKey: "main", EventType: EventTypeDelta,
			Payload: []byte(fmt.Sprintf(`{"text_delta":"%d","authority_sequence":%d}`, index+1, authority)),
		})
		if err != nil {
			t.Fatalf("AppendMessageEvent(%d): %v", authority, err)
		}
		if result.MsgEventSeq != uint64(index+1) {
			t.Fatalf("authority=%d msg_event_seq=%d, want %d", authority, result.MsgEventSeq, index+1)
		}
	}
	cursor, found, err := shard.GetMessageEventCursor(ctx, "g1", 2, "cmn-dual-seq", "run-1")
	if err != nil || !found {
		t.Fatalf("GetMessageEventCursor() found=%v err=%v", found, err)
	}
	if cursor.LastAuthoritySequence != 101 || cursor.LastMsgEventSeq != 2 {
		t.Fatalf("cursor=%+v, want authority=101 transport=2", cursor)
	}
}

func TestMessageEventAppendStreamOpenStartsDefaultLane(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	result, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-open", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-open", EventKey: "main", EventType: EventTypeOpen,
		OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(open): %v", err)
	}
	if result.MsgEventSeq != 1 || result.EventKey != "main" || result.Status != EventStatusOpen {
		t.Fatalf("open result = %#v, want seq=1 default open", result)
	}
	if result.State.LastVisibility != VisibilityPublic {
		t.Fatalf("open visibility = %q, want public default", result.State.LastVisibility)
	}
}

func TestMessageEventAppendIdempotentByLastEventID(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	first, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-idem", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("A", 1), OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(first): %v", err)
	}
	duplicate, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-idem", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("A", 1), OccurredAt: 10, UpdatedAt: 99,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(duplicate): %v", err)
	}
	if duplicate.MsgEventSeq != first.MsgEventSeq {
		t.Fatalf("duplicate seq = %d, want %d", duplicate.MsgEventSeq, first.MsgEventSeq)
	}
	if got, want := string(duplicate.State.SnapshotPayload), string(messageEventSnapshotForTest("A", 1, false)); !jsonEqualForTest(got, want) {
		t.Fatalf("duplicate snapshot = %s, want %s", got, want)
	}
}

func TestMessageEventAppendIdempotentAfterNewerEvent(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	first, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-idem-old", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("A", 1), UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(first): %v", err)
	}
	second, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-idem-old", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
		EventID: "evt-2", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("B", 2), UpdatedAt: 12,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(second): %v", err)
	}
	if second.MsgEventSeq != 2 {
		t.Fatalf("second seq = %d, want 2", second.MsgEventSeq)
	}
	duplicate, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-idem-old", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("A", 1), UpdatedAt: 99,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(duplicate old): %v", err)
	}
	if duplicate.MsgEventSeq != first.MsgEventSeq {
		t.Fatalf("duplicate old seq = %d, want original %d", duplicate.MsgEventSeq, first.MsgEventSeq)
	}
	if duplicate.State.LastMsgEventSeq != duplicate.MsgEventSeq || duplicate.State.Status != duplicate.Status {
		t.Fatalf("duplicate old result state = %#v, want seq/status consistent with result %#v", duplicate.State, duplicate)
	}
	state, ok, err := shard.GetMessageEventState(ctx, "g1", 2, "cmn-idem-old", "run-1", "main")
	if err != nil {
		t.Fatalf("GetMessageEventState(): %v", err)
	}
	if !ok {
		t.Fatal("GetMessageEventState(): missing state")
	}
	if state.LastMsgEventSeq != 2 {
		t.Fatalf("state seq = %d, want 2", state.LastMsgEventSeq)
	}
	if got, want := string(state.SnapshotPayload), string(messageEventSnapshotForTest("AB", 2, false)); !jsonEqualForTest(got, want) {
		t.Fatalf("snapshot = %s, want %s", got, want)
	}
}

func TestMessageEventAppendRejectsEventIDContentConflict(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)
	base := MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-conflict", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta, Visibility: VisibilityPublic,
		Payload: messageEventDeltaPayloadForTest("A", 1), OccurredAt: 10, UpdatedAt: 11,
	}
	first, err := shard.AppendMessageEvent(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := shard.AppendMessageEvent(ctx, base)
	if err != nil || duplicate.MsgEventSeq != first.MsgEventSeq {
		t.Fatalf("exact duplicate = %#v err=%v, want idempotent seq %d", duplicate, err, first.MsgEventSeq)
	}
	conflict := base
	conflict.Payload = messageEventDeltaPayloadForTest("B", 1)
	if _, err := shard.AppendMessageEvent(ctx, conflict); !errors.Is(err, dberrors.ErrConflict) {
		t.Fatalf("content conflict error = %v, want ErrConflict", err)
	}
	conflict = base
	conflict.OccurredAt++
	if _, err := shard.AppendMessageEvent(ctx, conflict); !errors.Is(err, dberrors.ErrConflict) {
		t.Fatalf("occurred_at conflict error = %v, want ErrConflict", err)
	}
}

func TestMessageEventCursorIsRunGlobalAcrossLanes(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	events := []MessageEventAppend{
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-runs", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1, EventID: "r1-main-1", EventKey: "main", EventType: EventTypeDelta},
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-runs", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2, EventID: "r1-tool-2", EventKey: "tool", EventType: EventTypeDelta},
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-runs", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 3, EventID: "r1-main-3", EventKey: "main", EventType: EventTypeDelta},
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-runs", RunID: "run-2", AuthorizationFence: "fence-1", AuthoritySequence: 1, EventID: "r2-main-1", EventKey: "main", EventType: EventTypeDelta},
	}
	for _, event := range events {
		if _, err := shard.AppendMessageEvent(ctx, event); err != nil {
			t.Fatalf("AppendMessageEvent(%s): %v", event.EventID, err)
		}
	}
	for runID, want := range map[string]uint64{"run-1": 3, "run-2": 1} {
		cursor, found, err := shard.GetMessageEventCursor(ctx, "g1", 2, "cmn-runs", runID)
		if err != nil || !found || cursor.LastMsgEventSeq != want {
			t.Fatalf("GetMessageEventCursor(%s) = %#v found=%v err=%v, want seq=%d", runID, cursor, found, err, want)
		}
	}
}

func TestMessageEventAppendTerminalRejectsLaterLane(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	closed, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-terminal", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-close", EventKey: "main", EventType: EventTypeFinish,
		Payload: messageEventFinishPayloadForTest("done", 1), OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(close): %v", err)
	}
	_, err = shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-terminal", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
		EventID: "evt-after-close", EventKey: "tool", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("late", 2), OccurredAt: 12, UpdatedAt: 13,
	})
	if !errors.Is(err, dberrors.ErrConflict) {
		t.Fatalf("AppendMessageEvent(delta) error = %v, want conflict after %#v", err, closed)
	}
}

func TestMessageEventAppendNormalizesEmptyEventKey(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	result, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-key", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("A", 1), OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(): %v", err)
	}
	if result.EventKey != "main" {
		t.Fatalf("event key = %q, want main", result.EventKey)
	}
	if _, ok, err := shard.GetMessageEventState(ctx, "g1", 2, "cmn-key", "run-1", "main"); err != nil {
		t.Fatalf("GetMessageEventState(default): %v", err)
	} else if !ok {
		t.Fatal("GetMessageEventState(default): missing state")
	}
}

func TestMessageEventAppendRejectsInvalidChannelType(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)

	_, err := store.db.HashSlot(4).AppendMessageEvent(context.Background(), MessageEventAppend{
		ChannelID: "g1", ChannelType: -1, ClientMsgNo: "cmn-invalid",
		EventID: "evt-1", EventType: EventTypeDelta,
	})
	if err == nil {
		t.Fatal("AppendMessageEvent() error = nil, want invalid argument")
	}
}

func TestMessageEventAppendTerminalKeepsTypedLaneKey(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	result, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-finish", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-finish", EventKey: "main", EventType: EventTypeFinish,
		Payload: []byte(`{"ok":true}`), OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(finish): %v", err)
	}
	if result.EventKey != "main" || result.Status != EventStatusClosed {
		t.Fatalf("finish result = %#v, want finish key and closed status", result)
	}
	state, ok, err := shard.GetMessageEventState(ctx, "g1", 2, "cmn-finish", "run-1", "main")
	if err != nil {
		t.Fatalf("GetMessageEventState(finish): %v", err)
	}
	if !ok {
		t.Fatal("GetMessageEventState(finish): missing state")
	}
	if state.RunID != "run-1" || state.EventKey != "main" || state.Status != EventStatusClosed {
		t.Fatalf("finish state = %#v, want finish key and closed status", state)
	}
}

func TestMessageEventAppendFailedFinishProjectsSnapshotErrorCode(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	result, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-failed", RunID: "run-1", AuthorizationFence: "1", AuthoritySequence: 1,
		EventID: "evt-failed", EventKey: "main", EventType: EventTypeFinish,
		Payload: []byte(`{"snapshot":{"state":"failed","complete":true,"text":"partial","authority_sequence":1,"public_error_code":"model_unavailable"}}`),
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(failed finish): %v", err)
	}
	if result.Status != EventStatusClosed || result.State.Error != "model_unavailable" {
		t.Fatalf("failed finish result = %#v, want closed with snapshot public_error_code", result)
	}
	if !jsonEqualForTest(string(result.State.SnapshotPayload), `{"state":"failed","complete":true,"text":"partial","authority_sequence":1,"public_error_code":"model_unavailable"}`) {
		t.Fatalf("failed finish snapshot = %s", result.State.SnapshotPayload)
	}
}

func TestMessageEventAppendBatchUsesPlatformAuthoritySequence(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()

	batch := store.db.NewBatch()
	first, err := batch.AppendMessageEvent(4, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-batch", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "left", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("A", 1), OccurredAt: 10, UpdatedAt: 11,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(first): %v", err)
	}
	second, err := batch.AppendMessageEvent(4, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-batch", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
		EventID: "evt-2", EventKey: "right", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("B", 2), OccurredAt: 12, UpdatedAt: 13,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(second): %v", err)
	}
	if first.MsgEventSeq != 1 || second.MsgEventSeq != 2 {
		t.Fatalf("batch seqs = %d,%d want 1,2", first.MsgEventSeq, second.MsgEventSeq)
	}
	if err := batch.Commit(ctx); err != nil {
		t.Fatalf("Commit(): %v", err)
	}
	states, err := store.db.HashSlot(4).ListMessageEventStates(ctx, "g1", 2, "cmn-batch", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates(): %v", err)
	}
	if len(states) != 2 || states[0].EventKey != "left" || states[1].EventKey != "right" {
		t.Fatalf("states = %#v, want left/right", states)
	}
}

func TestMessageEventAppendBatchDetectsExternalCursorDrift(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()

	batch := store.db.NewBatch()
	if _, err := batch.AppendMessageEvent(4, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-drift", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-batch", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("batch", 1), UpdatedAt: 10,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(batch): %v", err)
	}
	if _, err := store.db.HashSlot(4).AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-drift", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-direct", EventKey: "main", EventType: EventTypeDelta,
		Payload: messageEventDeltaPayloadForTest("direct", 1), UpdatedAt: 11,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(direct): %v", err)
	}
	if err := batch.Commit(ctx); !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("Commit() error = %v, want ErrStaleMeta", err)
	}
	state, ok, err := store.db.HashSlot(4).GetMessageEventState(ctx, "g1", 2, "cmn-drift", "run-1", "main")
	if err != nil {
		t.Fatalf("GetMessageEventState(): %v", err)
	}
	if !ok {
		t.Fatal("GetMessageEventState(): missing state")
	}
	if state.LastMsgEventSeq != 1 || state.LastEventID != "evt-direct" {
		t.Fatalf("state after conflict = %#v, want direct seq=1", state)
	}
}

func TestMessageEventAppendClonesPayloadAndReturnedState(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	payload := messageEventDeltaPayloadForTest("A", 1)
	result, err := shard.AppendMessageEvent(ctx, MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-clone", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: EventTypeDelta,
		Payload: payload, UpdatedAt: 10,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(): %v", err)
	}
	payload[0] = 'X'
	if len(result.State.SnapshotPayload) > 0 {
		result.State.SnapshotPayload[0] = 'X'
	}
	state, ok, err := shard.GetMessageEventState(ctx, "g1", 2, "cmn-clone", "run-1", "main")
	if err != nil {
		t.Fatalf("GetMessageEventState(): %v", err)
	}
	if !ok {
		t.Fatal("GetMessageEventState(): missing state")
	}
	if got, want := string(state.SnapshotPayload), string(messageEventSnapshotForTest("A", 1, false)); !jsonEqualForTest(got, want) {
		t.Fatalf("snapshot = %s, want %s", got, want)
	}
}

func TestMessageEventListStatesByClientMsgNo(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	ctx := context.Background()
	shard := store.db.HashSlot(4)

	events := []MessageEventAppend{
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-list", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1, EventID: "evt-a", EventKey: "a", EventType: EventTypeDelta, Payload: messageEventDeltaPayloadForTest("A", 1)},
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-list", RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2, EventID: "evt-b", EventKey: "b", EventType: EventTypeDelta, Payload: messageEventDeltaPayloadForTest("B", 2)},
		{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-other", RunID: "run-2", AuthorizationFence: "fence-1", AuthoritySequence: 1, EventID: "evt-c", EventKey: "c", EventType: EventTypeDelta, Payload: messageEventDeltaPayloadForTest("C", 1)},
	}
	for _, event := range events {
		if _, err := shard.AppendMessageEvent(ctx, event); err != nil {
			t.Fatalf("AppendMessageEvent(%s): %v", event.EventID, err)
		}
	}

	states, err := shard.ListMessageEventStates(ctx, "g1", 2, "cmn-list", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates(): %v", err)
	}
	if len(states) != 2 || states[0].EventKey != "a" || states[1].EventKey != "b" {
		t.Fatalf("states = %#v, want a/b only", states)
	}
}

func jsonEqualForTest(left, right string) bool {
	var leftValue any
	var rightValue any
	if err := json.Unmarshal([]byte(left), &leftValue); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(right), &rightValue); err != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func messageEventDeltaPayloadForTest(text string, authoritySequence uint64) []byte {
	return []byte(fmt.Sprintf(`{"text_delta":%q,"authority_sequence":%d}`, text, authoritySequence))
}

func messageEventSnapshotForTest(text string, authoritySequence uint64, complete bool) []byte {
	return []byte(fmt.Sprintf(`{"state":%q,"complete":%t,"text":%q,"authority_sequence":%d}`,
		map[bool]string{false: "running", true: "succeeded"}[complete], complete, text, authoritySequence))
}

func messageEventFinishPayloadForTest(text string, authoritySequence uint64) []byte {
	return []byte(fmt.Sprintf(`{"snapshot":%s}`, messageEventSnapshotForTest(text, authoritySequence, true)))
}
