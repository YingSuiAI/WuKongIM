//go:build integration

package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/propose"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestClusterMessageEventFacadeAppendsTerminalThroughChannelHashSlot(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-channel"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        "cmn-event-1",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-1",
		EventKey:           "main",
		EventType:          metadb.EventTypeFinish,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1000,
		Payload:            messageEventFinishForIntegrationTest("hello", 1),
		UpdatedAt:          1001,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent() error = %v", err)
	}
	if result.MsgEventSeq != 1 || result.Status != metadb.EventStatusClosed {
		t.Fatalf("append result = %#v, want seq=1 closed", result)
	}

	states, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, "cmn-event-1", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates(channel hash slot) error = %v", err)
	}
	if len(states) != 1 || states[0].LastMsgEventSeq != 1 || states[0].LastEventID != "evt-1" {
		t.Fatalf("stored states = %#v, want one state at seq 1", states)
	}
}

func TestClusterMessageEventFacadeCachesStreamUntilTerminalEvent(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-stream-cache"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:   channelID,
		ChannelType: 2,
		ClientMsgNo: "cmn-stream-cache",
		RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID:    "evt-delta-1",
		EventKey:   "main",
		EventType:  metadb.EventTypeDelta,
		Visibility: metadb.VisibilityPublic,
		OccurredAt: 1000,
		Payload:    messageEventDeltaForIntegrationTest("hello ", 1),
		UpdatedAt:  1001,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(delta 1) error = %v", err)
	}
	if result.MsgEventSeq != 1 || result.Status != metadb.EventStatusOpen {
		t.Fatalf("delta result = %#v, want cache-only open authority seq 1", result)
	}

	result, err = node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:   channelID,
		ChannelType: 2,
		ClientMsgNo: "cmn-stream-cache",
		RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
		EventID:    "evt-delta-2",
		EventKey:   "main",
		EventType:  metadb.EventTypeDelta,
		Visibility: metadb.VisibilityPublic,
		OccurredAt: 1002,
		Payload:    messageEventDeltaForIntegrationTest("world", 2),
		UpdatedAt:  1003,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(delta 2) error = %v", err)
	}
	if !strings.Contains(string(result.State.SnapshotPayload), "hello world") {
		t.Fatalf("cached delta snapshot = %s, want accumulated text", result.State.SnapshotPayload)
	}

	states, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, "cmn-stream-cache", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates(before terminal) error = %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("states before terminal = %#v, want no durable event state", states)
	}
	key := metadb.MessageEventMessageKey{ChannelID: channelID, ChannelType: 2, ClientMsgNo: "cmn-stream-cache"}
	summary, err := node.GetMessageEventStatesBatch(ctx, []metadb.MessageEventMessageKey{key}, 10)
	if err != nil {
		t.Fatalf("GetMessageEventStatesBatch(before terminal) error = %v", err)
	}
	if len(summary[key]) != 1 || summary[key][0].LastMsgEventSeq != 2 || summary[key][0].Status != metadb.EventStatusOpen {
		t.Fatalf("summary before terminal = %#v, want one cached open state", summary[key])
	}
	if !strings.Contains(string(summary[key][0].SnapshotPayload), "hello world") {
		t.Fatalf("summary snapshot before terminal = %s, want cached text", summary[key][0].SnapshotPayload)
	}

	closed, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:   channelID,
		ChannelType: 2,
		ClientMsgNo: "cmn-stream-cache",
		RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 3,
		EventID:    "evt-close",
		EventKey:   "main",
		EventType:  metadb.EventTypeFinish,
		Visibility: metadb.VisibilityPublic,
		OccurredAt: 1004,
		Payload:    messageEventFinishForIntegrationTest("hello world", 3),
		UpdatedAt:  1005,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(close) error = %v", err)
	}
	if closed.MsgEventSeq != 3 || closed.Status != metadb.EventStatusClosed {
		t.Fatalf("close result = %#v, want one durable closed event", closed)
	}
	if !strings.Contains(string(closed.State.SnapshotPayload), "hello world") {
		t.Fatalf("closed snapshot = %s, want cached text merged", closed.State.SnapshotPayload)
	}

	states, err = node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, "cmn-stream-cache", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates(after terminal) error = %v", err)
	}
	if len(states) != 1 || states[0].LastMsgEventSeq != 3 || states[0].LastEventID != "evt-close" {
		t.Fatalf("states after terminal = %#v, want one closed durable state", states)
	}
	summary, err = node.GetMessageEventStatesBatch(ctx, []metadb.MessageEventMessageKey{key}, 10)
	if err != nil {
		t.Fatalf("GetMessageEventStatesBatch(after terminal) error = %v", err)
	}
	if len(summary[key]) != 1 || summary[key][0].LastMsgEventSeq != 3 || summary[key][0].Status != metadb.EventStatusClosed {
		t.Fatalf("summary after terminal = %#v, want one durable closed state", summary[key])
	}
}

func TestClusterMessageEventFacadeFinishFlushesCachedLanes(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-stream-finish"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for sequence, tc := range []struct {
		eventID string
		key     string
		delta   string
	}{
		{eventID: "evt-main-delta", key: "main", delta: "answer"},
		{eventID: "evt-tool-delta", key: "tool", delta: "lookup"},
	} {
		if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
			ChannelID:   channelID,
			ChannelType: 2,
			ClientMsgNo: "cmn-stream-finish",
			RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: uint64(sequence + 1),
			EventID:    tc.eventID,
			EventKey:   tc.key,
			EventType:  metadb.EventTypeDelta,
			Visibility: metadb.VisibilityPublic,
			OccurredAt: 1000,
			Payload:    messageEventDeltaForIntegrationTest(tc.delta, uint64(sequence+1)),
			UpdatedAt:  1001,
		}); err != nil {
			t.Fatalf("AppendMessageEvent(%s delta) error = %v", tc.key, err)
		}
	}

	result, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:   channelID,
		ChannelType: 2,
		ClientMsgNo: "cmn-stream-finish",
		RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 3,
		EventKey:   "main",
		EventID:    "evt-finish",
		EventType:  metadb.EventTypeFinish,
		Visibility: metadb.VisibilityPublic,
		OccurredAt: 1002,
		Payload:    messageEventFinishForIntegrationTest("answer", 3),
		UpdatedAt:  1003,
	})
	if err != nil {
		t.Fatalf("AppendMessageEvent(finish) error = %v", err)
	}
	if result.EventKey != "main" || result.Status != metadb.EventStatusClosed {
		t.Fatalf("finish result = %#v, want closed finish marker", result)
	}

	states, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, "cmn-stream-finish", 10)
	if err != nil {
		t.Fatalf("ListMessageEventStates(after finish) error = %v", err)
	}
	byKey := make(map[string]metadb.MessageEventState, len(states))
	for _, state := range states {
		byKey[state.EventKey] = state
	}
	for _, want := range []struct {
		key  string
		text string
	}{
		{key: "main", text: "answer"},
		{key: "tool", text: "lookup"},
	} {
		state, ok := byKey[want.key]
		if !ok {
			t.Fatalf("durable states after finish = %#v, missing flushed %s lane", states, want.key)
		}
		if state.Status != metadb.EventStatusClosed || state.LastMsgEventSeq == 0 {
			t.Fatalf("%s lane after finish = %#v, want durable closed state", want.key, state)
		}
		if !strings.Contains(string(state.SnapshotPayload), want.text) {
			t.Fatalf("%s lane snapshot = %s, want cached text %q", want.key, state.SnapshotPayload, want.text)
		}
	}
	if _, ok := byKey["main"]; !ok {
		t.Fatalf("durable states after finish = %#v, missing finish marker", states)
	}
}

func TestClusterMessageEventFinishFlushUsesSingleProposal(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-finish-batch"
	waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for sequence, tc := range []struct {
		eventID string
		key     string
		delta   string
	}{
		{eventID: "evt-main-delta", key: "main", delta: "answer"},
		{eventID: "evt-tool-delta", key: "tool", delta: "lookup"},
	} {
		if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
			ChannelID:   channelID,
			ChannelType: 2,
			ClientMsgNo: "cmn-finish-batch",
			RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: uint64(sequence + 1),
			EventID:    tc.eventID,
			EventKey:   tc.key,
			EventType:  metadb.EventTypeDelta,
			Visibility: metadb.VisibilityPublic,
			OccurredAt: 1000,
			Payload:    messageEventDeltaForIntegrationTest(tc.delta, uint64(sequence+1)),
			UpdatedAt:  1001,
		}); err != nil {
			t.Fatalf("AppendMessageEvent(%s delta) error = %v", tc.key, err)
		}
	}

	proposer := wrapNodeResultProposer(t, node)
	before := proposer.resultCallCount()
	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:   channelID,
		ChannelType: 2,
		ClientMsgNo: "cmn-finish-batch",
		RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 3,
		EventKey:   "main",
		EventID:    "evt-finish",
		EventType:  metadb.EventTypeFinish,
		Visibility: metadb.VisibilityPublic,
		OccurredAt: 1002,
		Payload:    messageEventFinishForIntegrationTest("answer", 3),
		UpdatedAt:  1003,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(finish) error = %v", err)
	}
	if got := proposer.resultCallCount() - before; got != 1 {
		t.Fatalf("finish result proposals = %d, want 1", got)
	}
}

func TestClusterMessageEventFinishPreservesLaneSequencesAndClosesRun(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-finish-sequences"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientMsgNo := "cmn-finish-sequences"
	for _, event := range []metadb.MessageEventAppend{
		{AuthoritySequence: 1, EventID: "main-1", EventKey: "main", Payload: messageEventDeltaForIntegrationTest("a", 1)},
		{AuthoritySequence: 2, EventID: "tool-2", EventKey: "tool", Payload: messageEventDeltaForIntegrationTest("t", 2)},
		{AuthoritySequence: 3, EventID: "tool-3", EventKey: "tool", Payload: messageEventDeltaForIntegrationTest("ool", 3)},
		{AuthoritySequence: 4, EventID: "main-4", EventKey: "main", Payload: messageEventDeltaForIntegrationTest("nswer", 4)},
		{AuthoritySequence: 5, EventID: "progress-5", EventKey: "progress", Payload: messageEventDeltaForIntegrationTest("5", 5)},
		{AuthoritySequence: 6, EventID: "progress-6", EventKey: "progress", Payload: messageEventDeltaForIntegrationTest("6", 6)},
	} {
		event.ChannelID = channelID
		event.ChannelType = 2
		event.ClientMsgNo = clientMsgNo
		event.RunID = "run-1"
		event.AuthorizationFence = "fence-1"
		event.EventType = metadb.EventTypeDelta
		event.Visibility = metadb.VisibilityPublic
		if _, err := node.AppendMessageEvent(ctx, event); err != nil {
			t.Fatalf("AppendMessageEvent(%s) error = %v", event.EventID, err)
		}
	}

	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID: channelID, ChannelType: 2, ClientMsgNo: clientMsgNo,
		RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 7,
		EventID: "finish-7", EventKey: "final", EventType: metadb.EventTypeFinish,
		Visibility: metadb.VisibilityPublic, Payload: messageEventFinishForIntegrationTest("done", 7),
	}); err != nil {
		t.Fatalf("AppendMessageEvent(finish) error = %v", err)
	}

	states, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, clientMsgNo, 10)
	if err != nil {
		t.Fatal(err)
	}
	byKey := make(map[string]metadb.MessageEventState, len(states))
	for _, state := range states {
		byKey[state.EventKey] = state
		if state.Status != metadb.EventStatusClosed {
			t.Fatalf("state %s status = %s, want closed", state.EventKey, state.Status)
		}
	}
	if byKey["main"].LastMsgEventSeq != 4 || byKey["tool"].LastMsgEventSeq != 3 || byKey["progress"].LastMsgEventSeq != 6 || byKey["final"].LastMsgEventSeq != 7 {
		t.Fatalf("durable lane sequences = %#v, want main=4 tool=3 progress=6 final=7", byKey)
	}
	cursor, found, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).GetMessageEventCursor(ctx, channelID, 2, clientMsgNo, "run-1")
	if err != nil || !found || cursor.LastMsgEventSeq != 7 || !cursor.Terminal {
		t.Fatalf("cursor = %#v found=%v err=%v, want seq=7 terminal", cursor, found, err)
	}
	key := metadb.MessageEventMessageKey{ChannelID: channelID, ChannelType: 2, ClientMsgNo: clientMsgNo}
	summary, err := node.GetMessageEventStatesBatch(ctx, []metadb.MessageEventMessageKey{key}, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range summary[key] {
		if state.Status != metadb.EventStatusClosed {
			t.Fatalf("summary state = %#v, want no open lanes", state)
		}
	}
}

func TestClusterMessageEventConcurrentDifferentFinishesHaveOneWinner(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-concurrent-finish"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	base := metadb.MessageEventAppend{
		ChannelID: channelID, ChannelType: 2, ClientMsgNo: "cmn-concurrent-finish",
		RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "delta-1", EventKey: "main", EventType: metadb.EventTypeDelta,
		Visibility: metadb.VisibilityPublic, Payload: messageEventDeltaForIntegrationTest("answer", 1),
	}
	if _, err := node.AppendMessageEvent(ctx, base); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]metadb.MessageEventAppendResult, 2)
	errs := make([]error, 2)
	for i := range errs {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			finish := base
			finish.AuthoritySequence = 2
			finish.EventID = fmt.Sprintf("finish-%d", i)
			finish.EventType = metadb.EventTypeFinish
			finish.Payload = messageEventFinishForIntegrationTest(fmt.Sprintf("answer-%d", i), 2)
			results[i], errs[i] = node.AppendMessageEvent(ctx, finish)
		}()
	}
	wg.Wait()
	successes := 0
	failures := 0
	for i, err := range errs {
		if err == nil {
			successes++
			if !results[i].Applied || results[i].Status != metadb.EventStatusClosed {
				t.Fatalf("winner result = %#v", results[i])
			}
			continue
		}
		if !errors.Is(err, metadb.ErrStaleMeta) && !errors.Is(err, ErrMessageEventRunTerminal) {
			t.Fatalf("loser error = %v, want stale or terminal", err)
		}
		failures++
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("finish outcomes successes=%d failures=%d errs=%v", successes, failures, errs)
	}
	cursor, found, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).GetMessageEventCursor(ctx, channelID, 2, base.ClientMsgNo, base.RunID)
	if err != nil || !found || cursor.LastMsgEventSeq != 2 || !cursor.Terminal {
		t.Fatalf("cursor = %#v found=%v err=%v, want winner seq=2 terminal", cursor, found, err)
	}
}

func TestClusterMessageEventConcurrentSameIDDifferentFinishPayloadRejectsConflict(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-concurrent-finish-digest"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	base := metadb.MessageEventAppend{
		ChannelID: channelID, ChannelType: 2, ClientMsgNo: "cmn-concurrent-finish-digest",
		RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "delta-1", EventKey: "main", EventType: metadb.EventTypeDelta,
		Visibility: metadb.VisibilityPublic, Payload: messageEventDeltaForIntegrationTest("answer", 1),
	}
	if _, err := node.AppendMessageEvent(ctx, base); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make([]metadb.MessageEventAppendResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			finish := base
			finish.AuthoritySequence = 2
			finish.EventID = "finish-shared"
			finish.EventKey = "terminal"
			finish.EventType = metadb.EventTypeFinish
			finish.Payload = messageEventFinishForIntegrationTest(fmt.Sprintf("winner-%d", i), 2)
			results[i], errs[i] = node.AppendMessageEvent(ctx, finish)
		}()
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, err := range errs {
		if err == nil {
			if winner >= 0 {
				t.Fatalf("both conflicting finishes succeeded: results=%#v", results)
			}
			winner = i
			continue
		}
		if !errors.Is(err, metadb.ErrStaleMeta) {
			t.Fatalf("conflicting finish %d error = %v, want ErrStaleMeta", i, err)
		}
	}
	if winner < 0 {
		t.Fatalf("no finish succeeded: errors=%v", errs)
	}
	states, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, base.ClientMsgNo, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.EventKey == "terminal" && !strings.Contains(string(state.SnapshotPayload), fmt.Sprintf("winner-%d", winner)) {
			t.Fatalf("terminal snapshot = %s, want successful finish payload", state.SnapshotPayload)
		}
	}
}

func TestClusterMessageEventCoalescesConcurrentFinishesForSameChannel(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-finish-coalesce"
	route := waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, clientMsgNo := range []string{"cmn-coalesce-a", "cmn-coalesce-b"} {
		if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
			ChannelID:   channelID,
			ChannelType: 2,
			ClientMsgNo: clientMsgNo,
			RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 1,
			EventID:    "evt-delta-" + clientMsgNo,
			EventKey:   "main",
			EventType:  metadb.EventTypeDelta,
			Visibility: metadb.VisibilityPublic,
			OccurredAt: 1000,
			Payload:    messageEventDeltaForIntegrationTest(clientMsgNo, 1),
			UpdatedAt:  1001,
		}); err != nil {
			t.Fatalf("AppendMessageEvent(delta %s) error = %v", clientMsgNo, err)
		}
	}

	proposer := wrapNodeResultProposer(t, node)
	before := proposer.resultCallCount()
	var wg sync.WaitGroup
	results := make([]metadb.MessageEventAppendResult, 2)
	errs := make([]error, 2)
	for i, clientMsgNo := range []string{"cmn-coalesce-a", "cmn-coalesce-b"} {
		i, clientMsgNo := i, clientMsgNo
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
				ChannelID:   channelID,
				ChannelType: 2,
				ClientMsgNo: clientMsgNo,
				RunID:       "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
				EventKey:   "terminal",
				EventID:    "evt-finish-" + clientMsgNo,
				EventType:  metadb.EventTypeFinish,
				Visibility: metadb.VisibilityPublic,
				OccurredAt: 1002 + int64(i),
				Payload:    messageEventFinishForIntegrationTest(clientMsgNo, 2),
				UpdatedAt:  1004 + int64(i),
			})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("AppendMessageEvent(finish %d) error = %v", i, err)
		}
	}
	if got := proposer.resultCallCount() - before; got != 2 {
		t.Fatalf("independent anchor finish proposals = %d, want 2", got)
	}
	for i, clientMsgNo := range []string{"cmn-coalesce-a", "cmn-coalesce-b"} {
		if results[i].ClientMsgNo != clientMsgNo || results[i].EventKey != "terminal" || results[i].MsgEventSeq == 0 {
			t.Fatalf("finish result %d = %#v, want result for %s finish marker", i, results[i], clientMsgNo)
		}
		states, err := node.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, channelID, 2, clientMsgNo, 10)
		if err != nil {
			t.Fatalf("ListMessageEventStates(%s) error = %v", clientMsgNo, err)
		}
		if len(states) != 2 {
			t.Fatalf("states for %s = %#v, want flushed main lane and finish marker", clientMsgNo, states)
		}
	}
}

func TestClusterMessageEventFinishWithoutCachedLanesFailsClosed(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-finish-cache-miss"
	waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	proposer := wrapNodeResultProposer(t, node)
	before := proposer.resultCallCount()
	_, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        "cmn-finish-cache-miss",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-finish",
		EventType:          metadb.EventTypeFinish,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1002,
		Payload:            []byte(`{}`),
		UpdatedAt:          1003,
	})
	if !errors.Is(err, ErrMessageEventStreamCacheMiss) {
		t.Fatalf("AppendMessageEvent(finish without cache) error = %v, want ErrMessageEventStreamCacheMiss", err)
	}
	if got := proposer.resultCallCount() - before; got != 0 {
		t.Fatalf("finish cache miss proposals = %d, want 0", got)
	}
}

func TestClusterMessageEventCacheClearsWhenLocalSlotLeadershipIsLost(t *testing.T) {
	observer := &recordingMessageEventObserver{}
	node := &Node{
		cfg:                     Config{NodeID: 1, MessageEvent: MessageEventConfig{Observer: observer}},
		router:                  routing.NewRouter(),
		messageEventStreamCache: newMessageEventStreamCache(0),
	}
	snapshot := routeAuthoritySnapshot(1)
	if err := node.router.UpdateControlSnapshot(snapshot); err != nil {
		t.Fatalf("UpdateControlSnapshot() error = %v", err)
	}
	node.router.UpdateSlotLeaders([]routing.SlotStatus{{SlotID: 1, Leader: 1, LeaderTerm: 9}})
	node.started.Store(true)

	channelID := "message-event-clear-on-leader-loss"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        "cmn-clear-on-leader-loss",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-delta",
		EventKey:           "main",
		EventType:          metadb.EventTypeDelta,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1000,
		Payload:            messageEventDeltaForIntegrationTest("stale", 1),
		UpdatedAt:          1001,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(delta) error = %v", err)
	}
	if cache := observer.lastCache(); cache.Sessions != 1 || cache.OpenLanes != 1 {
		t.Fatalf("cache after delta = %#v, want one open session", cache)
	}

	before := node.router.Table()
	node.router.UpdateSlotLeaders([]routing.SlotStatus{{SlotID: 1, Leader: 2, LeaderTerm: 10}})
	node.publishRouteAuthorityChanges(before)

	if cache := observer.lastCache(); cache.Sessions != 0 || cache.OpenLanes != 0 || cache.PayloadBytes != 0 {
		t.Fatalf("cache after local leadership loss = %#v, want empty cache", cache)
	}
}

func TestClusterMessageEventAppendIsFencedDuringRestoreMaintenance(t *testing.T) {
	node := &Node{
		cfg:                     Config{NodeID: 1},
		router:                  routing.NewRouter(),
		messageEventStreamCache: newMessageEventStreamCache(0),
	}
	if err := node.router.UpdateControlSnapshot(routeAuthoritySnapshot(1)); err != nil {
		t.Fatalf("UpdateControlSnapshot() error = %v", err)
	}
	node.router.UpdateSlotLeaders([]routing.SlotStatus{{SlotID: 1, Leader: 1, LeaderTerm: 9}})
	node.started.Store(true)
	node.setMaintenance(true)

	_, err := node.AppendMessageEvent(context.Background(), metadb.MessageEventAppend{
		ChannelID:   "message-event-maintenance",
		ChannelType: 2,
		ClientMsgNo: "cmn-maintenance",
		EventID:     "evt-delta",
		EventKey:    "main",
		EventType:   metadb.EventTypeDelta,
		Visibility:  metadb.VisibilityPublic,
		Payload:     []byte(`{"delta":"stale"}`),
	})
	if !errors.Is(err, ErrMaintenance) {
		t.Fatalf("AppendMessageEvent() error = %v, want ErrMaintenance", err)
	}
	if got := node.messageEventStreamCache.observation(); got.Sessions != 0 {
		t.Fatalf("cache after rejected append = %#v, want empty", got)
	}
}

func TestClusterMessageEventCacheClearsAcrossSerializedLocalRemoteLocalAuthorityUpdates(t *testing.T) {
	node := &Node{
		cfg:                     Config{NodeID: 1},
		router:                  routing.NewRouter(),
		messageEventStreamCache: newMessageEventStreamCache(0),
		routeAuthorityEpochs:    make(map[uint16]uint64),
	}
	if err := node.router.UpdateControlSnapshot(routeAuthoritySnapshot(1)); err != nil {
		t.Fatalf("UpdateControlSnapshot() error = %v", err)
	}
	node.router.UpdateSlotLeaders([]routing.SlotStatus{{SlotID: 1, Leader: 1, LeaderTerm: 9}})
	node.publishRouteAuthorityChanges(nil)
	node.started.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          "message-event-serialized-leader-loss",
		ChannelType:        2,
		ClientMsgNo:        "cmn-serialized-leader-loss",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-delta",
		EventKey:           "main",
		EventType:          metadb.EventTypeDelta,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1000,
		Payload:            messageEventDeltaForIntegrationTest("stale", 1),
		UpdatedAt:          1001,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(delta) error = %v", err)
	}
	if got := node.messageEventStreamCache.observation(); got.Sessions != 1 {
		t.Fatalf("cache before authority changes = %#v, want one session", got)
	}

	remoteMutated := make(chan struct{})
	releaseRemote := make(chan struct{})
	remoteDone := make(chan struct{})
	go func() {
		defer close(remoteDone)
		_ = node.updateRouteAuthorityTable(func() error {
			node.router.UpdateSlotLeaders([]routing.SlotStatus{{SlotID: 1, Leader: 2, LeaderTerm: 10}})
			close(remoteMutated)
			<-releaseRemote
			return nil
		})
	}()
	<-remoteMutated

	localMutated := make(chan struct{})
	localDone := make(chan struct{})
	go func() {
		defer close(localDone)
		_ = node.updateRouteAuthorityTable(func() error {
			node.router.UpdateSlotLeaders([]routing.SlotStatus{{SlotID: 1, Leader: 1, LeaderTerm: 11}})
			close(localMutated)
			return nil
		})
	}()
	select {
	case <-localMutated:
		t.Fatal("local reacquire mutated Router before remote authority loss was published")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseRemote)
	<-remoteDone
	<-localDone

	if got := node.messageEventStreamCache.observation(); got.Sessions != 0 || got.OpenLanes != 0 || got.PayloadBytes != 0 {
		t.Fatalf("cache after local-remote-local authority changes = %#v, want empty cache", got)
	}
	if route := node.router.Table(); route == nil || route.SlotLeaders[1] != 1 || route.SlotLeaderTerms[1] != 11 {
		t.Fatalf("final route = %#v, want local leader term 11", route)
	}
}

func TestClusterMessageEventObserverTracksCacheAndFinishBatch(t *testing.T) {
	observer := &recordingMessageEventObserver{}
	cfg := Config{NodeID: 1, ListenAddr: freeTCPAddr(t), DataDir: t.TempDir()}
	cfg.Control.ClusterID = "cluster-message-event-observer"
	cfg.Slots.InitialSlotCount = 1
	cfg.Slots.HashSlotCount = 4
	cfg.Slots.ReplicaCount = 1
	cfg.Channel.TickInterval = time.Millisecond
	cfg.MessageEvent.Observer = observer
	node, err := New(cfg)
	if err != nil {
		t.Fatalf("New(single-node cluster) error = %v", err)
	}
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelID := "message-event-observer"
	waitRouteKeyLeaderReady(t, node, channelID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for sequence, tc := range []struct {
		eventID string
		key     string
		delta   string
	}{
		{eventID: "evt-main-delta", key: "main", delta: "answer"},
		{eventID: "evt-tool-delta", key: "tool", delta: "lookup"},
	} {
		if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
			ChannelID:          channelID,
			ChannelType:        2,
			ClientMsgNo:        "cmn-observer",
			RunID:              "run-1",
			AuthorizationFence: "fence-1",
			AuthoritySequence:  uint64(sequence + 1),
			EventID:            tc.eventID,
			EventKey:           tc.key,
			EventType:          metadb.EventTypeDelta,
			Visibility:         metadb.VisibilityPublic,
			OccurredAt:         1000,
			Payload:            messageEventDeltaForIntegrationTest(tc.delta, uint64(sequence+1)),
			UpdatedAt:          1001,
		}); err != nil {
			t.Fatalf("AppendMessageEvent(%s delta) error = %v", tc.key, err)
		}
	}
	if got := observer.proposeCount(); got != 0 {
		t.Fatalf("propose observations after cache-only deltas = %d, want 0", got)
	}
	cache := observer.lastCache()
	if cache.Sessions != 1 || cache.OpenLanes != 2 || cache.PayloadBytes == 0 || cache.MaxSessions != defaultMessageEventStreamCacheMaxSessions {
		t.Fatalf("cache observation after deltas = %#v, want one session/two lanes/non-zero bytes/default max", cache)
	}

	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelID,
		ChannelType:        2,
		ClientMsgNo:        "cmn-observer",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  3,
		EventID:            "evt-finish",
		EventType:          metadb.EventTypeFinish,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1002,
		Payload:            messageEventFinishForIntegrationTest("answer", 3),
		UpdatedAt:          1003,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(finish) error = %v", err)
	}
	observer.requireAppend(t, "cache", metadb.EventTypeDelta, "ok")
	observer.requireAppend(t, "finish_batch", metadb.EventTypeFinish, "ok")
	observer.requirePropose(t, "finish_batch", "ok", 2)
	observer.requireAppendStage(t, "finish_batch", "ok", "finish_batch_build")
	observer.requireAppendStage(t, "finish_batch", "ok", "finish_cache_remove")
	observer.requireProposeStage(t, "finish_batch", "ok", "encode")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_propose_wait")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_propose_submit")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_future_wait")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_control_wait")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_raft_commit_wait")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_fsm_apply")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_fsm_commit")
	observer.requireProposeStage(t, "finish_batch", "ok", "slot_mark_applied")
	observer.requireProposeStage(t, "finish_batch", "ok", "decode")
	cache = observer.lastCache()
	if cache.Sessions != 1 || cache.OpenLanes != 0 || cache.PayloadBytes != 0 {
		t.Fatalf("cache observation after finish = %#v, want lightweight terminal idempotency session", cache)
	}
}

func TestClusterGetMessageEventStatesBatchRoutesEachMessage(t *testing.T) {
	node := newDefaultSingleNode(t)
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })

	channelA := channelLatestKeyForHashSlot(t, 1, 4)
	channelB := channelLatestKeyForHashSlot(t, 3, 4)
	waitRouteKeyLeaderReady(t, node, channelA)
	waitRouteKeyLeaderReady(t, node, channelB)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelA,
		ChannelType:        2,
		ClientMsgNo:        "cmn-a",
		RunID:              "run-a",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-a",
		EventKey:           "main",
		EventType:          metadb.EventTypeFinish,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         1000,
		Payload:            messageEventFinishForIntegrationTest("a", 1),
		UpdatedAt:          1001,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(channelA) error = %v", err)
	}
	if _, err := node.AppendMessageEvent(ctx, metadb.MessageEventAppend{
		ChannelID:          channelB,
		ChannelType:        2,
		ClientMsgNo:        "cmn-b",
		RunID:              "run-b",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventID:            "evt-b",
		EventKey:           "tool",
		EventType:          metadb.EventTypeFinish,
		Visibility:         metadb.VisibilityPublic,
		OccurredAt:         2000,
		Payload:            messageEventFinishForIntegrationTest("b", 1),
		UpdatedAt:          2001,
	}); err != nil {
		t.Fatalf("AppendMessageEvent(channelB) error = %v", err)
	}

	keyA := metadb.MessageEventMessageKey{ChannelID: channelA, ChannelType: 2, ClientMsgNo: "cmn-a"}
	keyB := metadb.MessageEventMessageKey{ChannelID: channelB, ChannelType: 2, ClientMsgNo: "cmn-b"}
	missing := metadb.MessageEventMessageKey{ChannelID: channelA, ChannelType: 2, ClientMsgNo: "missing"}
	got, err := node.GetMessageEventStatesBatch(ctx, []metadb.MessageEventMessageKey{keyA, missing, keyB, keyA}, 10)
	if err != nil {
		t.Fatalf("GetMessageEventStatesBatch() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetMessageEventStatesBatch() returned %d keys, want 2: %#v", len(got), got)
	}
	if len(got[keyA]) != 1 || got[keyA][0].EventKey != "main" {
		t.Fatalf("states for keyA = %#v, want main lane", got[keyA])
	}
	if len(got[keyB]) != 1 || got[keyB][0].EventKey != "tool" {
		t.Fatalf("states for keyB = %#v, want tool lane", got[keyB])
	}
	if _, ok := got[missing]; ok {
		t.Fatalf("missing key returned states: %#v", got[missing])
	}
}

func messageEventDeltaForIntegrationTest(text string, authoritySequence uint64) []byte {
	return []byte(fmt.Sprintf(`{"text_delta":%q,"authority_sequence":%d,"projection_digest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, text, authoritySequence))
}

func messageEventFinishForIntegrationTest(text string, authoritySequence uint64) []byte {
	return []byte(fmt.Sprintf(`{"snapshot":{"state":"succeeded","complete":true,"text":%q,"authority_sequence":%d,"projection_digest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`, text, authoritySequence))
}

type countingResultProposer struct {
	delegate interface {
		Propose(context.Context, propose.Request) error
	}
	resultDelegate resultProposer
	mu             sync.Mutex
	resultCalls    int
}

func wrapNodeResultProposer(t *testing.T, node *Node) *countingResultProposer {
	t.Helper()
	node.mu.Lock()
	defer node.mu.Unlock()
	delegate := node.proposer
	resultDelegate, ok := delegate.(resultProposer)
	if !ok {
		t.Fatalf("node proposer %T does not support ProposeResult", delegate)
	}
	proposer := &countingResultProposer{delegate: delegate, resultDelegate: resultDelegate}
	node.proposer = proposer
	return proposer
}

func (p *countingResultProposer) Propose(ctx context.Context, req propose.Request) error {
	return p.delegate.Propose(ctx, req)
}

func (p *countingResultProposer) ProposeResult(ctx context.Context, req propose.Request) ([]byte, error) {
	p.mu.Lock()
	p.resultCalls++
	p.mu.Unlock()
	return p.resultDelegate.ProposeResult(ctx, req)
}

func (p *countingResultProposer) resultCallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resultCalls
}

type recordingMessageEventObserver struct {
	mu            sync.Mutex
	appends       []MessageEventAppendObservation
	appendStages  []MessageEventAppendStageObservation
	proposes      []MessageEventProposeObservation
	proposeStages []MessageEventProposeStageObservation
	caches        []MessageEventStreamCacheObservation
}

func (o *recordingMessageEventObserver) ObserveMessageEventAppend(event MessageEventAppendObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.appends = append(o.appends, event)
}

func (o *recordingMessageEventObserver) ObserveMessageEventAppendStage(event MessageEventAppendStageObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.appendStages = append(o.appendStages, event)
}

func (o *recordingMessageEventObserver) ObserveMessageEventPropose(event MessageEventProposeObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.proposes = append(o.proposes, event)
}

func (o *recordingMessageEventObserver) ObserveMessageEventProposeStage(event MessageEventProposeStageObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.proposeStages = append(o.proposeStages, event)
}

func (o *recordingMessageEventObserver) SetMessageEventStreamCache(event MessageEventStreamCacheObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.caches = append(o.caches, event)
}

func (o *recordingMessageEventObserver) proposeCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.proposes)
}

func (o *recordingMessageEventObserver) lastCache() MessageEventStreamCacheObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.caches) == 0 {
		return MessageEventStreamCacheObservation{}
	}
	return o.caches[len(o.caches)-1]
}

func (o *recordingMessageEventObserver) requireAppend(t *testing.T, path, eventType, result string) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.appends {
		if event.Path == path && event.EventType == eventType && event.Result == result {
			return
		}
	}
	t.Fatalf("append observations = %#v, missing path=%s event_type=%s result=%s", o.appends, path, eventType, result)
}

func (o *recordingMessageEventObserver) requirePropose(t *testing.T, path, result string, batchSize int) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.proposes {
		if event.Path == path && event.Result == result && event.BatchSize == batchSize {
			return
		}
	}
	t.Fatalf("propose observations = %#v, missing path=%s result=%s batch_size=%d", o.proposes, path, result, batchSize)
}

func (o *recordingMessageEventObserver) requireAppendStage(t *testing.T, path, result, stage string) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.appendStages {
		if event.Path == path && event.Result == result && event.Stage == stage {
			return
		}
	}
	t.Fatalf("append stage observations = %#v, missing path=%s result=%s stage=%s", o.appendStages, path, result, stage)
}

func (o *recordingMessageEventObserver) requireProposeStage(t *testing.T, path, result, stage string) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.proposeStages {
		if event.Path == path && event.Result == result && event.Stage == stage {
			return
		}
	}
	t.Fatalf("propose stage observations = %#v, missing path=%s result=%s stage=%s", o.proposeStages, path, result, stage)
}
