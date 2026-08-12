package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestMergeMessageEventTerminalPayloadKeepsCachedSnapshot(t *testing.T) {
	snapshot := []byte(`{"kind":"text","text":"cached"}`)

	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{name: "missing snapshot", payload: []byte(`{"end_reason":2}`)},
		{name: "null snapshot", payload: []byte(`{"snapshot":null,"end_reason":2}`)},
		{name: "invalid json", payload: []byte(`not-json`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mergeMessageEventTerminalPayload(tc.payload, snapshot)
			var body map[string]json.RawMessage
			if err := json.Unmarshal(got, &body); err != nil {
				t.Fatalf("merged payload is invalid JSON: %s: %v", got, err)
			}
			if !strings.Contains(string(body["snapshot"]), "cached") {
				t.Fatalf("snapshot = %s, want cached snapshot in payload %s", body["snapshot"], got)
			}
			if tc.name == "invalid json" && !strings.Contains(string(body["raw_payload"]), "not-json") {
				t.Fatalf("raw_payload = %s, want original invalid payload preserved", body["raw_payload"])
			}
		})
	}
}

func TestMessageEventStreamCacheRejectsNewActiveSessionWhenFull(t *testing.T) {
	cache := newMessageEventStreamCache(1)
	first := metadb.MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-1", RunID: "run-1",
		AuthorizationFence: "fence-1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: metadb.EventTypeDelta,
		Visibility: metadb.VisibilityPublic, Payload: []byte(`{"kind":"text","delta":"a"}`),
	}
	if _, err := cache.appendCached(first); err != nil {
		t.Fatalf("appendCached(first) error = %v", err)
	}
	second := first
	second.ClientMsgNo = "cmn-2"
	second.EventID = "evt-2"
	if _, err := cache.appendCached(second); !errors.Is(err, ErrBackpressured) {
		t.Fatalf("appendCached(second) error = %v, want ErrBackpressured", err)
	}

	closeEvent := metadb.MessageEventAppend{
		ChannelID: first.ChannelID, ChannelType: first.ChannelType, ClientMsgNo: first.ClientMsgNo,
		RunID: "run-1", AuthorizationFence: "fence-1", AuthoritySequence: 2,
		EventID: "evt-close", EventKey: "main", EventType: metadb.EventTypeFinish,
	}
	closeResult := metadb.MessageEventAppendResult{
		ChannelID:   first.ChannelID,
		ChannelType: first.ChannelType,
		ClientMsgNo: first.ClientMsgNo,
		EventID:     "evt-close",
		EventKey:    "main",
		MsgEventSeq: 2,
		Status:      metadb.EventStatusClosed,
		State: metadb.MessageEventState{
			ChannelID:   first.ChannelID,
			ChannelType: first.ChannelType,
			ClientMsgNo: first.ClientMsgNo,
			RunID:       "run-1",
			EventKey:    "main",
			Status:      metadb.EventStatusClosed,
		},
	}
	cache.completeRunObserved(closeEvent, closeResult)
	if _, err := cache.appendCached(second); err != nil {
		t.Fatalf("appendCached(second after terminal eviction) error = %v", err)
	}
}

func TestMessageEventCacheLimitsLanesPerRun(t *testing.T) {
	cache := newMessageEventStreamCache(10)
	base := metadb.MessageEventAppend{
		ChannelID:          "g1",
		ChannelType:        2,
		ClientMsgNo:        "cmn-1",
		RunID:              "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1,
		EventType:          metadb.EventTypeDelta,
		Visibility:         metadb.VisibilityPublic,
	}
	for i := 0; i < maxMessageEventLanesPerRun; i++ {
		event := base
		event.EventID = "evt-" + string(rune('a'+i))
		event.EventKey = "lane-" + string(rune('a'+i))
		event.AuthoritySequence = uint64(i + 1)
		if _, err := cache.appendCached(event); err != nil {
			t.Fatalf("appendCached(lane %d) error = %v", i, err)
		}
	}
	ninth := base
	ninth.EventID = "evt-ninth"
	ninth.EventKey = "lane-ninth"
	ninth.AuthoritySequence = maxMessageEventLanesPerRun + 1
	if _, err := cache.appendCached(ninth); !errors.Is(err, ErrMessageEventLaneLimit) {
		t.Fatalf("appendCached(ninth lane) error = %v, want ErrMessageEventLaneLimit", err)
	}

	otherRun := ninth
	otherRun.RunID = "run-2"
	otherRun.AuthorizationFence = "fence-2"
	otherRun.EventID = "evt-other-run"
	otherRun.EventKey = "main"
	otherRun.AuthoritySequence = 1
	if _, err := cache.appendCached(otherRun); err != nil {
		t.Fatalf("appendCached(other run) error = %v", err)
	}
}

func TestMessageEventCacheRejectsSnapshotGrowthWithoutAdvancing(t *testing.T) {
	cache := newMessageEventStreamCache(10)
	base := metadb.MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-large", RunID: "run-1",
		AuthorizationFence: "fence-1", EventKey: "main", EventType: metadb.EventTypeDelta,
		Visibility: metadb.VisibilityPublic,
	}
	delta := strings.Repeat("x", 16*1024-64)
	for sequence := uint64(1); sequence <= 16; sequence++ {
		event := base
		event.AuthoritySequence = sequence
		event.EventID = fmt.Sprintf("evt-%d", sequence)
		event.Payload = []byte(`{"text_delta":"` + delta + `"}`)
		if _, err := cache.appendCached(event); err != nil {
			t.Fatalf("appendCached(%d) error = %v", sequence, err)
		}
	}
	event := base
	event.AuthoritySequence = 17
	event.EventID = "evt-17"
	event.Payload = []byte(`{"text_delta":"` + delta + `"}`)
	if _, err := cache.appendCached(event); !errors.Is(err, ErrBackpressured) {
		t.Fatalf("appendCached(17) error = %v, want ErrBackpressured", err)
	}
	session := cache.sessions[messageEventCacheKey(base)]
	if session.runSequences[base.RunID] != 16 || session.states[messageEventLaneCacheKey(base)].LastMsgEventSeq != 16 {
		t.Fatalf("rejected event advanced cache: %#v", session)
	}
}

func TestMessageEventCacheUsesRunGlobalAuthoritySequenceAcrossLanes(t *testing.T) {
	cache := newMessageEventStreamCache(8)
	base := metadb.MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-global", RunID: "run-1",
		AuthorizationFence: "fence-1",
		AuthoritySequence:  1, Visibility: metadb.VisibilityPublic,
		EventType: metadb.EventTypeDelta, Payload: []byte(`{"kind":"text","delta":"x"}`),
	}
	for index, lane := range []string{"main", "tool", "main"} {
		event := base
		event.EventID = fmt.Sprintf("evt-%d", index+1)
		event.EventKey = lane
		event.AuthoritySequence = uint64(index + 1)
		result, err := cache.appendCached(event)
		if err != nil {
			t.Fatalf("appendCached(%s): %v", lane, err)
		}
		if result.MsgEventSeq != event.AuthoritySequence {
			t.Fatalf("appendCached(%s) seq=%d, want %d", lane, result.MsgEventSeq, event.AuthoritySequence)
		}
	}
	gap := base
	gap.EventID = "evt-5"
	gap.EventKey = "tool"
	gap.AuthoritySequence = 5
	if _, err := cache.appendCached(gap); !errors.Is(err, metadb.ErrStaleMeta) {
		t.Fatalf("appendCached(gap) error=%v, want stale meta", err)
	}
}

func TestMessageEventCacheRequiresSnapshotAfterFailover(t *testing.T) {
	cache := newMessageEventStreamCache(8)
	base := metadb.MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-recovery", RunID: "run-1",
		AuthorizationFence: "7", AuthoritySequence: 100, EventID: "snapshot-100", EventKey: "main",
		EventType: metadb.EventTypeSnapshot, Visibility: metadb.VisibilityPublic,
		Payload: []byte(`{"snapshot":{"state":"running","text":"hello"}}`),
	}
	first, err := cache.appendCached(base)
	if err != nil || first.MsgEventSeq != 100 {
		t.Fatalf("initial snapshot = %+v err=%v, want recovery seq=100", first, err)
	}
	base.AuthoritySequence = 101
	base.EventID = "delta-101"
	base.EventType = metadb.EventTypeDelta
	base.Payload = []byte(`{"text_delta":" world"}`)
	second, err := cache.appendCached(base)
	if err != nil || second.MsgEventSeq != 101 {
		t.Fatalf("delta = %+v err=%v, want seq=101", second, err)
	}

	cache.resetAfterRestore()
	base.AuthoritySequence = 103
	base.EventID = "delta-103"
	if _, err := cache.appendCached(base); !errors.Is(err, ErrMessageEventStreamCacheMiss) {
		t.Fatalf("first delta after reset error=%v, want cache miss", err)
	}
	base.EventID = "snapshot-103"
	base.EventType = metadb.EventTypeSnapshot
	base.Payload = []byte(`{"snapshot":{"state":"running","text":"hello recovered"}}`)
	recovered, err := cache.appendCached(base)
	if err != nil || recovered.MsgEventSeq != 103 || recovered.State.LastAuthoritySequence != 103 {
		t.Fatalf("recovered snapshot = %+v err=%v, want transport/authority=103", recovered, err)
	}
}

func TestMessageEventCacheIdempotencyRecordsDoNotRetainSnapshots(t *testing.T) {
	cache := newMessageEventStreamCache(8)
	event := metadb.MessageEventAppend{
		ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-memory", RunID: "run-1",
		AuthorizationFence: "1", EventKey: "main", EventType: metadb.EventTypeSnapshot,
		Visibility: metadb.VisibilityPublic,
	}
	for sequence := uint64(1); sequence <= 64; sequence++ {
		event.AuthoritySequence = sequence
		event.EventID = fmt.Sprintf("snapshot-%d", sequence)
		event.Payload = []byte(fmt.Sprintf(`{"snapshot":{"text":%q}}`, strings.Repeat("x", int(sequence)*64)))
		if _, err := cache.appendCached(event); err != nil {
			t.Fatal(err)
		}
	}
	session := cache.sessions[messageEventCacheKey(event)]
	retained := 0
	for _, result := range session.applied {
		retained += len(result.State.SnapshotPayload)
	}
	if retained != 0 {
		t.Fatalf("idempotency records retained %d snapshot bytes", retained)
	}
}

func TestMessageEventCacheTerminalClosesOnlyItsRun(t *testing.T) {
	cache := newMessageEventStreamCache(10)
	appendDelta := func(runID, eventID string) metadb.MessageEventAppend {
		return metadb.MessageEventAppend{
			ChannelID:          "g1",
			ChannelType:        2,
			ClientMsgNo:        "cmn-1",
			RunID:              runID,
			AuthorizationFence: "fence-1",
			AuthoritySequence:  1,
			EventID:            eventID,
			EventKey:           "main",
			EventType:          metadb.EventTypeDelta,
			Visibility:         metadb.VisibilityPublic,
		}
	}
	first := appendDelta("run-1", "evt-1")
	second := appendDelta("run-2", "evt-2")
	if _, err := cache.appendCached(first); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.appendCached(second); err != nil {
		t.Fatal(err)
	}
	terminal := first
	terminal.EventID = "evt-terminal"
	terminal.EventKey = "main"
	terminal.EventType = metadb.EventTypeFinish
	terminal.AuthoritySequence = 2
	result := cachedMessageEventResult(terminal, cachedMessageEventState(terminal))
	result.Status = metadb.EventStatusClosed
	result.State.Status = metadb.EventStatusClosed
	cache.completeRunObserved(terminal, result)

	retry := appendDelta("run-1", "evt-after-terminal")
	if _, err := cache.appendCached(retry); !errors.Is(err, ErrMessageEventRunTerminal) {
		t.Fatalf("appendCached(after terminal) error = %v, want ErrMessageEventRunTerminal", err)
	}
	second.EventID = "evt-run-2-next"
	second.AuthoritySequence = 2
	if _, err := cache.appendCached(second); err != nil {
		t.Fatalf("appendCached(other run after terminal) error = %v", err)
	}
	states := cache.states(metadb.MessageEventMessageKey{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "cmn-1"})
	if len(states) != 1 || states[0].RunID != "run-2" || states[0].EventKey != "main" {
		t.Fatalf("states after run terminal = %#v, want only run-2 lane", states)
	}
}
