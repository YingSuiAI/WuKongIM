package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	presenceusecase "github.com/WuKongIM/WuKongIM/internal/usecase/presence"
)

func TestRealtimeMessageEventCarriesTypedReducerFields(t *testing.T) {
	payload := json.RawMessage(`{"run_id":"run-1","event_type":"delta","event_key":"main","authority_sequence":7,"text_delta":"hello"}`)
	got, err := marshalRealtimeMessageEvent(messageusecase.MessageEventAppend{
		EventType: messageusecase.EventTypeDelta,
		Payload:   payload,
	}, messageusecase.MessageEventAppendResult{
		ChannelID: "u1@agent-a", ChannelType: 11, MessageID: 9001, ClientMsgNo: "client-msg-9001",
		RunID: "run-1", EventKey: "main", MsgEventSeq: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		ChannelID   string `json:"channel_id"`
		MessageID   uint64 `json:"message_id"`
		RunID       string `json:"run_id"`
		EventType   string `json:"event_type"`
		EventKey    string `json:"event_key"`
		MsgEventSeq uint64 `json:"msg_event_seq"`
		Payload     struct {
			RunID             string `json:"run_id"`
			EventType         string `json:"event_type"`
			EventKey          string `json:"event_key"`
			AuthoritySequence uint64 `json:"authority_sequence"`
			TextDelta         string `json:"text_delta"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(got, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.ChannelID != "u1@agent-a" || envelope.MessageID != 9001 || envelope.RunID != "run-1" ||
		envelope.EventType != "delta" || envelope.EventKey != "main" || envelope.MsgEventSeq != 2 ||
		envelope.Payload.RunID != "run-1" || envelope.Payload.EventType != "delta" || envelope.Payload.EventKey != "main" ||
		envelope.Payload.AuthoritySequence != 7 || envelope.Payload.TextDelta != "hello" {
		t.Fatalf("production EVENT data = %s", got)
	}
}

func TestMessageEventPushChunksOwnerRoutesAt256(t *testing.T) {
	routes := make([]presenceusecase.Route, 4097)
	for i := range routes {
		routes[i] = presenceusecase.Route{UID: "u1", OwnerNodeID: 7, SessionID: uint64(i + 1), ProtocolVersion: 6}
	}
	delivery := &recordingMessageEventDelivery{}
	facade := &messageEventAPIFacade{
		delivery: delivery,
		presence: staticMessageEventPresence{routes: map[string][]presenceusecase.Route{"u1": routes}},
	}
	err := facade.pushMessageEventPage(context.Background(), messageusecase.MessageEventAppend{EventType: messageusecase.EventTypeDelta}, messageusecase.MessageEventAppendResult{EventID: "evt-1"}, []byte(`{}`), 1, []string{"u1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.pushes) != 17 {
		t.Fatalf("push calls = %d, want 17", len(delivery.pushes))
	}
	total := 0
	for _, push := range delivery.pushes {
		if len(push.Routes) > 256 {
			t.Fatalf("route batch = %d, want <= 256", len(push.Routes))
		}
		total += len(push.Routes)
	}
	if total != 4097 {
		t.Fatalf("pushed routes = %d, want 4097", total)
	}
}

func TestMessageEventGroupSubscribersAreVisitedPageByPage(t *testing.T) {
	visitedFirst := false
	pager := &orderedMessageEventPager{firstVisited: &visitedFirst}
	facade := &messageEventAPIFacade{subscribers: pager}
	var pages [][]string
	err := facade.visitRecipientUIDPages(context.Background(), "g1", 2, func(uids []string) error {
		pages = append(pages, append([]string(nil), uids...))
		if len(pages) == 1 {
			visitedFirst = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 || len(pages[0]) != 512 || len(pages[1]) != 1 {
		t.Fatalf("visited page sizes = %v", []int{len(pages[0]), len(pages[1])})
	}
}

func TestMessageEventRealtimeFailureDoesNotFailDurableAppend(t *testing.T) {
	store := &stubMessageEventStore{applied: true}
	delivery := &recordingMessageEventDelivery{err: errors.New("retry exhausted")}
	facade := &messageEventAPIFacade{
		App:      messageusecase.New(messageusecase.Options{EventStore: store}),
		delivery: delivery,
		presence: staticMessageEventPresence{routes: map[string][]presenceusecase.Route{
			"u1": {{UID: "u1", OwnerNodeID: 7, SessionID: 1, ProtocolVersion: 6}},
		}},
	}
	result, err := facade.AppendMessageEvent(context.Background(), validMessageEventAppend())
	if err != nil || !result.Applied {
		t.Fatalf("AppendMessageEvent() = %+v, %v", result, err)
	}
	if len(delivery.pushes) != 1 {
		t.Fatalf("push calls = %d, want 1", len(delivery.pushes))
	}
}

func TestMessageEventDurableReplayDoesNotFanoutAgain(t *testing.T) {
	store := &stubMessageEventStore{applied: false}
	delivery := &recordingMessageEventDelivery{}
	facade := &messageEventAPIFacade{
		App:      messageusecase.New(messageusecase.Options{EventStore: store}),
		delivery: delivery,
		presence: staticMessageEventPresence{routes: map[string][]presenceusecase.Route{
			"u1": {{UID: "u1", OwnerNodeID: 7, SessionID: 1, ProtocolVersion: 6}},
		}},
	}
	result, err := facade.AppendMessageEvent(context.Background(), validMessageEventAppend())
	if err != nil || result.Applied {
		t.Fatalf("AppendMessageEvent() = %+v, %v", result, err)
	}
	if len(delivery.pushes) != 0 {
		t.Fatalf("push calls = %d, want 0", len(delivery.pushes))
	}
}

type staticMessageEventPresence struct {
	routes map[string][]presenceusecase.Route
}

func (p staticMessageEventPresence) EndpointsByUIDs(_ context.Context, uids []string) (map[string][]presenceusecase.Route, error) {
	out := make(map[string][]presenceusecase.Route, len(uids))
	for _, uid := range uids {
		out[uid] = append([]presenceusecase.Route(nil), p.routes[uid]...)
	}
	return out, nil
}

type recordingMessageEventDelivery struct {
	pushes []onlinedelivery.EventPush
	err    error
}

func (d *recordingMessageEventDelivery) PushEvent(_ context.Context, push onlinedelivery.EventPush) (onlinedelivery.OwnerPushResult, error) {
	d.pushes = append(d.pushes, push.Clone())
	return onlinedelivery.OwnerPushResult{}, d.err
}

type orderedMessageEventPager struct {
	calls        int
	firstVisited *bool
}

func (p *orderedMessageEventPager) ListSubscribersPage(_ context.Context, req channelusecase.MemberListPageRequest) (channelusecase.MemberListPageResult, error) {
	p.calls++
	if p.calls == 2 && !*p.firstVisited {
		return channelusecase.MemberListPageResult{}, errors.New("second page requested before first page was visited")
	}
	if p.calls == 1 {
		members := make([]channelusecase.Member, 512)
		for i := range members {
			members[i].UID = "u"
		}
		return channelusecase.MemberListPageResult{Members: members, HasMore: true, NextCursor: "u"}, nil
	}
	return channelusecase.MemberListPageResult{Members: []channelusecase.Member{{UID: "last"}}}, nil
}

type stubMessageEventStore struct {
	applied bool
}

func (s *stubMessageEventStore) LookupMessageEventAnchor(_ context.Context, _ string, _ int64, _ uint64) (messageusecase.MessageEventAnchor, bool, error) {
	return messageusecase.MessageEventAnchor{ChannelID: "u1@agent", ChannelType: 11, FromUID: "u1", MessageID: 9, ClientMsgNo: "cmn-1", RunID: "run-1", AuthorizationFence: "1"}, true, nil
}

func (s *stubMessageEventStore) AppendMessageEvent(_ context.Context, event messageusecase.MessageEventAppend) (messageusecase.MessageEventAppendResult, error) {
	return messageusecase.MessageEventAppendResult{Applied: s.applied, ChannelID: event.ChannelID, ChannelType: event.ChannelType, MessageID: event.MessageID, ClientMsgNo: event.ClientMsgNo, RunID: event.RunID, EventID: event.EventID, EventKey: event.EventKey, MsgEventSeq: event.AuthoritySequence}, nil
}

func (*stubMessageEventStore) GetMessageEventStatesBatch(context.Context, []messageusecase.MessageEventMessageKey, int) (map[messageusecase.MessageEventMessageKey][]messageusecase.MessageEventState, error) {
	return nil, nil
}

func validMessageEventAppend() messageusecase.MessageEventAppend {
	return messageusecase.MessageEventAppend{
		ChannelID: "u1@agent", ChannelType: 11, FromUID: "u1", MessageID: 9, ClientMsgNo: "cmn-1",
		RunID: "run-1", AuthorizationFence: "1", AuthoritySequence: 1,
		EventID: "evt-1", EventKey: "main", EventType: messageusecase.EventTypeDelta,
		Payload: []byte(`{"event_id":"evt-1","run_id":"run-1","base_message":{"conversation_id":"019c0000-0000-7000-8000-000000000001","message_id":"019c0000-0000-7000-8000-000000000002","client_msg_id":"cmn-1","committed_message_ref":"agent-run.test","message_sequence":1,"source_principal_id":"u1"},"event_key":"main","event_type":"delta","authority_sequence":1,"text_delta":"hi","authorization_fence":1,"occurred_at":"2023-11-14T22:13:20Z"}`),
	}
}
