package app

import (
	"context"
	"testing"

	channelmembers "github.com/WuKongIM/WuKongIM/internal/contracts/channelmembers"
	conversationusecase "github.com/WuKongIM/WuKongIM/internal/usecase/conversation"
	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestConversationLegacyMessageReaderIncludesTypedEventProjection(t *testing.T) {
	messageReader := legacyConversationMessageBatchReader{messages: []messageusecase.SyncedMessage{{
		Setting: 2, MessageID: 9, MessageSeq: 3, ClientMsgNo: "c1",
		ChannelID: "g1", ChannelType: 2, Payload: []byte("base"),
	}}}
	eventKey := messageusecase.MessageEventMessageKey{ChannelID: "g1", ChannelType: 2, ClientMsgNo: "c1"}
	messages := messageusecase.New(messageusecase.Options{
		Reader:              messageReader,
		Memberships:         legacyConversationMembership{},
		MembershipAuthority: legacyConversationMembership{},
		EventStore: legacyConversationEventStore{states: map[messageusecase.MessageEventMessageKey][]messageusecase.MessageEventState{
			eventKey: {
				{EventKey: messageusecase.EventKeyDefault, Status: messageusecase.EventStatusClosed, LastMsgEventSeq: 2, LastEventType: messageusecase.EventTypeFinish, SnapshotPayload: []byte(`{"kind":"text","text":"done"}`), EndReason: 3},
			},
		}},
	})
	reader := conversationLegacyMessageReader{messages: messages}

	result, err := reader.ReadLegacyMessagesBatch(context.Background(), "u1", []conversationusecase.LegacyMessageQuery{{
		ChannelID: "g1", ChannelType: 2, Limit: 1,
	}})
	if err != nil {
		t.Fatalf("ReadLegacyMessagesBatch(): %v", err)
	}
	if len(result) != 1 || len(result[0].Messages) != 1 {
		t.Fatalf("result = %#v, want one message", result)
	}
	msg := result[0].Messages[0]
	if msg.EventMeta == nil || !msg.EventMeta.Completed || len(msg.EventMeta.Events) != 1 || msg.EventMeta.Events[0].Snapshot == nil {
		t.Fatalf("legacy event meta = %#v", msg.EventMeta)
	}
}

type legacyConversationMessageBatchReader struct {
	messages []messageusecase.SyncedMessage
}

func (r legacyConversationMessageBatchReader) SyncMessages(context.Context, messageusecase.ChannelMessageQuery) (messageusecase.ChannelMessagePage, error) {
	return messageusecase.ChannelMessagePage{Messages: r.messages}, nil
}

func (r legacyConversationMessageBatchReader) SyncMessagesBatch(_ context.Context, queries []messageusecase.ChannelMessageQuery) ([]messageusecase.ChannelMessageReadResult, error) {
	result := make([]messageusecase.ChannelMessageReadResult, len(queries))
	for index := range result {
		result[index].Page.Messages = append([]messageusecase.SyncedMessage(nil), r.messages...)
	}
	return result, nil
}

type legacyConversationMembership struct{}

func (legacyConversationMembership) GetUserChannelMembership(_ context.Context, uid, channelID string, channelType int64) (metadb.UserChannelMembership, bool, error) {
	return metadb.UserChannelMembership{UID: uid, ChannelID: channelID, ChannelType: channelType, JoinSeq: 1}, true, nil
}

func (legacyConversationMembership) AuthorizeLiveMemberships(_ context.Context, memberships []channelmembers.LiveMembership) []channelmembers.LiveMembershipAuthorityResult {
	results := make([]channelmembers.LiveMembershipAuthorityResult, len(memberships))
	for index := range results {
		results[index] = channelmembers.LiveMembershipAuthorityResult{ChannelFound: true, Subscriber: true}
	}
	return results
}

func (legacyConversationMembership) TombstoneRevokedMembership(context.Context, channelmembers.LiveMembership, uint64, int64) error {
	return nil
}

type legacyConversationEventStore struct {
	states map[messageusecase.MessageEventMessageKey][]messageusecase.MessageEventState
}

func (legacyConversationEventStore) LookupMessageEventAnchor(context.Context, string, int64, uint64) (messageusecase.MessageEventAnchor, bool, error) {
	return messageusecase.MessageEventAnchor{}, false, nil
}

func (legacyConversationEventStore) AppendMessageEvent(context.Context, messageusecase.MessageEventAppend) (messageusecase.MessageEventAppendResult, error) {
	return messageusecase.MessageEventAppendResult{}, nil
}

func (s legacyConversationEventStore) GetMessageEventStatesBatch(_ context.Context, _ []messageusecase.MessageEventMessageKey, _ int) (map[messageusecase.MessageEventMessageKey][]messageusecase.MessageEventState, error) {
	return s.states, nil
}
