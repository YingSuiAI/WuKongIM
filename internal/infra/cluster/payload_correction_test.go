package cluster

import (
	"context"
	"errors"
	"testing"

	conversationusecase "github.com/WuKongIM/WuKongIM/internal/usecase/conversation"
	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
	"github.com/stretchr/testify/require"
)

// Existing mapping fixtures explicitly model channels without corrections.
// Production adapters do not infer absence from a missing capability.
func (*recordingReadNode) ApplyMessagePayloadCorrections(_ context.Context, messages []ch.Message) ([]ch.Message, error) {
	return messages, nil
}

type selectivelyDeletedPayloads struct{}

func (selectivelyDeletedPayloads) ApplyMessagePayloadCorrections(_ context.Context, messages []ch.Message) ([]ch.Message, error) {
	for _, message := range messages {
		if message.ChannelID == "deleted" {
			return nil, messagepayload.ErrNotFound
		}
	}
	out := append([]ch.Message(nil), messages...)
	for index := range out {
		out[index].Payload = []byte("current")
	}
	return out, nil
}

type selectiveConversationNode struct{ *conversationNodeFake }

func (*selectiveConversationNode) ApplyMessagePayloadCorrections(ctx context.Context, messages []ch.Message) ([]ch.Message, error) {
	return (selectivelyDeletedPayloads{}).ApplyMessagePayloadCorrections(ctx, messages)
}

type selectiveManagementNode struct {
	*recordingManagementMessageNode
}

func (*selectiveManagementNode) ApplyMessagePayloadCorrections(ctx context.Context, messages []ch.Message) ([]ch.Message, error) {
	return (selectivelyDeletedPayloads{}).ApplyMessagePayloadCorrections(ctx, messages)
}

func TestCurrentPayloadDeletedHeadDoesNotBlockUnrelatedConversation(t *testing.T) {
	node := &selectiveConversationNode{&conversationNodeFake{heads: []clusterchannels.ConversationHeadResult{
		{Head: clusterchannels.ConversationHead{Found: true, Message: ch.Message{ChannelID: "deleted", ChannelType: 2, MessageSeq: 1}}},
		{Head: clusterchannels.ConversationHead{Found: true, Message: ch.Message{ChannelID: "active", ChannelType: 2, MessageSeq: 2}}},
	}}}
	got, err := NewConversationStore(node).HydrateConversationHeads(context.Background(), "human", []metadb.UserChannelMembership{{ChannelID: "deleted", ChannelType: 2}, {ChannelID: "active", ChannelType: 2}})
	require.NoError(t, err)
	require.Equal(t, conversationusecase.HydrationRetryable, got[0].Outcome)
	require.Equal(t, conversationusecase.HydrationOK, got[1].Outcome)
	require.Equal(t, []byte("current"), got[1].LastMessage.Payload)
}

func TestCurrentPayloadManagerSkipsPurgedRowWithoutBlockingActiveRows(t *testing.T) {
	reader := NewManagementMessageReader(&selectiveManagementNode{&recordingManagementMessageNode{}})
	got, err := reader.currentManagementMessages(context.Background(), []managementusecase.Message{{ChannelID: "deleted", ChannelType: 2, MessageSeq: 1}, {ChannelID: "active", ChannelType: 2, MessageSeq: 2}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "active", got[0].ChannelID)
	require.Equal(t, []byte("current"), got[0].Payload)
}
func (*recordingChannelMetadataNode) ApplyMessagePayloadCorrections(_ context.Context, messages []ch.Message) ([]ch.Message, error) {
	return messages, nil
}
func (*conversationNodeFake) ApplyMessagePayloadCorrections(_ context.Context, messages []ch.Message) ([]ch.Message, error) {
	return messages, nil
}
func (*recordingManagementMessageNode) ApplyMessagePayloadCorrections(_ context.Context, messages []ch.Message) ([]ch.Message, error) {
	return messages, nil
}
func (*routedManagementMessageNode) ApplyMessagePayloadCorrections(_ context.Context, messages []ch.Message) ([]ch.Message, error) {
	return messages, nil
}

type failingCurrentPayloadReader struct{ err error }

func (f failingCurrentPayloadReader) ApplyMessagePayloadCorrections(context.Context, []ch.Message) ([]ch.Message, error) {
	return nil, f.err
}

func TestCurrentPayloadReaderNeverFallsBackAfterAuthorityFailure(t *testing.T) {
	messages := []ch.Message{{MessageID: 71, MessageSeq: 1, Payload: []byte("old body")}}
	for _, reader := range []any{struct{}{}, failingCurrentPayloadReader{messagepayload.ErrUnavailable}} {
		result, err := currentMessagePayloads(context.Background(), reader, messages)
		if !errors.Is(err, messagepayload.ErrUnavailable) || result != nil {
			t.Fatalf("authority failure returned original body: result=%v err=%v", result, err)
		}
	}
}
