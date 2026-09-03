package cluster

import (
	"context"
	"errors"
	"testing"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type committedMessageNode struct {
	*recordingChannelMetadataNode
	id                    ch.ChannelID
	messageID, messageSeq uint64
	message               ch.Message
	found                 bool
	err                   error
}

func (n *committedMessageNode) ReadChannelCommittedMessage(_ context.Context, id ch.ChannelID, messageID, messageSeq uint64) (ch.Message, bool, error) {
	n.id, n.messageID, n.messageSeq = id, messageID, messageSeq
	return n.message, n.found, n.err
}

func TestChannelMetadataStoreReadsExactCommittedMessageThroughClusterFacade(t *testing.T) {
	payload := []byte("proof")
	node := &committedMessageNode{
		recordingChannelMetadataNode: &recordingChannelMetadataNode{}, found: true,
		message: ch.Message{MessageID: 51, MessageSeq: 7, ChannelID: "opaque", ChannelType: 2, FromUID: "u1", Payload: payload},
	}
	store := NewChannelMetadataStore(node, nil, nil)
	key := channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2}
	identity := channelusecase.CommittedMessageIdentity{MessageID: 51, MessageSeq: 7}

	message, found, err := store.ReadCommittedMessage(context.Background(), key, identity)

	if err != nil || !found || message.MessageID != 51 || node.id != (ch.ChannelID{ID: "opaque", Type: 2}) || node.messageID != 51 || node.messageSeq != 7 {
		t.Fatalf("message=%+v found=%v err=%v node=%+v", message, found, err, node)
	}
	payload[0] = 'x'
	if string(message.Payload) != "proof" {
		t.Fatalf("payload aliases runtime message: %q", message.Payload)
	}
}

func TestChannelMetadataStoreCommittedMessageFailsClosedWithoutFacade(t *testing.T) {
	store := NewChannelMetadataStore(&recordingChannelMetadataNode{}, nil, nil)
	_, _, err := store.ReadCommittedMessage(context.Background(), channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2}, channelusecase.CommittedMessageIdentity{MessageID: 51, MessageSeq: 7})
	if !errors.Is(err, channelusecase.ErrCommittedMessageUnavailable) {
		t.Fatalf("ReadCommittedMessage() error = %v", err)
	}
}
