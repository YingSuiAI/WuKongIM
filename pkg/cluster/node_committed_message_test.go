package cluster

import (
	"context"
	"errors"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type committedMessageChannelService struct {
	noopChannelService
	id                    ch.ChannelID
	messageID, messageSeq uint64
	message               ch.Message
	found                 bool
	err                   error
}

func (s *committedMessageChannelService) ReadCommittedMessage(_ context.Context, id ch.ChannelID, messageID, messageSeq uint64) (ch.Message, bool, error) {
	s.id, s.messageID, s.messageSeq = id, messageID, messageSeq
	return s.message, s.found, s.err
}

func TestNodeReadChannelCommittedMessageUsesForegroundChannelService(t *testing.T) {
	want := ch.Message{MessageID: 71, MessageSeq: 9, Payload: []byte("proof")}
	service := &committedMessageChannelService{message: want, found: true}
	node := &Node{channels: service}
	node.started.Store(true)
	id := ch.ChannelID{ID: "opaque", Type: 2}

	got, found, err := node.ReadChannelCommittedMessage(context.Background(), id, 71, 9)

	if err != nil || !found || got.MessageID != want.MessageID || service.id != id || service.messageID != 71 || service.messageSeq != 9 {
		t.Fatalf("message=%+v found=%v err=%v service=%+v", got, found, err, service)
	}
}

func TestNodeReadChannelCommittedMessageFailsClosedWithoutCapability(t *testing.T) {
	node := &Node{channels: noopChannelService{}}
	node.started.Store(true)
	_, _, err := node.ReadChannelCommittedMessage(context.Background(), ch.ChannelID{ID: "opaque", Type: 2}, 71, 9)
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("ReadChannelCommittedMessage() error = %v", err)
	}
}
