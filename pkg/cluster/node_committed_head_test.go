package cluster

import (
	"context"
	"errors"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type committedHeadChannelService struct {
	noopChannelService
	id    ch.ChannelID
	calls int
	seq   uint64
	err   error
}

func (s *committedHeadChannelService) ReadCommittedHead(_ context.Context, id ch.ChannelID) (uint64, error) {
	s.id = id
	s.calls++
	return s.seq, s.err
}

func TestNodeReadChannelCommittedHeadUsesForegroundChannelService(t *testing.T) {
	service := &committedHeadChannelService{seq: 71}
	node := &Node{channels: service}
	node.started.Store(true)
	id := ch.ChannelID{ID: "opaque", Type: 2}

	seq, err := node.ReadChannelCommittedHead(context.Background(), id)

	if err != nil || seq != 71 || service.calls != 1 || service.id != id {
		t.Fatalf("seq=%d err=%v calls=%d id=%+v", seq, err, service.calls, service.id)
	}
}

func TestNodeReadChannelCommittedHeadFailsClosedWithoutCapability(t *testing.T) {
	node := &Node{channels: noopChannelService{}}
	node.started.Store(true)
	_, err := node.ReadChannelCommittedHead(context.Background(), ch.ChannelID{ID: "opaque", Type: 2})
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("ReadChannelCommittedHead() error = %v", err)
	}
}
