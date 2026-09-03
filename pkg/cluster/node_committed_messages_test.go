package cluster

import (
	"context"
	"errors"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

type committedMessagesChannelService struct {
	noopChannelService
	id              ch.ChannelID
	after, scanHead uint64
	limit           int
	result          clusterchannels.CommittedMessagesResult
	found           bool
	err             error
}

func (s *committedMessagesChannelService) ReadCommittedMessages(_ context.Context, id ch.ChannelID, after uint64, limit int, scanHead uint64) (clusterchannels.CommittedMessagesResult, bool, error) {
	s.id, s.after, s.limit, s.scanHead = id, after, limit, scanHead
	return s.result, s.found, s.err
}

func TestNodeReadChannelCommittedMessagesUsesForegroundChannelService(t *testing.T) {
	want := clusterchannels.CommittedMessagesResult{ScanHead: 9, NextAfterMessageSeq: 9}
	service := &committedMessagesChannelService{result: want, found: true}
	node := &Node{channels: service}
	node.started.Store(true)
	id := ch.ChannelID{ID: "opaque", Type: 2}

	got, found, err := node.ReadChannelCommittedMessages(context.Background(), id, 3, 10, 9)

	if err != nil || !found || got.ScanHead != 9 || service.id != id || service.after != 3 || service.limit != 10 || service.scanHead != 9 {
		t.Fatalf("result=%+v found=%v err=%v service=%+v", got, found, err, service)
	}
}

func TestNodeReadChannelCommittedMessagesFailsClosedWithoutCapability(t *testing.T) {
	node := &Node{channels: noopChannelService{}}
	node.started.Store(true)
	_, _, err := node.ReadChannelCommittedMessages(context.Background(), ch.ChannelID{ID: "opaque", Type: 2}, 0, 1, 0)
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("ReadChannelCommittedMessages() error = %v", err)
	}
}
