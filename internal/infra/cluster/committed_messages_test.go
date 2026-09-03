package cluster

import (
	"context"
	"errors"
	"testing"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterchannels "github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

type committedMessagesNode struct {
	*recordingChannelMetadataNode
	id              ch.ChannelID
	after, scanHead uint64
	limit           int
	result          clusterchannels.CommittedMessagesResult
	found           bool
	err             error
}

func (n *committedMessagesNode) ReadChannelCommittedMessages(_ context.Context, id ch.ChannelID, after uint64, limit int, scanHead uint64) (clusterchannels.CommittedMessagesResult, bool, error) {
	n.id, n.after, n.limit, n.scanHead = id, after, limit, scanHead
	return n.result, n.found, n.err
}

func TestChannelMetadataStoreReadsCommittedMessagesThroughClusterFacade(t *testing.T) {
	node := &committedMessagesNode{recordingChannelMetadataNode: &recordingChannelMetadataNode{}, found: true, result: clusterchannels.CommittedMessagesResult{
		Messages: []ch.Message{{MessageID: 7, MessageSeq: 3, ChannelID: "opaque", ChannelType: 2, Payload: []byte("proof")}},
		ScanHead: 9, FirstAvailableMessageSeq: 1, NextAfterMessageSeq: 3, HasMore: true,
	}}
	store := NewChannelMetadataStore(node, nil, nil)
	query := channelusecase.CommittedMessagesQuery{AfterMessageSeq: 2, Limit: 1, ScanHead: 9}

	page, found, err := store.ReadCommittedMessages(context.Background(), channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2}, query)

	if err != nil || !found || node.id != (ch.ChannelID{ID: "opaque", Type: 2}) || node.after != 2 || node.limit != 1 || node.scanHead != 9 ||
		len(page.Messages) != 1 || string(page.Messages[0].Payload) != "proof" || page.ScanHead != 9 || !page.HasMore {
		t.Fatalf("page=%+v found=%v err=%v node=%+v", page, found, err, node)
	}
}

func TestChannelMetadataStoreCommittedMessagesFailsClosedWithoutFacade(t *testing.T) {
	store := NewChannelMetadataStore(&recordingChannelMetadataNode{}, nil, nil)
	_, _, err := store.ReadCommittedMessages(context.Background(), channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2}, channelusecase.CommittedMessagesQuery{Limit: 1})
	if !errors.Is(err, channelusecase.ErrCommittedMessagesUnavailable) {
		t.Fatalf("ReadCommittedMessages() error = %v", err)
	}
}
