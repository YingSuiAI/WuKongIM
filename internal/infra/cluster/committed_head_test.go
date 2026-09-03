package cluster

import (
	"context"
	"errors"
	"testing"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type committedHeadNode struct {
	*recordingChannelMetadataNode
	id    ch.ChannelID
	calls int
	seq   uint64
	err   error
}

func (n *committedHeadNode) ReadChannelCommittedHead(_ context.Context, id ch.ChannelID) (uint64, error) {
	n.id = id
	n.calls++
	return n.seq, n.err
}

func TestChannelMetadataStoreReadsCommittedHeadThroughClusterFacade(t *testing.T) {
	node := &committedHeadNode{recordingChannelMetadataNode: &recordingChannelMetadataNode{}, seq: 51}
	store := NewChannelMetadataStore(node, nil, nil)

	seq, err := store.ReadCommittedHead(context.Background(), channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2})

	if err != nil || seq != 51 || node.calls != 1 || node.id != (ch.ChannelID{ID: "opaque", Type: 2}) {
		t.Fatalf("seq=%d err=%v calls=%d id=%+v", seq, err, node.calls, node.id)
	}
}

func TestChannelMetadataStoreCommittedHeadFailsClosedWithoutFacade(t *testing.T) {
	store := NewChannelMetadataStore(&recordingChannelMetadataNode{}, nil, nil)
	_, err := store.ReadCommittedHead(context.Background(), channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2})
	if !errors.Is(err, channelusecase.ErrCommittedHeadUnavailable) {
		t.Fatalf("ReadCommittedHead() error = %v", err)
	}
}
