package channels

import (
	"context"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/stretchr/testify/require"
)

func TestServiceReadCommittedMessageReturnsExactImmutableTuple(t *testing.T) {
	id := ch.ChannelID{ID: "exact-proof", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{
		ID: 71, Setting: 3, FromUID: "sender", ClientMsgNo: "client-1",
		ServerTimestampMS: 1700000000123, SyncOnce: true, Payload: []byte("proof"),
	}}})
	require.NoError(t, err)
	guard := &exactCommittedMessageReadFactory{base: base}
	svc, err := NewService(Config{
		Runtime: &fakeRuntime{}, LocalNode: 1, Store: guard,
		MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 1)}),
	})
	require.NoError(t, err)

	message, found, err := svc.ReadCommittedMessage(context.Background(), id, 71, 1)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ch.Message{
		MessageID: 71, MessageSeq: 1, ChannelID: id.ID, ChannelType: id.Type,
		Setting: 3, FromUID: "sender", ClientMsgNo: "client-1",
		ServerTimestampMS: 1700000000123, SyncOnce: true, Payload: []byte("proof"),
	}, message)
	require.Equal(t, 1, guard.reads)
	require.Equal(t, channelstore.ReadCommittedRequest{FromSeq: 1, MaxSeq: 1, MinSeq: 1, Limit: 1, MaxBytes: maxInt()}, guard.request)
}

func TestServiceReadCommittedMessageRejectsUncommittedAndMismatchedIdentity(t *testing.T) {
	id := ch.ChannelID{ID: "proof-fences", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 71}, {ID: 72}}})
	require.NoError(t, err)
	runtime := &runtimeHWProbeRuntime{fakeRuntime: &fakeRuntime{}, probe: ch.RuntimeProbeResult{Channels: []ch.RuntimeProbeChannel{{
		ChannelID: id, ChannelEpoch: 2, LeaderEpoch: 3, Role: ch.RoleLeader, Status: ch.StatusActive, LEO: 2, HW: 1,
	}}}}
	svc, err := NewService(Config{
		Runtime: runtime, LocalNode: 1, Store: base,
		MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 2)}),
	})
	require.NoError(t, err)

	for _, identity := range []struct{ messageID, messageSeq uint64 }{
		{messageID: 72, messageSeq: 2}, // present but above HW
		{messageID: 72, messageSeq: 1}, // wrong ID for committed sequence
		{messageID: 71, messageSeq: 2}, // wrong sequence for ID and above HW
	} {
		message, found, err := svc.ReadCommittedMessage(context.Background(), id, identity.messageID, identity.messageSeq)
		require.NoError(t, err)
		require.False(t, found)
		require.Equal(t, ch.Message{}, message)
	}
}

func TestServiceReadCommittedMessageReturnsNotFoundAfterPhysicalRetention(t *testing.T) {
	id := ch.ChannelID{ID: "retained-proof", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 71, Payload: []byte("removed")}}})
	require.NoError(t, err)
	_, err = store.AdoptRetentionBoundary(context.Background(), 1, "test")
	require.NoError(t, err)
	trimmed, err := store.TrimMessagesThrough(context.Background(), 1, channelstore.RetentionTrimOptions{MaxMessages: 1})
	require.NoError(t, err)
	require.Equal(t, 1, trimmed.Deleted)
	svc, err := NewService(Config{
		Runtime: &fakeRuntime{}, LocalNode: 1, Store: base,
		MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 1)}),
	})
	require.NoError(t, err)

	message, found, err := svc.ReadCommittedMessage(context.Background(), id, 71, 1)

	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, ch.Message{}, message)
}

func TestServiceReadCommittedMessageRoutesToExactRemoteLeader(t *testing.T) {
	id := ch.ChannelID{ID: "remote-proof", Type: 2}
	meta := committedMessageMeta(id, 1)
	meta.Leader, meta.Replicas, meta.ISR = 2, []ch.NodeID{1, 2}, []ch.NodeID{1, 2}
	network := clusternet.NewLocalNetwork()
	client := NewTransportClient(network)
	leaderStore := channelstore.NewMemoryFactory()
	store, err := leaderStore.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 81, FromUID: "remote", SyncOnce: true, Payload: []byte("body")}}})
	require.NoError(t, err)
	leader, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 2, MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: leaderStore, Forward: client})
	require.NoError(t, err)
	RegisterServiceHandlers(network, 2, leader)
	origin, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: channelstore.NewMemoryFactory(), Forward: client})
	require.NoError(t, err)

	message, found, err := origin.ReadCommittedMessage(context.Background(), id, 81, 1)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(81), message.MessageID)
	require.Equal(t, []byte("body"), message.Payload)
	require.True(t, message.SyncOnce)

	_, err = client.ForwardCommittedMessage(context.Background(), 2, CommittedMessageRequest{
		ChannelID: id, MessageID: 81, MessageSeq: 1, ExpectedLeader: 2,
		ExpectedChannelEpoch: 1, ExpectedLeaderEpoch: 3, ExpectedMinISR: 1,
	})
	require.ErrorIs(t, err, ch.ErrStaleMeta)
}

func TestServiceReadCommittedMessageUnknownBadAndUnavailableSemantics(t *testing.T) {
	unknown, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, MetaSource: NewStaticMetaSource(nil), Store: channelstore.NewMemoryFactory()})
	require.NoError(t, err)
	message, found, err := unknown.ReadCommittedMessage(context.Background(), ch.ChannelID{ID: "unknown", Type: 2}, 1, 1)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, ch.Message{}, message)

	for _, input := range []struct {
		id                    ch.ChannelID
		messageID, messageSeq uint64
	}{
		{id: ch.ChannelID{Type: 2}, messageID: 1, messageSeq: 1},
		{id: ch.ChannelID{ID: "g", Type: 2}, messageSeq: 1},
		{id: ch.ChannelID{ID: "g", Type: 2}, messageID: 1},
	} {
		_, _, err = unknown.ReadCommittedMessage(context.Background(), input.id, input.messageID, input.messageSeq)
		require.ErrorIs(t, err, ch.ErrInvalidConfig)
	}

}

func TestCommittedMessageCodecRejectsLegacyAndTrailingFrames(t *testing.T) {
	req := CommittedMessageRequest{
		ChannelID: ch.ChannelID{ID: "opaque", Type: 2}, MessageID: 7, MessageSeq: 3,
		ExpectedLeader: 1, ExpectedChannelEpoch: 2, ExpectedLeaderEpoch: 4, ExpectedMinISR: 2,
	}
	encoded := encodeCommittedMessageRequest(req)
	decoded, err := decodeCommittedMessageRequest(encoded)
	require.NoError(t, err)
	require.Equal(t, req, decoded)

	legacy := append([]byte(nil), encoded...)
	legacy[0] = legacyCodecVersionV6
	_, err = decodeCommittedMessageRequest(legacy)
	require.ErrorIs(t, err, errInvalidCodecFrame)
	_, err = decodeCommittedMessageRequest(append(encoded, 1))
	require.Error(t, err)
}

func committedMessageMeta(id ch.ChannelID, minISR int) ch.Meta {
	replicas := []ch.NodeID{1}
	if minISR > 1 {
		replicas = []ch.NodeID{1, 2}
	}
	return ch.Meta{
		ID: id, Epoch: 2, LeaderEpoch: 3, Leader: 1,
		Replicas: replicas, ISR: append([]ch.NodeID(nil), replicas...), MinISR: minISR, Status: ch.StatusActive,
	}
}

type exactCommittedMessageReadFactory struct {
	base    channelstore.Factory
	reads   int
	request channelstore.ReadCommittedRequest
}

func (f *exactCommittedMessageReadFactory) ChannelStore(key ch.ChannelKey, id ch.ChannelID) (channelstore.ChannelStore, error) {
	store, err := f.base.ChannelStore(key, id)
	if err != nil {
		return nil, err
	}
	return &exactCommittedMessageReadStore{ChannelStore: store, parent: f}, nil
}

type exactCommittedMessageReadStore struct {
	channelstore.ChannelStore
	parent *exactCommittedMessageReadFactory
}

func (s *exactCommittedMessageReadStore) ReadCommitted(ctx context.Context, req channelstore.ReadCommittedRequest) (channelstore.ReadCommittedResult, error) {
	s.parent.reads++
	s.parent.request = req
	return s.ChannelStore.ReadCommitted(ctx, req)
}
