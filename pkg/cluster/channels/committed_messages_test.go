package channels

import (
	"context"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/stretchr/testify/require"
)

func TestServiceReadCommittedMessagesPagesSparseSequencesInOneSnapshot(t *testing.T) {
	id := ch.ChannelID{ID: "sparse-recovery", Type: 2}
	base := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, base, id, 5)
	svc, err := NewService(Config{
		Runtime: &fakeRuntime{}, LocalNode: 1,
		Store:      &sparseCommittedMessagesFactory{base: base, include: map[uint64]bool{1: true, 3: true, 5: true}},
		MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 1)}),
	})
	require.NoError(t, err)

	first, found, err := svc.ReadCommittedMessages(context.Background(), id, 0, 2, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(5), first.ScanHead)
	require.Equal(t, uint64(1), first.FirstAvailableMessageSeq)
	require.False(t, first.RetentionGap)
	require.True(t, first.HasMore)
	require.Equal(t, uint64(3), first.NextAfterMessageSeq)
	require.Equal(t, []uint64{1, 3}, committedMessageSeqs(first.Messages))

	second, found, err := svc.ReadCommittedMessages(context.Background(), id, first.NextAfterMessageSeq, 2, first.ScanHead)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, second.HasMore)
	require.Equal(t, uint64(5), second.NextAfterMessageSeq)
	require.Equal(t, []uint64{5}, committedMessageSeqs(second.Messages))
}

func TestServiceReadCommittedMessagesFencesConcurrentAppendAtScanHead(t *testing.T) {
	id := ch.ChannelID{ID: "snapshot-recovery", Type: 2}
	base := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, base, id, 2)
	svc, err := NewService(Config{
		Runtime: &fakeRuntime{}, LocalNode: 1, Store: base,
		MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 1)}),
	})
	require.NoError(t, err)

	first, found, err := svc.ReadCommittedMessages(context.Background(), id, 0, 1, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2), first.ScanHead)
	require.True(t, first.HasMore)

	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 103, Payload: []byte("message-3")}}})
	require.NoError(t, err)

	second, found, err := svc.ReadCommittedMessages(context.Background(), id, first.NextAfterMessageSeq, 10, first.ScanHead)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []uint64{2}, committedMessageSeqs(second.Messages))
	require.Equal(t, uint64(2), second.NextAfterMessageSeq)
	require.False(t, second.HasMore)
}

func TestServiceReadCommittedMessagesExcludesPresentUncommittedSuffix(t *testing.T) {
	id := ch.ChannelID{ID: "committed-only", Type: 2}
	base := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, base, id, 3)
	runtime := &conversationHeadProbeRuntime{fakeRuntime: &fakeRuntime{}, probe: ch.RuntimeProbeResult{Channels: []ch.RuntimeProbeChannel{{
		ChannelID: id, ChannelEpoch: 2, LeaderEpoch: 3, Role: ch.RoleLeader, Status: ch.StatusActive, LEO: 3, HW: 2,
	}}}}
	svc, err := NewService(Config{
		Runtime: runtime, LocalNode: 1, Store: base,
		MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 2)}),
	})
	require.NoError(t, err)

	page, found, err := svc.ReadCommittedMessages(context.Background(), id, 0, 10, 0)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2), page.ScanHead)
	require.Equal(t, []uint64{1, 2}, committedMessageSeqs(page.Messages))
	require.False(t, page.HasMore)
}

func TestServiceReadCommittedMessagesReportsRetentionGapAndFirstAvailable(t *testing.T) {
	id := ch.ChannelID{ID: "retained-recovery", Type: 2}
	base := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, base, id, 4)
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AdoptRetentionBoundary(context.Background(), 2, "test")
	require.NoError(t, err)
	_, err = store.TrimMessagesThrough(context.Background(), 2, channelstore.RetentionTrimOptions{MaxMessages: 10})
	require.NoError(t, err)
	meta := committedMessageMeta(id, 1)
	meta.RetentionThroughSeq = 2
	svc, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, Store: base, MetaSource: NewStaticMetaSource([]ch.Meta{meta})})
	require.NoError(t, err)

	page, found, err := svc.ReadCommittedMessages(context.Background(), id, 0, 10, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, page.RetentionGap)
	require.Equal(t, uint64(3), page.FirstAvailableMessageSeq)
	require.Equal(t, []uint64{3, 4}, committedMessageSeqs(page.Messages))
	require.Equal(t, uint64(4), page.NextAfterMessageSeq)
	require.False(t, page.HasMore)

	page, found, err = svc.ReadCommittedMessages(context.Background(), id, 2, 10, page.ScanHead)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, page.RetentionGap)
	require.Equal(t, uint64(3), page.FirstAvailableMessageSeq)
	require.Equal(t, []uint64{3, 4}, committedMessageSeqs(page.Messages))

	store, err = base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AdoptRetentionBoundary(context.Background(), 4, "test")
	require.NoError(t, err)
	_, err = store.TrimMessagesThrough(context.Background(), 4, channelstore.RetentionTrimOptions{MaxMessages: 10})
	require.NoError(t, err)
	meta.RetentionThroughSeq = 4
	svc.metaSource = NewStaticMetaSource([]ch.Meta{meta})

	page, found, err = svc.ReadCommittedMessages(context.Background(), id, 0, 10, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, page.RetentionGap)
	require.Equal(t, uint64(5), page.FirstAvailableMessageSeq)
	require.Empty(t, page.Messages)
	require.Equal(t, uint64(4), page.ScanHead)
	require.Equal(t, uint64(4), page.NextAfterMessageSeq)
	require.False(t, page.HasMore)
}

func TestServiceReadCommittedMessagesRoutesToRemoteLeader(t *testing.T) {
	id := ch.ChannelID{ID: "remote-recovery", Type: 2}
	meta := committedMessageMeta(id, 1)
	meta.Leader, meta.Replicas, meta.ISR = 2, []ch.NodeID{1, 2}, []ch.NodeID{1, 2}
	network := clusternet.NewLocalNetwork()
	client := NewTransportClient(network)
	leaderStore := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, leaderStore, id, 3)
	leader, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 2, MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: leaderStore, Forward: client})
	require.NoError(t, err)
	RegisterServiceHandlers(network, 2, leader)
	origin, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: channelstore.NewMemoryFactory(), Forward: client})
	require.NoError(t, err)

	page, found, err := origin.ReadCommittedMessages(context.Background(), id, 0, 2, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []uint64{1, 2}, committedMessageSeqs(page.Messages))
	require.Equal(t, uint64(3), page.ScanHead)
	require.True(t, page.HasMore)
	require.True(t, page.Messages[0].SyncOnce)

	store, err := leaderStore.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 104}, {ID: 105}}})
	require.NoError(t, err)
	_, err = store.AdoptRetentionBoundary(context.Background(), 4, "test")
	require.NoError(t, err)
	_, err = store.TrimMessagesThrough(context.Background(), 4, channelstore.RetentionTrimOptions{MaxMessages: 10})
	require.NoError(t, err)
	meta.RetentionThroughSeq = 4
	leader.metaSource = NewStaticMetaSource([]ch.Meta{meta})
	origin.metaSource = NewStaticMetaSource([]ch.Meta{meta})

	terminal, found, err := origin.ReadCommittedMessages(context.Background(), id, page.NextAfterMessageSeq, 2, page.ScanHead)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), terminal.ScanHead)
	require.Equal(t, uint64(5), terminal.FirstAvailableMessageSeq)
	require.True(t, terminal.RetentionGap)
	require.Empty(t, terminal.Messages)
	require.Equal(t, uint64(3), terminal.NextAfterMessageSeq)
	require.False(t, terminal.HasMore)
}

func TestServiceReadCommittedMessagesFailsClosedWhenRetentionAdvancesDuringRead(t *testing.T) {
	id := ch.ChannelID{ID: "retention-race", Type: 2}
	base := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, base, id, 3)
	source := &advancingRetentionMetaSource{meta: committedMessageMeta(id, 1), advanceOnCall: 3}
	svc, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, Store: base, MetaSource: source})
	require.NoError(t, err)

	_, _, err = svc.ReadCommittedMessages(context.Background(), id, 0, 2, 0)

	require.ErrorIs(t, err, ch.ErrStaleMeta)
}

func TestServiceReadCommittedMessagesBadUnknownAndUnavailableSemantics(t *testing.T) {
	unknown, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, MetaSource: NewStaticMetaSource(nil), Store: channelstore.NewMemoryFactory()})
	require.NoError(t, err)
	page, found, err := unknown.ReadCommittedMessages(context.Background(), ch.ChannelID{ID: "unknown", Type: 2}, 0, 10, 0)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, CommittedMessagesResult{}, page)

	for _, input := range []struct {
		id              ch.ChannelID
		after, scanHead uint64
		limit           int
	}{
		{id: ch.ChannelID{Type: 2}, limit: 1},
		{id: ch.ChannelID{ID: "g"}, limit: 1},
		{id: ch.ChannelID{ID: "g", Type: 2}},
		{id: ch.ChannelID{ID: "g", Type: 2}, limit: MaxCommittedMessagesPageLimit + 1},
		{id: ch.ChannelID{ID: "g", Type: 2}, after: 3, scanHead: 2, limit: 1},
	} {
		_, _, err = unknown.ReadCommittedMessages(context.Background(), input.id, input.after, input.limit, input.scanHead)
		require.ErrorIs(t, err, ch.ErrInvalidConfig)
	}

	id := ch.ChannelID{ID: "quorum-no-probe", Type: 2}
	unavailable, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, Store: channelstore.NewMemoryFactory(), MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 2)})})
	require.NoError(t, err)
	_, _, err = unavailable.ReadCommittedMessages(context.Background(), id, 0, 1, 0)
	require.ErrorIs(t, err, ch.ErrNotReady)

	id = ch.ChannelID{ID: "head-ahead", Type: 2}
	base := channelstore.NewMemoryFactory()
	appendCommittedMessageRecords(t, base, id, 2)
	unavailable, err = NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, Store: base, MetaSource: NewStaticMetaSource([]ch.Meta{committedMessageMeta(id, 1)})})
	require.NoError(t, err)
	_, _, err = unavailable.ReadCommittedMessages(context.Background(), id, 1, 1, 3)
	require.ErrorIs(t, err, ch.ErrNotReady)
}

func TestCommittedMessagesCodecRejectsLegacyAndTrailingFrames(t *testing.T) {
	req := CommittedMessagesRequest{
		ChannelID: ch.ChannelID{ID: "opaque", Type: 2}, AfterMessageSeq: 7, Limit: 10, ScanHead: 20,
		ExpectedLeader: 1, ExpectedChannelEpoch: 2, ExpectedLeaderEpoch: 4, ExpectedMinISR: 2, ExpectedRetentionThroughSeq: 3,
	}
	encoded := encodeCommittedMessagesRequest(req)
	decoded, err := decodeCommittedMessagesRequest(encoded)
	require.NoError(t, err)
	require.Equal(t, req, decoded)

	legacy := append([]byte(nil), encoded...)
	legacy[0] = legacyCodecVersionV6
	_, err = decodeCommittedMessagesRequest(legacy)
	require.ErrorIs(t, err, errInvalidCodecFrame)
	_, err = decodeCommittedMessagesRequest(append(encoded, 1))
	require.Error(t, err)
}

func TestValidCommittedMessagesResultAllowsOnlyEmptyTerminalGapPastScanHead(t *testing.T) {
	id := ch.ChannelID{ID: "terminal-gap", Type: 2}
	req := CommittedMessagesRequest{
		ChannelID: id, AfterMessageSeq: 1, Limit: 10, ScanHead: 2,
		ExpectedRetentionThroughSeq: 4,
	}
	terminal := CommittedMessagesResult{
		ScanHead: 2, FirstAvailableMessageSeq: 5, RetentionGap: true,
		NextAfterMessageSeq: 2,
	}
	require.True(t, validCommittedMessagesResult(id, req, terminal))

	for _, invalid := range []CommittedMessagesResult{
		{ScanHead: 2, FirstAvailableMessageSeq: 5, RetentionGap: true, NextAfterMessageSeq: 2, HasMore: true},
		{ScanHead: 2, FirstAvailableMessageSeq: 5, RetentionGap: true, NextAfterMessageSeq: 2, Messages: []ch.Message{{MessageID: 7, MessageSeq: 2, ChannelID: id.ID, ChannelType: id.Type}}},
		{ScanHead: 2, FirstAvailableMessageSeq: 5, RetentionGap: true, NextAfterMessageSeq: 2, Messages: []ch.Message{{MessageID: 7, MessageSeq: 5, ChannelID: id.ID, ChannelType: id.Type}}},
	} {
		require.False(t, validCommittedMessagesResult(id, req, invalid), "invalid=%+v", invalid)
	}
}

func appendCommittedMessageRecords(t *testing.T, factory channelstore.Factory, id ch.ChannelID, count int) {
	t.Helper()
	store, err := factory.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	records := make([]ch.Record, count)
	for index := range records {
		records[index] = ch.Record{ID: uint64(101 + index), FromUID: "sender", ClientMsgNo: "client", SyncOnce: index%2 == 0, Payload: []byte("message")}
	}
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: records})
	require.NoError(t, err)
}

func committedMessageSeqs(messages []ch.Message) []uint64 {
	sequences := make([]uint64, len(messages))
	for index, message := range messages {
		sequences[index] = message.MessageSeq
	}
	return sequences
}

type sparseCommittedMessagesFactory struct {
	base    channelstore.Factory
	include map[uint64]bool
}

func (f *sparseCommittedMessagesFactory) ChannelStore(key ch.ChannelKey, id ch.ChannelID) (channelstore.ChannelStore, error) {
	store, err := f.base.ChannelStore(key, id)
	if err != nil {
		return nil, err
	}
	return &sparseCommittedMessagesStore{ChannelStore: store, include: f.include}, nil
}

type sparseCommittedMessagesStore struct {
	channelstore.ChannelStore
	include map[uint64]bool
}

type advancingRetentionMetaSource struct {
	meta          ch.Meta
	calls         int
	advanceOnCall int
}

func (s *advancingRetentionMetaSource) ResolveChannelMeta(context.Context, ch.ChannelID) (ch.Meta, error) {
	s.calls++
	meta := s.meta
	if s.calls >= s.advanceOnCall {
		meta.RetentionThroughSeq++
	}
	return meta, nil
}

func (s *sparseCommittedMessagesStore) ReadCommitted(ctx context.Context, req channelstore.ReadCommittedRequest) (channelstore.ReadCommittedResult, error) {
	read, err := s.ChannelStore.ReadCommitted(ctx, channelstore.ReadCommittedRequest{
		FromSeq: req.FromSeq, MaxSeq: req.MaxSeq, MinSeq: req.MinSeq, Limit: MaxCommittedMessagesPageLimit + 1, MaxBytes: req.MaxBytes,
	})
	if err != nil {
		return channelstore.ReadCommittedResult{}, err
	}
	filtered := make([]ch.Message, 0, req.Limit)
	for _, message := range read.Messages {
		if s.include[message.MessageSeq] {
			filtered = append(filtered, message)
			if len(filtered) == req.Limit {
				break
			}
		}
	}
	return channelstore.ReadCommittedResult{Messages: filtered}, nil
}
