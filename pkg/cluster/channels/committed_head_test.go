package channels

import (
	"context"
	"sync/atomic"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/stretchr/testify/require"
)

func TestServiceReadCommittedHeadUsesDurableLEOAndSurvivesFullRetention(t *testing.T) {
	id := ch.ChannelID{ID: "retained-head", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{
		{ID: 1, Payload: []byte("one")},
		{ID: 2, Payload: []byte("two")},
	}})
	require.NoError(t, err)
	_, err = store.AdoptRetentionBoundary(context.Background(), 2, "test")
	require.NoError(t, err)
	trimmed, err := store.TrimMessagesThrough(context.Background(), 2, channelstore.RetentionTrimOptions{MaxMessages: 10})
	require.NoError(t, err)
	require.Equal(t, 2, trimmed.Deleted)

	guard := &committedHeadGuardFactory{base: base}
	runtime := &runtimeHWProbeRuntime{fakeRuntime: &fakeRuntime{}, probe: ch.RuntimeProbeResult{Channels: []ch.RuntimeProbeChannel{{
		ChannelID: id, ChannelEpoch: 4, LeaderEpoch: 7,
		Role: ch.RoleLeader, Status: ch.StatusActive, LEO: 2, HW: 1,
	}}}}
	svc, err := NewService(Config{
		Runtime:   runtime,
		LocalNode: 1,
		MetaSource: NewStaticMetaSource([]ch.Meta{{
			ID: id, Epoch: 4, LeaderEpoch: 7, Leader: 1,
			Replicas: []ch.NodeID{1}, ISR: []ch.NodeID{1}, MinISR: 1, Status: ch.StatusActive,
		}}),
		Store: guard,
	})
	require.NoError(t, err)

	seq, err := svc.ReadCommittedHead(context.Background(), id)

	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
	require.Equal(t, 1, runtime.probeCalls)
	require.Equal(t, int64(1), guard.loads.Load())
	require.Equal(t, int64(0), guard.contentReads.Load())
	require.Equal(t, int64(0), guard.retentionReads.Load())
	require.Equal(t, int64(1), guard.closes.Load())
}

func TestServiceReadCommittedHeadUsesLiveLeaderHWForQuorumChannel(t *testing.T) {
	id := ch.ChannelID{ID: "live-quorum-head", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{
		{ID: 1}, {ID: 2}, {ID: 3},
	}})
	require.NoError(t, err)
	runtime := &runtimeHWProbeRuntime{
		fakeRuntime: &fakeRuntime{},
		probe: ch.RuntimeProbeResult{Channels: []ch.RuntimeProbeChannel{{
			ChannelID: id, ChannelEpoch: 3, LeaderEpoch: 5,
			Role: ch.RoleLeader, Status: ch.StatusActive, LEO: 3, HW: 2,
		}}},
	}
	svc, err := NewService(Config{
		Runtime:   runtime,
		LocalNode: 1,
		MetaSource: NewStaticMetaSource([]ch.Meta{{
			ID: id, Epoch: 3, LeaderEpoch: 5, Leader: 1,
			Replicas: []ch.NodeID{1, 2, 3}, ISR: []ch.NodeID{1, 2, 3}, MinISR: 2, Status: ch.StatusActive,
		}}),
		Store: base,
	})
	require.NoError(t, err)

	seq, err := svc.ReadCommittedHead(context.Background(), id)

	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
	require.Equal(t, 1, runtime.probeCalls)
}

func TestServiceReadCommittedHeadActivatesColdRuntimeBeforeUsingLaggingCheckpoint(t *testing.T) {
	id := ch.ChannelID{ID: "evicted-quorum-head", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 1}, {ID: 2}}})
	require.NoError(t, err)
	guard := &committedHeadGuardFactory{base: base}
	recovered := ch.RuntimeProbeResult{Channels: []ch.RuntimeProbeChannel{{
		ChannelID: id, ChannelEpoch: 2, LeaderEpoch: 3,
		Role: ch.RoleLeader, Status: ch.StatusActive, LEO: 2, HW: 2,
	}}}
	runtime := &runtimeHWProbeRuntime{
		fakeRuntime:     &fakeRuntime{},
		probe:           ch.RuntimeProbeResult{Checked: 1, Missing: []ch.ChannelID{id}},
		probeAfterApply: &recovered,
	}
	svc, err := NewService(Config{
		Runtime:   runtime,
		LocalNode: 1,
		MetaSource: NewStaticMetaSource([]ch.Meta{{
			ID: id, Epoch: 2, LeaderEpoch: 3, Leader: 1,
			Replicas: []ch.NodeID{1, 2}, ISR: []ch.NodeID{1, 2}, MinISR: 2, Status: ch.StatusActive,
		}}),
		Store: guard,
	})
	require.NoError(t, err)

	seq, err := svc.ReadCommittedHead(context.Background(), id)

	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
	require.Equal(t, 1, runtime.applyCalls)
	require.Equal(t, 2, runtime.probeCalls)
	require.Equal(t, int64(1), guard.loads.Load())
}

func TestServiceReadCommittedHeadRejectsColdRuntimeThatRemainsMissing(t *testing.T) {
	id := ch.ChannelID{ID: "cold-quorum-head-remains-missing", Type: 2}
	base := channelstore.NewMemoryFactory()
	store, err := base.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = store.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{{ID: 1}}})
	require.NoError(t, err)
	runtime := &runtimeHWProbeRuntime{
		fakeRuntime: &fakeRuntime{},
		probe:       ch.RuntimeProbeResult{Checked: 1, Missing: []ch.ChannelID{id}},
	}
	svc, err := NewService(Config{
		Runtime: runtime, LocalNode: 1,
		MetaSource: NewStaticMetaSource([]ch.Meta{{
			ID: id, Epoch: 2, LeaderEpoch: 3, Leader: 1,
			Replicas: []ch.NodeID{1, 2}, ISR: []ch.NodeID{1, 2}, MinISR: 2, Status: ch.StatusActive,
		}}),
		Store: base,
	})
	require.NoError(t, err)

	_, err = svc.ReadCommittedHead(context.Background(), id)

	require.ErrorIs(t, err, ch.ErrNotReady)
	require.Equal(t, 1, runtime.applyCalls)
	require.Equal(t, 2, runtime.probeCalls)
}

func TestServiceReadCommittedHeadRejectsIncompleteRuntimeEvidence(t *testing.T) {
	id := ch.ChannelID{ID: "incomplete-runtime-evidence", Type: 2}
	svc, err := NewService(Config{
		Runtime:   &runtimeHWProbeRuntime{fakeRuntime: &fakeRuntime{}},
		LocalNode: 1,
		MetaSource: NewStaticMetaSource([]ch.Meta{{
			ID: id, Epoch: 2, LeaderEpoch: 3, Leader: 1,
			Replicas: []ch.NodeID{1, 2}, ISR: []ch.NodeID{1, 2}, MinISR: 2, Status: ch.StatusActive,
		}}),
		Store: channelstore.NewMemoryFactory(),
	})
	require.NoError(t, err)

	_, err = svc.ReadCommittedHead(context.Background(), id)

	require.ErrorIs(t, err, ch.ErrNotReady)
}

func TestServiceReadCommittedHeadRejectsMalformedActiveMeta(t *testing.T) {
	id := ch.ChannelID{ID: "malformed-active-meta", Type: 2}
	valid := ch.Meta{
		ID: id, Epoch: 2, LeaderEpoch: 3, Leader: 1,
		Replicas: []ch.NodeID{1, 2}, ISR: []ch.NodeID{1, 2}, MinISR: 2, Status: ch.StatusActive,
	}
	tests := []struct {
		name   string
		mutate func(*ch.Meta)
	}{
		{name: "zero channel epoch", mutate: func(meta *ch.Meta) { meta.Epoch = 0 }},
		{name: "zero leader epoch", mutate: func(meta *ch.Meta) { meta.LeaderEpoch = 0 }},
		{name: "zero leader", mutate: func(meta *ch.Meta) { meta.Leader = 0 }},
		{name: "empty replicas", mutate: func(meta *ch.Meta) { meta.Replicas = nil }},
		{name: "zero replica", mutate: func(meta *ch.Meta) { meta.Replicas = []ch.NodeID{1, 0} }},
		{name: "duplicate replica", mutate: func(meta *ch.Meta) { meta.Replicas = []ch.NodeID{1, 1, 2} }},
		{name: "empty isr", mutate: func(meta *ch.Meta) { meta.ISR = nil }},
		{name: "zero isr", mutate: func(meta *ch.Meta) { meta.ISR = []ch.NodeID{1, 0} }},
		{name: "duplicate isr", mutate: func(meta *ch.Meta) { meta.ISR = []ch.NodeID{1, 1} }},
		{name: "leader absent from replicas", mutate: func(meta *ch.Meta) { meta.Replicas = []ch.NodeID{2} }},
		{name: "leader absent from isr", mutate: func(meta *ch.Meta) { meta.ISR = []ch.NodeID{2}; meta.MinISR = 1 }},
		{name: "isr outside replicas", mutate: func(meta *ch.Meta) { meta.ISR = []ch.NodeID{1, 3} }},
		{name: "zero min isr", mutate: func(meta *ch.Meta) { meta.MinISR = 0 }},
		{name: "min isr exceeds isr", mutate: func(meta *ch.Meta) { meta.MinISR = 3 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			meta := valid
			meta.Replicas = append([]ch.NodeID(nil), valid.Replicas...)
			meta.ISR = append([]ch.NodeID(nil), valid.ISR...)
			test.mutate(&meta)
			svc, err := NewService(Config{
				Runtime: &fakeRuntime{}, LocalNode: 1,
				MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: channelstore.NewMemoryFactory(),
			})
			require.NoError(t, err)

			_, err = svc.ReadCommittedHead(context.Background(), id)
			require.ErrorIs(t, err, ch.ErrNotReady)

			err = svc.validateCommittedHeadRoute(context.Background(), CommittedHeadRequest{
				ChannelID: id, ExpectedLeader: 1, ExpectedChannelEpoch: 2, ExpectedLeaderEpoch: 3, ExpectedMinISR: 2,
			})
			require.ErrorIs(t, err, ch.ErrNotReady)
		})
	}
}

func TestServiceReadCommittedHeadForwardsToExactLeaderOverRPC(t *testing.T) {
	id := ch.ChannelID{ID: "remote-head", Type: 2}
	meta := ch.Meta{
		ID: id, Epoch: 6, LeaderEpoch: 8, Leader: 2,
		Replicas: []ch.NodeID{1, 2}, ISR: []ch.NodeID{1, 2}, MinISR: 1, Status: ch.StatusActive,
	}
	network := clusternet.NewLocalNetwork()
	client := NewTransportClient(network)
	leaderFactory := channelstore.NewMemoryFactory()
	leaderStore, err := leaderFactory.ChannelStore(ch.ChannelKeyForID(id), id)
	require.NoError(t, err)
	_, err = leaderStore.AppendLeader(context.Background(), channelstore.AppendLeaderRequest{Records: []ch.Record{
		{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
	}})
	require.NoError(t, err)
	leader, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 2, MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: leaderFactory, Forward: client})
	require.NoError(t, err)
	RegisterServiceHandlers(network, 2, leader)
	originGuard := &committedHeadGuardFactory{base: channelstore.NewMemoryFactory()}
	origin, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, MetaSource: NewStaticMetaSource([]ch.Meta{meta}), Store: originGuard, Forward: client})
	require.NoError(t, err)

	seq, err := origin.ReadCommittedHead(context.Background(), id)

	require.NoError(t, err)
	require.Equal(t, uint64(4), seq)
	require.Equal(t, int64(0), originGuard.loads.Load())

	_, err = client.ForwardCommittedHead(context.Background(), 2, CommittedHeadRequest{
		ChannelID: id, ExpectedLeader: 2, ExpectedChannelEpoch: 5, ExpectedLeaderEpoch: 8, ExpectedMinISR: 1,
	})
	require.ErrorIs(t, err, ch.ErrStaleMeta)
	_, err = client.ForwardCommittedHead(context.Background(), 2, CommittedHeadRequest{
		ChannelID: id, ExpectedLeader: 1, ExpectedChannelEpoch: 6, ExpectedLeaderEpoch: 8, ExpectedMinISR: 1,
	})
	require.ErrorIs(t, err, ch.ErrNotLeader)
	_, err = client.ForwardCommittedHead(context.Background(), 2, CommittedHeadRequest{
		ChannelID: id, ExpectedLeader: 2, ExpectedChannelEpoch: 6, ExpectedLeaderEpoch: 8, ExpectedMinISR: 0,
	})
	require.ErrorIs(t, err, ch.ErrInvalidConfig)
	_, err = client.ForwardCommittedHead(context.Background(), 2, CommittedHeadRequest{
		ChannelID: id, ExpectedLeader: 2, ExpectedChannelEpoch: 0, ExpectedLeaderEpoch: 8, ExpectedMinISR: 1,
	})
	require.ErrorIs(t, err, ch.ErrInvalidConfig)
	_, err = client.ForwardCommittedHead(context.Background(), 2, CommittedHeadRequest{
		ChannelID: id, ExpectedLeader: 2, ExpectedChannelEpoch: 6, ExpectedLeaderEpoch: 0, ExpectedMinISR: 1,
	})
	require.ErrorIs(t, err, ch.ErrInvalidConfig)
}

func TestServiceReadCommittedHeadUnknownAndUnavailableSemantics(t *testing.T) {
	unknown, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 1, MetaSource: NewStaticMetaSource(nil), Store: channelstore.NewMemoryFactory()})
	require.NoError(t, err)
	seq, err := unknown.ReadCommittedHead(context.Background(), ch.ChannelID{ID: "unknown", Type: 2})
	require.NoError(t, err)
	require.Zero(t, seq)

	id := ch.ChannelID{ID: "remote-no-capability", Type: 2}
	remote, err := NewService(Config{
		Runtime: &fakeRuntime{}, LocalNode: 1,
		MetaSource: NewStaticMetaSource([]ch.Meta{{
			ID: id, Epoch: 1, LeaderEpoch: 1, Leader: 2,
			Replicas: []ch.NodeID{2}, ISR: []ch.NodeID{2}, MinISR: 1, Status: ch.StatusActive,
		}}),
		Forward: &recordingLastVisibleForward{},
	})
	require.NoError(t, err)
	_, err = remote.ReadCommittedHead(context.Background(), id)
	require.ErrorIs(t, err, ch.ErrNotReady)

	for _, invalid := range []ch.ChannelID{{}, {ID: "channel"}} {
		_, err = remote.ReadCommittedHead(context.Background(), invalid)
		require.ErrorIs(t, err, ch.ErrInvalidConfig)
	}
}

func TestCommittedHeadCodecRejectsLegacyAndTrailingFrames(t *testing.T) {
	req := CommittedHeadRequest{
		ChannelID: ch.ChannelID{ID: "opaque", Type: 2}, ExpectedLeader: 3,
		ExpectedChannelEpoch: 4, ExpectedLeaderEpoch: 5, ExpectedMinISR: 2,
	}
	encoded := encodeCommittedHeadRequest(req)
	decoded, err := decodeCommittedHeadRequest(encoded)
	require.NoError(t, err)
	require.Equal(t, req, decoded)

	legacy := append([]byte(nil), encoded...)
	legacy[0] = legacyCodecVersionV6
	_, err = decodeCommittedHeadRequest(legacy)
	require.ErrorIs(t, err, errInvalidCodecFrame)

	_, err = decodeCommittedHeadRequest(append(encoded, 1))
	require.Error(t, err)
}

type committedHeadGuardFactory struct {
	base           channelstore.Factory
	afterFirstLoad func()
	loads          atomic.Int64
	contentReads   atomic.Int64
	retentionReads atomic.Int64
	closes         atomic.Int64
}

func (f *committedHeadGuardFactory) ChannelStore(key ch.ChannelKey, id ch.ChannelID) (channelstore.ChannelStore, error) {
	store, err := f.base.ChannelStore(key, id)
	if err != nil {
		return nil, err
	}
	return &committedHeadGuardStore{ChannelStore: store, parent: f}, nil
}

type committedHeadGuardStore struct {
	channelstore.ChannelStore
	parent *committedHeadGuardFactory
}

func (s *committedHeadGuardStore) Load(ctx context.Context) (channelstore.InitialState, error) {
	call := s.parent.loads.Add(1)
	state, err := s.ChannelStore.Load(ctx)
	if call == 1 && s.parent.afterFirstLoad != nil {
		s.parent.afterFirstLoad()
	}
	return state, err
}

func (s *committedHeadGuardStore) ReadCommitted(ctx context.Context, req channelstore.ReadCommittedRequest) (channelstore.ReadCommittedResult, error) {
	s.parent.contentReads.Add(1)
	return s.ChannelStore.ReadCommitted(ctx, req)
}

func (s *committedHeadGuardStore) LoadRetentionState(ctx context.Context) (channelstore.RetentionState, error) {
	s.parent.retentionReads.Add(1)
	return s.ChannelStore.LoadRetentionState(ctx)
}

func (s *committedHeadGuardStore) Close() error {
	s.parent.closes.Add(1)
	return s.ChannelStore.Close()
}
