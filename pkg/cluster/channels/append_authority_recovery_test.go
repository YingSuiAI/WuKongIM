package channels

import (
	"context"
	"errors"
	"testing"
	"time"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/stretchr/testify/require"
)

func TestServiceFailedAuthorityStartsRecoveryAndWaitsForNewAuthoritativeMeta(t *testing.T) {
	id := ch.ChannelID{ID: "foreground-failover", Type: 2}
	failed := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 11, LeaderEpoch: 20, RouteGeneration: 30, Leader: 1, Replicas: []ch.NodeID{1, 2, 3}, ISR: []ch.NodeID{1, 2, 3}, MinISR: 2, Status: ch.StatusActive}
	fresh := failed
	fresh.Leader = 2
	fresh.LeaderEpoch++
	fresh.RouteGeneration++
	source := &countingMetaSource{metas: []ch.Meta{failed, failed, fresh}}
	recovery := &recordingAppendAuthorityRecovery{}
	svc, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 3, MetaSource: source, AppendAuthorityRecovery: recovery})
	require.NoError(t, err)

	got, err := svc.ResolveAppendAuthority(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, failed.Leader, got.Leader)

	svc.MarkAppendAuthorityFailed(id, failed.Leader, failed.Epoch, failed.LeaderEpoch, failed.RouteGeneration)
	_, err = svc.ResolveAppendAuthority(context.Background(), id)
	require.ErrorIs(t, err, ch.ErrNotReady)
	require.Equal(t, []ch.Meta{failed}, recovery.metas)

	got, err = svc.ResolveAppendAuthority(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, fresh.Leader, got.Leader)
	require.Equal(t, fresh.LeaderEpoch, got.LeaderEpoch)
	require.Equal(t, 1, len(recovery.metas), "new authority must clear the failed-version recovery marker")
}

func TestServiceTransientInvalidationReusesSameHealthyAuthorityWithoutRecovery(t *testing.T) {
	id := ch.ChannelID{ID: "transient-authority", Type: 2}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 1, LeaderEpoch: 1, RouteGeneration: 1, Leader: 4, Replicas: []ch.NodeID{1, 3, 4}, ISR: []ch.NodeID{1, 3, 4}, MinISR: 2, Status: ch.StatusActive}
	source := &countingMetaSource{meta: meta}
	recovery := &recordingAppendAuthorityRecovery{}
	svc, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 4, MetaSource: source, AppendAuthorityRecovery: recovery})
	require.NoError(t, err)

	_, err = svc.ResolveAppendAuthority(context.Background(), id)
	require.NoError(t, err)
	svc.InvalidateAppendAuthority(id, meta.Leader, meta.Epoch, meta.LeaderEpoch, meta.RouteGeneration)
	got, err := svc.ResolveAppendAuthority(context.Background(), id)

	require.NoError(t, err)
	require.Equal(t, meta, got)
	require.Empty(t, recovery.metas)
	require.Equal(t, 2, source.ensureCalls)
}

func TestServiceHealthyFailedMarkerClearsAndReusesCurrentAuthority(t *testing.T) {
	id := ch.ChannelID{ID: "healthy-marked-authority", Type: 2}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 1, LeaderEpoch: 1, RouteGeneration: 1, Leader: 4, Replicas: []ch.NodeID{1, 3, 4}, ISR: []ch.NodeID{1, 3, 4}, MinISR: 2, Status: ch.StatusActive}
	source := &countingMetaSource{meta: meta}
	recovery := &recordingAppendAuthorityRecovery{disposition: AppendAuthorityRecoveryCurrent}
	svc, err := NewService(Config{Runtime: &fakeRuntime{}, LocalNode: 4, MetaSource: source, AppendAuthorityRecovery: recovery})
	require.NoError(t, err)

	_, err = svc.ResolveAppendAuthority(context.Background(), id)
	require.NoError(t, err)
	svc.MarkAppendAuthorityFailed(id, meta.Leader, meta.Epoch, meta.LeaderEpoch, meta.RouteGeneration)
	got, err := svc.ResolveAppendAuthority(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, meta, got)
	got, err = svc.ResolveAppendAuthority(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, meta, got)
	require.Len(t, recovery.metas, 1, "confirmed current authority must clear the exact failed marker")
	require.Equal(t, 2, source.ensureCalls, "confirmed current authority must be restored to the hot cache")
}

func TestForegroundAppendAuthorityRecoveryReusesExactCreateCommandAfterUncertainResult(t *testing.T) {
	id := ch.ChannelID{ID: "same-task", Type: 2}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 11, LeaderEpoch: 20, RouteGeneration: 30, Leader: 1, Replicas: []ch.NodeID{1, 2, 3}, ISR: []ch.NodeID{1, 2, 3}, MinISR: 2, Status: ch.StatusActive}
	source := &foregroundRecoverySourceFake{
		snapshot: control.Snapshot{Nodes: failoverHealthyNodes(2, 3)},
		probes: map[uint64]ch.RuntimeProbeChannel{
			2: failoverProbe(id, 2, 11, 20, 10, 10).Probe,
			3: failoverProbe(id, 3, 11, 20, 9, 9).Probe,
		},
	}
	store := &foregroundRecoveryStoreFake{createErrs: []error{errors.New("uncertain proposal"), nil}}
	recovery := &ForegroundAppendAuthorityRecovery{source: source, store: store, now: func() time.Time { return time.UnixMilli(1234) }}

	_, err := recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)
	require.Error(t, err)
	_, err = recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)
	require.NoError(t, err)
	require.Len(t, store.requests, 2)
	require.Equal(t, store.requests[0], store.requests[1], "uncertain create retries must reuse the exact task identity and command fields")
	require.NotEmpty(t, store.requests[0].TaskID)
	require.Equal(t, int64(1234), store.requests[0].CreatedAtMS)
	require.Equal(t, ch.NodeID(2), store.requests[0].DesiredLeader)
	require.Equal(t, 1, source.snapshotCalls)
	require.Equal(t, []uint64{2, 3}, source.probeCalls)
}

func TestForegroundAppendAuthorityRecoveryJoinsActiveChannelTask(t *testing.T) {
	id := ch.ChannelID{ID: "active-task", Type: 2}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 1, LeaderEpoch: 1, Leader: 1, Replicas: []ch.NodeID{1, 2}, ISR: []ch.NodeID{1, 2}, MinISR: 2, Status: ch.StatusActive}
	source := &foregroundRecoverySourceFake{}
	store := &foregroundRecoveryStoreFake{active: true}
	recovery := &ForegroundAppendAuthorityRecovery{source: source, store: store, now: time.Now}

	_, err := recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)
	require.NoError(t, err)
	require.Empty(t, store.requests)
	require.Zero(t, source.snapshotCalls)
}

func TestForegroundAppendAuthorityRecoveryUsesDurableReplicaWhenFollowerRuntimeIsUnloaded(t *testing.T) {
	id := ch.ChannelID{ID: "unloaded-follower", Type: 2}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 11, LeaderEpoch: 20, RouteGeneration: 30, Leader: 1, Replicas: []ch.NodeID{1, 2, 3}, ISR: []ch.NodeID{1, 2, 3}, MinISR: 2, Status: ch.StatusActive}
	source := &foregroundRecoverySourceFake{
		snapshot: control.Snapshot{Nodes: failoverHealthyNodes(2, 3)},
		probes: map[uint64]ch.RuntimeProbeChannel{
			2: failoverProbe(id, 2, 11, 20, 10, 10).Probe,
			3: failoverProbe(id, 3, 11, 20, 10, 10).Probe,
		},
	}
	store := &foregroundRecoveryStoreFake{}
	recovery := &ForegroundAppendAuthorityRecovery{source: source, store: store, now: time.Now}

	_, err := recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)
	require.NoError(t, err)
	require.Zero(t, source.runtimeProbeCalls, "failover must not require a loaded follower reactor")
	require.Equal(t, []uint64{2, 3}, source.probeCalls)
	require.Len(t, store.requests, 1)
}

func TestForegroundAppendAuthorityRecoveryKeepsHealthyCurrentLeader(t *testing.T) {
	id := ch.ChannelID{ID: "healthy-current-leader", Type: 2}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 11, LeaderEpoch: 20, RouteGeneration: 30, Leader: 4, Replicas: []ch.NodeID{1, 3, 4}, ISR: []ch.NodeID{1, 3, 4}, MinISR: 2, Status: ch.StatusActive}
	leaderProbe := failoverProbe(id, 4, 11, 20, 10, 10).Probe
	leaderProbe.Role = ch.RoleLeader
	source := &foregroundRecoverySourceFake{
		snapshot: control.Snapshot{Nodes: failoverHealthyNodes(1, 3, 4)},
		probes:   map[uint64]ch.RuntimeProbeChannel{4: leaderProbe},
	}
	store := &foregroundRecoveryStoreFake{}
	recovery := &ForegroundAppendAuthorityRecovery{source: source, store: store, now: time.Now}

	disposition, err := recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)

	require.NoError(t, err)
	require.Equal(t, AppendAuthorityRecoveryCurrent, disposition)
	require.Equal(t, []uint64{4}, source.probeCalls)
	require.Empty(t, store.requests)
}

func TestForegroundAppendAuthorityRecoveryKeepsReachableEmptyCurrentLeader(t *testing.T) {
	id := ch.ChannelID{ID: "healthy-empty-current-leader", Type: 1}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 1, LeaderEpoch: 1, RouteGeneration: 1, Leader: 4, Replicas: []ch.NodeID{1, 3, 4}, ISR: []ch.NodeID{1, 3, 4}, MinISR: 2, Status: ch.StatusActive}
	source := &foregroundRecoverySourceFake{
		snapshot: control.Snapshot{Nodes: failoverHealthyNodes(1, 3, 4)},
		probes:   map[uint64]ch.RuntimeProbeChannel{},
	}
	store := &foregroundRecoveryStoreFake{}
	recovery := &ForegroundAppendAuthorityRecovery{source: source, store: store, now: time.Now}

	disposition, err := recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)

	require.NoError(t, err)
	require.Equal(t, AppendAuthorityRecoveryCurrent, disposition)
	require.Equal(t, []uint64{4}, source.probeCalls)
	require.Empty(t, store.requests)
}

func TestForegroundAppendAuthorityRecoveryDoesNotMigrateOnIndeterminateCurrentLeaderProbe(t *testing.T) {
	id := ch.ChannelID{ID: "indeterminate-current-leader", Type: 1}
	meta := ch.Meta{Key: ch.ChannelKeyForID(id), ID: id, Epoch: 1, LeaderEpoch: 1, RouteGeneration: 1, Leader: 4, Replicas: []ch.NodeID{1, 3, 4}, ISR: []ch.NodeID{1, 3, 4}, MinISR: 2, Status: ch.StatusActive}
	source := &foregroundRecoverySourceFake{
		snapshot:  control.Snapshot{Nodes: failoverHealthyNodes(1, 3, 4)},
		probes:    map[uint64]ch.RuntimeProbeChannel{},
		probeErrs: map[uint64]error{4: errors.New("temporary discovery miss")},
	}
	store := &foregroundRecoveryStoreFake{}
	recovery := &ForegroundAppendAuthorityRecovery{source: source, store: store, now: time.Now}

	disposition, err := recovery.EnsureAppendAuthorityRecovery(context.Background(), meta)

	require.NoError(t, err)
	require.Equal(t, AppendAuthorityRecoveryPending, disposition)
	require.Equal(t, []uint64{4}, source.probeCalls)
	require.Empty(t, store.requests, "indeterminate leader probe must not create durable failover")
}

type recordingAppendAuthorityRecovery struct {
	metas       []ch.Meta
	disposition AppendAuthorityRecoveryDisposition
}

func (r *recordingAppendAuthorityRecovery) EnsureAppendAuthorityRecovery(_ context.Context, meta ch.Meta) (AppendAuthorityRecoveryDisposition, error) {
	r.metas = append(r.metas, cloneMeta(meta))
	return r.disposition, nil
}

type foregroundRecoverySourceFake struct {
	snapshot          control.Snapshot
	probes            map[uint64]ch.RuntimeProbeChannel
	probeErrs         map[uint64]error
	snapshotCalls     int
	probeCalls        []uint64
	runtimeProbeCalls int
}

func (s *foregroundRecoverySourceFake) ProbeChannel(context.Context, uint64, string, uint8) (ch.RuntimeProbeChannel, error) {
	s.runtimeProbeCalls++
	return ch.RuntimeProbeChannel{}, ch.ErrChannelNotFound
}

func (s *foregroundRecoverySourceFake) ControlSnapshot(context.Context) (control.Snapshot, error) {
	s.snapshotCalls++
	return s.snapshot, nil
}

func (s *foregroundRecoverySourceFake) ProbeChannelReplica(_ context.Context, nodeID uint64, _ metadb.ChannelRuntimeMeta) (ch.RuntimeProbeChannel, error) {
	s.probeCalls = append(s.probeCalls, nodeID)
	if err := s.probeErrs[nodeID]; err != nil {
		return ch.RuntimeProbeChannel{}, err
	}
	probe, ok := s.probes[nodeID]
	if !ok {
		return ch.RuntimeProbeChannel{}, ch.ErrChannelNotFound
	}
	return probe, nil
}

type foregroundRecoveryStoreFake struct {
	active     bool
	createErrs []error
	requests   []CreateLeaderFailoverRequest
}

func (s *foregroundRecoveryStoreFake) GetActive(context.Context, ch.ChannelID) (metadb.ChannelMigrationTask, bool, error) {
	return metadb.ChannelMigrationTask{}, s.active, nil
}

func (s *foregroundRecoveryStoreFake) CreateLeaderFailover(_ context.Context, req CreateLeaderFailoverRequest) (metadb.ChannelMigrationTask, error) {
	s.requests = append(s.requests, req)
	var err error
	if len(s.createErrs) > 0 {
		err = s.createErrs[0]
		s.createErrs = s.createErrs[1:]
	}
	return metadb.ChannelMigrationTask{TaskID: req.TaskID}, err
}
