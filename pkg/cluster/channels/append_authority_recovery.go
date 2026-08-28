package channels

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

const (
	foregroundAppendRecoveryTimeout = time.Second
	foregroundCandidateProbeTimeout = 250 * time.Millisecond
	foregroundRecoveryPlanLimit     = 1024
)

// AppendAuthorityRecoverySource supplies current node health and replica
// durable replica proofs for foreground dead-leader recovery.
type AppendAuthorityRecoverySource interface {
	ControlSnapshot(context.Context) (control.Snapshot, error)
	ProbeChannelReplica(context.Context, uint64, metadb.ChannelRuntimeMeta) (ch.RuntimeProbeChannel, error)
}

type appendAuthorityRecoveryStore interface {
	GetActive(context.Context, ch.ChannelID) (metadb.ChannelMigrationTask, bool, error)
	CreateLeaderFailover(context.Context, CreateLeaderFailoverRequest) (metadb.ChannelMigrationTask, error)
}

type appendRecoveryPlanKey struct {
	id              ch.ChannelID
	leader          ch.NodeID
	epoch           uint64
	leaderEpoch     uint64
	routeGeneration uint64
}

// ForegroundAppendAuthorityRecovery creates the same create-only failover task
// on every retry for one exact failed authority version.
type ForegroundAppendAuthorityRecovery struct {
	source AppendAuthorityRecoverySource
	store  appendAuthorityRecoveryStore
	now    func() time.Time

	mu       sync.Mutex
	plans    map[appendRecoveryPlanKey]CreateLeaderFailoverRequest
	planFIFO []appendRecoveryPlanKey
}

type currentAppendAuthorityState uint8

const (
	currentAppendAuthorityUnknown currentAppendAuthorityState = iota
	currentAppendAuthorityUsable
	currentAppendAuthorityUnavailable
)

// NewForegroundAppendAuthorityRecovery creates a bounded foreground recovery seam.
func NewForegroundAppendAuthorityRecovery(source AppendAuthorityRecoverySource, store *MigrationStore) *ForegroundAppendAuthorityRecovery {
	var recoveryStore appendAuthorityRecoveryStore
	if store != nil {
		recoveryStore = store
	}
	return &ForegroundAppendAuthorityRecovery{source: source, store: recoveryStore, now: time.Now}
}

// EnsureAppendAuthorityRecovery joins an active migration or creates one
// guarded by the exact authoritative runtime version observed by the caller.
func (r *ForegroundAppendAuthorityRecovery) EnsureAppendAuthorityRecovery(ctx context.Context, meta ch.Meta) (AppendAuthorityRecoveryDisposition, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctxErr(ctx); err != nil {
		return AppendAuthorityRecoveryPending, err
	}
	if r == nil || r.source == nil || r.store == nil {
		return AppendAuthorityRecoveryPending, fmt.Errorf("%w: append recovery is not configured", ch.ErrInvalidConfig)
	}
	if !cacheableAppendMeta(meta.ID, meta) {
		return AppendAuthorityRecoveryPending, ch.ErrNotReady
	}

	recoveryCtx, cancel := context.WithTimeout(ctx, foregroundAppendRecoveryTimeout)
	defer cancel()
	if _, active, err := r.store.GetActive(recoveryCtx, meta.ID); err != nil {
		return AppendAuthorityRecoveryPending, err
	} else if active {
		return AppendAuthorityRecoveryPending, nil
	}

	key := appendRecoveryKey(meta)
	req, planned := r.loadPlan(key)
	if !planned {
		snapshot, err := r.source.ControlSnapshot(recoveryCtx)
		if err != nil {
			return AppendAuthorityRecoveryPending, err
		}
		switch assessCurrentChannelLeader(recoveryCtx, snapshot.Nodes, runtimeMetaFromAppendMeta(meta), r.source.ProbeChannelReplica) {
		case currentAppendAuthorityUsable:
			return AppendAuthorityRecoveryCurrent, nil
		case currentAppendAuthorityUnknown:
			// A schedulable leader whose probe has an indeterminate transport
			// outcome is not death evidence. Let bounded foreground retries and
			// the control-plane health view establish the next fact.
			return AppendAuthorityRecoveryPending, nil
		}
		req, err = r.plan(recoveryCtx, snapshot, meta)
		if err != nil {
			return AppendAuthorityRecoveryPending, err
		}
		req = r.storePlan(key, req)
	}
	_, err := r.store.CreateLeaderFailover(recoveryCtx, req)
	if errors.Is(err, metadb.ErrAlreadyExists) {
		return AppendAuthorityRecoveryPending, nil
	}
	return AppendAuthorityRecoveryPending, err
}

type channelReplicaProbe func(context.Context, uint64, metadb.ChannelRuntimeMeta) (ch.RuntimeProbeChannel, error)

// assessCurrentChannelLeader is the single foreground/background authority
// liveness rule. It requires durable membership plus a direct replica probe;
// low-frequency health alone never proves an active leader dead.
func assessCurrentChannelLeader(ctx context.Context, nodes []control.Node, meta metadb.ChannelRuntimeMeta, probeReplica channelReplicaProbe) currentAppendAuthorityState {
	if meta.Leader == 0 || probeReplica == nil {
		return currentAppendAuthorityUnavailable
	}
	node, found := currentChannelLeaderNode(nodes, meta.Leader)
	if !found {
		// ControlSnapshot is the complete durable membership projection. A
		// metadata leader absent from it is no longer an active data member.
		return currentAppendAuthorityUnavailable
	}
	if !control.NodeActiveDataMember(node) {
		return currentAppendAuthorityUnavailable
	}
	probeCtx, cancel := context.WithTimeout(ctx, foregroundCandidateProbeTimeout)
	probe, err := probeReplica(probeCtx, meta.Leader, meta)
	cancel()
	if err != nil {
		if currentChannelLeaderHealthUnavailable(node.Health) {
			return currentAppendAuthorityUnavailable
		}
		// An empty new Channel has no durable authority identity yet. A direct
		// not-found response from a serviceable leader still proves that the
		// authority node is reachable and can perform first install.
		if ch.ErrorMatches(err, ch.ErrChannelNotFound) {
			return currentAppendAuthorityUsable
		}
		// A transport failure alone may be a transient local discovery miss. It
		// becomes death evidence only after the durable health lease has expired,
		// or a fresh report explicitly says the runtime is no longer serviceable.
		// This keeps a healthy, schedulable leader safe while allowing a crashed
		// process (which cannot publish a fresh "down" report) to fail over after
		// its last lease expires.
		return currentAppendAuthorityUnknown
	}
	id := ch.ChannelID{ID: meta.ChannelID, Type: uint8(meta.ChannelType)}
	if probe.ChannelID == id &&
		probe.ChannelEpoch == meta.ChannelEpoch &&
		probe.LeaderEpoch == meta.LeaderEpoch &&
		probe.Role == ch.RoleLeader &&
		probe.Status == ch.StatusActive {
		return currentAppendAuthorityUsable
	}
	if currentChannelLeaderHealthUnavailable(node.Health) {
		return currentAppendAuthorityUnavailable
	}
	return currentAppendAuthorityUnknown
}

func currentChannelLeaderHealthUnavailable(health control.NodeHealth) bool {
	if health.Freshness == control.NodeHealthStale {
		return true
	}
	return health.Freshness == control.NodeHealthFresh &&
		(health.Status != control.NodeAlive || !health.RuntimeReady)
}

func currentChannelLeaderNode(nodes []control.Node, nodeID uint64) (control.Node, bool) {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}
	return control.Node{}, false
}

func (r *ForegroundAppendAuthorityRecovery) plan(ctx context.Context, snapshot control.Snapshot, meta ch.Meta) (CreateLeaderFailoverRequest, error) {
	probes := make([]FailoverCandidateProbe, 0, len(meta.ISR))
	for _, nodeID := range meta.ISR {
		if nodeID == 0 || nodeID == meta.Leader {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, foregroundCandidateProbeTimeout)
		probe, probeErr := r.source.ProbeChannelReplica(probeCtx, uint64(nodeID), runtimeMetaFromAppendMeta(meta))
		cancel()
		if probeErr == nil {
			probes = append(probes, FailoverCandidateProbe{NodeID: uint64(nodeID), Probe: probe})
		}
	}
	decision := NewFailoverPlanner().Plan(FailoverPlanInput{
		Meta:          runtimeMetaFromAppendMeta(meta),
		Nodes:         snapshot.Nodes,
		Probes:        probes,
		LeaderSuspect: true,
	})
	if decision.Action != FailoverActionCreateLeaderTransfer {
		return CreateLeaderFailoverRequest{}, fmt.Errorf("%w: %s", ch.ErrNotReady, decision.BlockReason)
	}
	createdAtMS := time.Now().UnixMilli()
	if r.now != nil {
		createdAtMS = r.now().UnixMilli()
	}
	return CreateLeaderFailoverRequest{
		ChannelID:               meta.ID,
		TaskID:                  foregroundFailoverTaskID(meta),
		DesiredLeader:           ch.NodeID(decision.TargetNode),
		ObservedHW:              decision.ObservedHW,
		ObservedLeaderEpoch:     decision.ObservedEpoch,
		ExpectedLeader:          meta.Leader,
		ExpectedChannelEpoch:    meta.Epoch,
		ExpectedLeaderEpoch:     meta.LeaderEpoch,
		ExpectedRouteGeneration: meta.RouteGeneration,
		CreatedAtMS:             createdAtMS,
	}, nil
}

func appendRecoveryKey(meta ch.Meta) appendRecoveryPlanKey {
	return appendRecoveryPlanKey{id: meta.ID, leader: meta.Leader, epoch: meta.Epoch, leaderEpoch: meta.LeaderEpoch, routeGeneration: meta.RouteGeneration}
}

func foregroundFailoverTaskID(meta ch.Meta) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d\x00%d\x00%d\x00%d", meta.ID.ID, meta.ID.Type, meta.Leader, meta.Epoch, meta.LeaderEpoch, meta.RouteGeneration)))
	return fmt.Sprintf("foreground-leader-failover-%x", sum[:16])
}

func runtimeMetaFromAppendMeta(meta ch.Meta) metadb.ChannelRuntimeMeta {
	return metadb.NormalizeChannelRuntimeMeta(metadb.ChannelRuntimeMeta{
		ChannelID:       meta.ID.ID,
		ChannelType:     int64(meta.ID.Type),
		ChannelEpoch:    meta.Epoch,
		LeaderEpoch:     meta.LeaderEpoch,
		RouteGeneration: meta.RouteGeneration,
		Leader:          uint64(meta.Leader),
		Replicas:        projectUint64NodeIDs(meta.Replicas),
		ISR:             projectUint64NodeIDs(meta.ISR),
		MinISR:          int64(meta.MinISR),
		Status:          uint8(meta.Status),
	})
}

func (r *ForegroundAppendAuthorityRecovery) loadPlan(key appendRecoveryPlanKey) (CreateLeaderFailoverRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, ok := r.plans[key]
	return req, ok
}

func (r *ForegroundAppendAuthorityRecovery) storePlan(key appendRecoveryPlanKey, req CreateLeaderFailoverRequest) CreateLeaderFailoverRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.plans == nil {
		r.plans = make(map[appendRecoveryPlanKey]CreateLeaderFailoverRequest)
	}
	if existing, exists := r.plans[key]; exists {
		return existing
	}
	if len(r.planFIFO) >= foregroundRecoveryPlanLimit {
		oldest := r.planFIFO[0]
		r.planFIFO = r.planFIFO[1:]
		delete(r.plans, oldest)
	}
	r.plans[key] = req
	r.planFIFO = append(r.planFIFO, key)
	return req
}
