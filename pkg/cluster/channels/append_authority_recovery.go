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
		if r.currentLeaderUsable(recoveryCtx, snapshot, meta) {
			return AppendAuthorityRecoveryCurrent, nil
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

func (r *ForegroundAppendAuthorityRecovery) currentLeaderUsable(ctx context.Context, snapshot control.Snapshot, meta ch.Meta) bool {
	if !appendRecoveryNodeSchedulable(snapshot.Nodes, uint64(meta.Leader)) {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, foregroundCandidateProbeTimeout)
	probe, err := r.source.ProbeChannelReplica(probeCtx, uint64(meta.Leader), runtimeMetaFromAppendMeta(meta))
	cancel()
	if err != nil {
		// An empty new Channel has no durable authority identity yet. A direct
		// not-found response from its current, schedulable leader still proves
		// that the authority node is reachable and can perform first install.
		return ch.ErrorMatches(err, ch.ErrChannelNotFound)
	}
	return probe.ChannelID == meta.ID &&
		probe.ChannelEpoch == meta.Epoch &&
		probe.LeaderEpoch == meta.LeaderEpoch &&
		probe.Role == ch.RoleLeader &&
		probe.Status == ch.StatusActive
}

func appendRecoveryNodeSchedulable(nodes []control.Node, nodeID uint64) bool {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return control.NodeSchedulableForPlacement(node)
		}
	}
	return false
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
