//go:build integration

package multiraft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type gatedReadTransport struct {
	base      Transport
	blockTerm *atomic.Uint64
}

func (g gatedReadTransport) Send(ctx context.Context, batch []Envelope) error {
	allowed := make([]Envelope, 0, len(batch))
	for _, env := range batch {
		term := g.blockTerm.Load()
		if term != 0 && env.Message.Term >= term && (env.Message.Type == raftpb.MsgAppResp || env.Message.Type == raftpb.MsgHeartbeatResp) {
			continue
		}
		allowed = append(allowed, env)
	}
	return g.base.Send(ctx, allowed)
}

func TestReadBarrierNewLeaderWaitsForCurrentTermQuorum(t *testing.T) {
	var blockedTerm atomic.Uint64
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{Seed: 271, WrapTransport: func(_ NodeID, base Transport) Transport {
		return gatedReadTransport{base: base, blockTerm: &blockedTerm}
	}})
	id := SlotID(271)
	cluster.bootstrapSlot(t, id, []NodeID{1, 2, 3})
	leader := cluster.waitForLeader(t, id)
	future, err := cluster.runtime(leader).Propose(context.Background(), id, proposalString("acknowledged-correction"))
	require.NoError(t, err)
	written := waitForFutureResult(t, future)
	cluster.waitForAllNodesAppliedIndex(t, id, written.Index)
	before, err := cluster.runtime(leader).FreshStatus(context.Background(), id)
	require.NoError(t, err)
	target := cluster.pickFollower(leader)
	blockedTerm.Store(before.Term + 1)
	require.NoError(t, cluster.runtime(leader).TransferLeadership(context.Background(), id, target))
	cluster.waitForSpecificLeader(t, id, target)
	newLeader, err := cluster.runtime(target).FreshStatus(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, RoleLeader, newLeader.Role)
	require.Equal(t, newLeader.AppliedIndex, newLeader.CommitIndex, "fresh role and matching indexes are still insufficient before the first current-term quorum")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = cluster.runtime(target).ReadBarrier(ctx, id)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	blockedTerm.Store(0)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	proof, err := cluster.runtime(target).ReadBarrier(ctx, id)
	require.NoError(t, err)
	require.Equal(t, target, proof.LeaderID)
	require.Greater(t, proof.Term, before.Term)
	require.GreaterOrEqual(t, proof.Index, written.Index)
}

func TestReadBarrierIsolatedLeaderCannotReturnCachedAuthority(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{Seed: 272})
	id := SlotID(272)
	cluster.bootstrapSlot(t, id, []NodeID{1, 2, 3})
	leader := cluster.waitForLeader(t, id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	proof, err := cluster.runtime(leader).ReadBarrier(ctx, id)
	cancel()
	require.NoError(t, err)
	require.NotZero(t, proof.Index)
	cluster.partitionNode(leader)
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = cluster.runtime(leader).ReadBarrier(ctx, id)
	cancel()
	require.Error(t, err, "a cached RoleLeader is not a fresh majority acknowledgement")
	cluster.healNode(leader)
}

func TestReadBarrierWaitsForDurableStateMachineApplication(t *testing.T) {
	rt := newStartedRuntimeWithTick(t, time.Hour)
	id := SlotID(273)
	store := &internalFakeStorage{}
	fsm := newBlockingStateMachine()
	t.Cleanup(fsm.unblock)
	require.NoError(t, rt.BootstrapSlot(context.Background(), BootstrapSlotRequest{Slot: SlotOptions{ID: id, Storage: store, StateMachine: fsm}, Voters: []NodeID{1}}))
	waitForCondition(t, func() bool { status, err := rt.Status(id); return err == nil && status.Role == RoleLeader })
	future, err := rt.Propose(context.Background(), id, proposalString("pending-apply"))
	require.NoError(t, err)
	select {
	case <-fsm.started:
	case <-time.After(time.Second):
		t.Fatal("FSM apply did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = rt.ReadBarrier(ctx, id)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	fsm.unblock()
	written := waitForFutureResult(t, future)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	proof, err := rt.ReadBarrier(ctx, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, proof.Index, written.Index)
}

func TestReadBarrierCancellationCannotGrowRaftPendingReadsUnbounded(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{Seed: 274})
	id := SlotID(274)
	cluster.bootstrapSlot(t, id, []NodeID{1, 2, 3})
	leader := cluster.waitForLeader(t, id)
	rt := cluster.runtime(leader)
	rt.mu.RLock()
	owner := rt.slots[id]
	rt.mu.RUnlock()
	owner.mu.Lock()
	owner.maxQueuedControls = 1
	owner.mu.Unlock()
	cluster.partitionNode(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := rt.ReadBarrier(ctx, id)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = rt.ReadBarrier(ctx, id)
	cancel()
	require.ErrorIs(t, err, ErrSlotBusy)
	cluster.healNode(leader)
	waitForCondition(t, func() bool { owner.mu.Lock(); defer owner.mu.Unlock(); return len(owner.pendingReads) == 0 })
}

type barrierCompletionObserver struct{ completed chan struct{} }

func (o barrierCompletionObserver) ObserveFutureCompletion(Result, error) { close(o.completed) }

func TestReadBarrierProposalObserverOutlivesCanceledWaiter(t *testing.T) {
	rt := newStartedRuntimeWithTick(t, time.Hour)
	id := SlotID(275)
	fsm := newBlockingStateMachine()
	t.Cleanup(fsm.unblock)
	require.NoError(t, rt.BootstrapSlot(context.Background(), BootstrapSlotRequest{Slot: SlotOptions{ID: id, Storage: &internalFakeStorage{}, StateMachine: fsm}, Voters: []NodeID{1}}))
	waitForCondition(t, func() bool { status, err := rt.Status(id); return err == nil && status.Role == RoleLeader })
	completed := make(chan struct{})
	future, err := rt.ProposeObserved(context.Background(), id, proposalString("holds-restore-admission"), barrierCompletionObserver{completed: completed})
	require.NoError(t, err)
	select {
	case <-fsm.started:
	case <-time.After(time.Second):
		t.Fatal("FSM apply did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = future.Wait(ctx)
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-completed:
		t.Fatal("canceled waiter released accepted proposal lifecycle")
	default:
	}
	fsm.unblock()
	_ = waitForFutureResult(t, future)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("terminal proposal did not release admission")
	}
}

func TestReadBarrierClosedOwnerCannotRegisterNewPendingRead(t *testing.T) {
	owner := &slot{closed: true}
	request := &readBarrierRequest{ctx: context.Background(), resp: make(chan readBarrierResponse, 1)}
	owner.startReadBarrier(request)
	response := <-request.resp
	require.ErrorIs(t, response.err, ErrSlotClosed)
	require.Empty(t, owner.pendingReads)
}

type committedProposalApplyGate struct {
	started, proceed chan struct{}
	once             sync.Once
}

func (g *committedProposalApplyGate) ObserveProposalStage(stage, _ string, _ time.Duration) {
	if stage == "meta_create_slot_raft_commit_wait" {
		g.once.Do(func() { close(g.started); <-g.proceed })
	}
}

func TestReadBarrierObservedCommittedProposalSurvivesLeaderTransfer(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{Seed: 276})
	id := SlotID(276)
	cluster.bootstrapSlot(t, id, []NodeID{1, 2, 3})
	leader := cluster.waitForLeader(t, id)
	gate := &committedProposalApplyGate{started: make(chan struct{}), proceed: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.proceed) }) }
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(WithProposalStageObserver(context.Background(), gate), 5*time.Second)
	defer cancel()
	completed := make(chan struct{})
	future, err := cluster.runtime(leader).ProposeObserved(ctx, id, proposalString("committed-before-transfer"), barrierCompletionObserver{completed: completed})
	require.NoError(t, err)
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("proposal did not reach committed/pre-apply gate")
	}
	target := cluster.pickFollower(leader)
	require.NoError(t, cluster.runtime(leader).TransferLeadership(ctx, id, target))
	cluster.waitForLeaderAmong(t, id, cluster.otherNodes(leader))
	oldStatus, err := cluster.runtime(leader).FreshStatus(ctx, id)
	require.NoError(t, err)
	require.Equal(t, RoleFollower, oldStatus.Role)
	select {
	case <-completed:
		t.Error("leadership loss completed an already committed proposal before durable application")
	default:
	}
	release()
	_, err = future.Wait(ctx)
	require.NoError(t, err, "known committed proposals still belong to FSM apply after step-down")
}
