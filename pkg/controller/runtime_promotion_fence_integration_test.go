//go:build integration

package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/controller/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/controller/statefile"
)

// This exercises the real Runtime and Raft membership consumer, not forged
// ObservedVoters. With no target transport, promotion reaches live voters [1,2]
// but cannot commit its final state command. Slot expansion must still reject.
func TestRuntimeLivePromotionReservationBlocksConcurrentSlotExpansion(t *testing.T) {
	runtime, before := promotionFenceRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runtime.PromoteControllerVoter(ctx, PromoteControllerVoterRequest{NodeID: 2, ExpectedRevision: before.Revision})
		done <- err
	}()
	defer func() { cancel(); <-done }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !containsRuntimeUint64(runtime.raft.Status().Voters, 2) {
		select {
		case <-deadline.C:
			t.Fatal("real Controller Raft never reached the pending live promotion")
		case <-tick.C:
		}
	}
	reserved, err := runtime.LocalState(context.Background())
	if err != nil || reserved.ControllerVoterPromotion == nil || len(reserved.Controllers) != 1 {
		t.Fatalf("expected in-flight durable reservation before final promotion: %#v, %v", reserved, err)
	}
	request := promotionFenceSlotRequest(reserved)
	if _, err := runtime.RequestSlotReplicaMove(context.Background(), request); !errors.Is(err, ErrProposalRejected) || !strings.Contains(err.Error(), fsm.ReasonControllerVoterPromotionActive) {
		t.Fatalf("expansion during real live membership change = %v", err)
	}
	persisted, err := statefile.New(filepath.Join(runtime.cfg.StateDir, "cluster-state.json")).Load(context.Background())
	if err != nil || persisted.ControllerVoterPromotion == nil || persisted.SlotReplicaCountTransition != nil {
		t.Fatalf("durable reservation was lost: %#v, %v", persisted, err)
	}
}

func TestRuntimePromotionReservationSurvivesRestartAndResumes(t *testing.T) {
	runtime, before := promotionFenceRuntime(t)
	request := PromoteControllerVoterRequest{NodeID: 2, ExpectedRevision: before.Revision, ReserveOnly: true}
	reserved, err := runtime.PromoteControllerVoter(context.Background(), request)
	if err != nil || !reserved.Changed {
		t.Fatalf("reserve promotion = %#v, %v", reserved, err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewRuntime(runtime.cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := restarted.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Stop(context.Background()) })
	state := waitForState(t, restarted, func(st ClusterState) bool { return st.ControllerVoterPromotion != nil && restarted.isLocalLeader() })
	if _, err := restarted.RequestSlotReplicaMove(ctx, promotionFenceSlotRequest(state)); !errors.Is(err, ErrProposalRejected) {
		t.Fatalf("restart lost expansion fence: %v", err)
	}
	request.ExpectedRevision = state.Revision
	retry, err := restarted.PromoteControllerVoter(ctx, request)
	if err != nil || retry.Changed || retry.Revision != reserved.Revision {
		t.Fatalf("reservation retry = %#v, %v", retry, err)
	}
	request.NodeID = 3
	if _, err := restarted.PromoteControllerVoter(ctx, request); !errors.Is(err, ErrProposalRejected) {
		t.Fatalf("different target replaced reservation: %v", err)
	}
}

func TestRuntimePrepareControllerVoterRejectsTransitionWithoutStoppingMirror(t *testing.T) {
	source, before := promotionFenceRuntime(t)
	if _, err := source.RequestSlotReplicaMove(context.Background(), promotionFenceSlotRequest(before)); err != nil {
		t.Fatal(err)
	}
	target, err := NewRuntime(RuntimeConfig{NodeID: 2, Addr: "n2", StateDir: t.TempDir(),
		ClusterID: source.cfg.ClusterID, Role: RuntimeRoleMirror, Voters: source.cfg.Voters,
		SyncPeers:    fixedPeerPicker{ids: []uint64{1}, endpoints: map[uint64]Endpoint{1: source}},
		TickInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Stop(context.Background()) })
	st, _ := target.LocalState(context.Background())
	_, err = target.PrepareControllerVoter(context.Background(), PrepareControllerVoterRequest{
		NodeID: 2, ClusterID: source.cfg.ClusterID, ExpectedRevision: st.Revision,
		NextVoters: []Voter{source.cfg.Voters[0], {NodeID: 2, Addr: "n2"}},
	})
	if !errors.Is(err, ErrProposalRejected) || !strings.Contains(err.Error(), fsm.ReasonSlotReplicaCountTransitionActive) {
		t.Fatalf("prepare during expansion = %v", err)
	}
	if target.syncClient == nil || target.refreshCancel == nil || target.raft != nil || target.cfg.Role != RuntimeRoleMirror {
		t.Fatal("rejected preparation changed the target's mirror runtime")
	}
	if _, err := os.Stat(filepath.Join(target.cfg.StateDir, mirrorBeforeControllerVoterPromotionFile)); !os.IsNotExist(err) {
		t.Fatalf("rejected preparation moved mirror state: %v", err)
	}
	// Drive another authoritative revision and prove the actual mirror consumer
	// remains alive after rejection, so later Slot tasks can still reach it.
	task := st.Tasks[0]
	if err := source.FailTask(context.Background(), TaskResult{TaskID: task.TaskID, SlotID: task.SlotID,
		TaskKind: task.Kind, ConfigEpoch: task.ConfigEpoch, Attempt: task.Attempt, Err: "fence-test"}); err != nil {
		t.Fatal(err)
	}
	waitForState(t, target, func(next ClusterState) bool { return next.Revision > st.Revision })
}

func promotionFenceRuntime(t *testing.T) (*Runtime, ClusterState) {
	t.Helper()
	runtime := startSingleVoterRuntime(t, "promotion-fence")
	for _, nodeID := range []uint64{2, 3} {
		addr := "n2"
		if nodeID == 3 {
			addr = "n3"
		}
		if _, err := runtime.JoinNode(context.Background(), JoinNodeRequest{NodeID: nodeID, Addr: addr, Roles: []NodeRole{NodeRoleData}, CapacityWeight: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.ActivateNode(context.Background(), ActivateNodeRequest{NodeID: nodeID}); err != nil {
			t.Fatal(err)
		}
	}
	st := waitForState(t, runtime, func(st ClusterState) bool { return len(st.Tasks) == 1 && len(st.Slots) == 1 })
	task := st.Tasks[0]
	if err := runtime.CompleteTask(context.Background(), TaskResult{TaskID: task.TaskID, SlotID: task.SlotID, TaskKind: task.Kind, ConfigEpoch: task.ConfigEpoch, Attempt: task.Attempt}); err != nil {
		t.Fatal(err)
	}
	return runtime, waitForState(t, runtime, func(st ClusterState) bool { return len(st.Tasks) == 0 })
}

func promotionFenceSlotRequest(st ClusterState) SlotReplicaMoveRequest {
	return SlotReplicaMoveRequest{SlotID: 1, TargetNode: 2, TargetPeers: []uint64{1, 2}, ConfigEpoch: st.Slots[0].ConfigEpoch,
		StateRevision: st.Revision, TargetReplicaCount: 3, TransitionTargetNodes: []uint64{1, 2, 3}}
}
