package controller

import (
	"context"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/controller/command"
	"github.com/WuKongIM/WuKongIM/pkg/controller/state"
)

// SlotReplicaMoveRequest describes a staged physical Slot replica move.
type SlotReplicaMoveRequest struct {
	// SlotID is the physical Slot whose replica set should change.
	SlotID uint32
	// SourceNode is the current desired peer that will be removed. Zero selects
	// a forward-only voter addition during a replica-count transition.
	SourceNode uint64
	// TargetNode is the active data node that will replace SourceNode.
	TargetNode uint64
	// TargetPeers is the desired peer set after replacing SourceNode with TargetNode.
	TargetPeers []uint64
	// ConfigEpoch fences the request to the current Slot assignment epoch.
	ConfigEpoch uint64
	// StateRevision is the Controller cluster-state revision observed by the caller.
	StateRevision uint64
	// TargetReplicaCount starts or continues a forward-only replica-count
	// transition when SourceNode is zero.
	TargetReplicaCount uint16
	// TransitionTargetNodes is the exact immutable final Slot voter topology.
	TransitionTargetNodes []uint64
}

// SlotReplicaMoveResult is returned after a move task intent is accepted.
type SlotReplicaMoveResult struct {
	// Created reports whether a durable task was proposed.
	Created bool
	// Task is the deterministic staged task written by the proposal.
	Task *ReconcileTask
}

// RequestSlotReplicaMove creates a staged move task without changing DesiredPeers.
func (r *Runtime) RequestSlotReplicaMove(ctx context.Context, req SlotReplicaMoveRequest) (SlotReplicaMoveResult, error) {
	if err := ctxErr(ctx); err != nil {
		return SlotReplicaMoveResult{}, err
	}
	st, err := r.LocalState(ctx)
	if err != nil {
		return SlotReplicaMoveResult{}, err
	}
	assignment, ok := findSlotReplicaMoveAssignment(st, req.SlotID)
	if !ok {
		return SlotReplicaMoveResult{}, fmt.Errorf("controller: slot %d assignment not found", req.SlotID)
	}
	if err := validateSlotReplicaMoveRequest(st, assignment, req); err != nil {
		return SlotReplicaMoveResult{}, err
	}
	if existing, ok := findSlotReplicaMoveTask(st.Tasks, req.SlotID); ok {
		if sameSlotReplicaMoveIntent(existing, req) {
			if existing.Status != state.TaskStatusFailed {
				task := existing
				return SlotReplicaMoveResult{Created: false, Task: (*ReconcileTask)(&task)}, nil
			}
			resumed := existing
			resumed.Status = state.TaskStatusPending
			resumed.LastError = ""
			expectedRevision := req.StateRevision
			if err := r.proposeTaskCommand(ctx, command.Command{
				Kind:             command.KindUpsertSlotReplicaMoveTask,
				ExpectedRevision: &expectedRevision,
				Task:             &resumed,
			}); err != nil {
				return SlotReplicaMoveResult{}, err
			}
			return SlotReplicaMoveResult{Created: true, Task: (*ReconcileTask)(&resumed)}, nil
		}
		return SlotReplicaMoveResult{}, fmt.Errorf("%w: slot %d task %s", ErrSlotActiveTaskConflict, req.SlotID, existing.TaskID)
	}
	taskID := fmt.Sprintf("slot-%d-replica-move-%d-to-%d-r%d", req.SlotID, req.SourceNode, req.TargetNode, req.StateRevision)
	task := state.ReconcileTask{
		TaskID:           taskID,
		SlotID:           req.SlotID,
		Kind:             state.TaskKindSlotReplicaMove,
		Step:             state.TaskStepOpenLearner,
		SourceNode:       req.SourceNode,
		TargetNode:       req.TargetNode,
		TargetPeers:      append([]uint64(nil), req.TargetPeers...),
		CompletionPolicy: state.TaskCompletionPolicySingleObserver,
		ConfigEpoch:      req.ConfigEpoch,
		Status:           state.TaskStatusPending,
	}
	expectedRevision := req.StateRevision
	var transition *state.SlotReplicaCountTransition
	if req.SourceNode == 0 && st.SlotReplicaCountTransition == nil {
		transition = &state.SlotReplicaCountTransition{
			SourceReplicaCount: st.Config.ReplicaCount,
			TargetReplicaCount: req.TargetReplicaCount,
			StartedAtRevision:  st.Revision,
			TargetNodeIDs:      append([]uint64(nil), req.TransitionTargetNodes...),
		}
	}
	if err := r.proposeTaskCommand(ctx, command.Command{
		Kind:                       command.KindUpsertSlotReplicaMoveTask,
		ExpectedRevision:           &expectedRevision,
		Task:                       &task,
		SlotReplicaCountTransition: transition,
	}); err != nil {
		return SlotReplicaMoveResult{}, err
	}
	return SlotReplicaMoveResult{Created: true, Task: (*ReconcileTask)(&task)}, nil
}

func validateSlotReplicaMoveRequest(st state.ClusterState, assignment state.SlotAssignment, req SlotReplicaMoveRequest) error {
	if req.SlotID == 0 || req.TargetNode == 0 || req.ConfigEpoch == 0 || req.StateRevision == 0 {
		return fmt.Errorf("controller: slot replica move requires slot, target, config epoch, and state revision")
	}
	if assignment.ConfigEpoch != req.ConfigEpoch {
		return fmt.Errorf("controller: slot %d config epoch %d does not match request %d", req.SlotID, assignment.ConfigEpoch, req.ConfigEpoch)
	}
	if req.SourceNode != 0 && st.SlotReplicaCountTransition != nil {
		return fmt.Errorf("controller: ordinary slot replica moves are blocked during replica count transition")
	}
	if req.SourceNode == 0 {
		if err := validateSlotReplicaAdditionRequest(st, assignment, req); err != nil {
			return err
		}
	} else if !containsSlotReplicaMovePeer(assignment.DesiredPeers, req.SourceNode) {
		return fmt.Errorf("controller: source node %d is not a desired peer for slot %d", req.SourceNode, req.SlotID)
	}
	if containsSlotReplicaMovePeer(assignment.DesiredPeers, req.TargetNode) {
		return fmt.Errorf("controller: target node %d is already a desired peer for slot %d", req.TargetNode, req.SlotID)
	}
	if !activeDataNodeForSlotReplicaMove(st.Nodes, req.TargetNode) {
		return fmt.Errorf("controller: target node %d is not an active data node", req.TargetNode)
	}
	if req.SourceNode != 0 && !sameUint64Set(req.TargetPeers, replaceSlotReplicaMovePeer(assignment.DesiredPeers, req.SourceNode, req.TargetNode)) {
		return fmt.Errorf("controller: target peers must replace source node %d with target node %d", req.SourceNode, req.TargetNode)
	}
	for _, task := range st.Tasks {
		if task.SlotID == req.SlotID {
			if sameSlotReplicaMoveIntent(task, req) {
				continue
			}
			return fmt.Errorf("%w: slot %d task %s", ErrSlotActiveTaskConflict, req.SlotID, task.TaskID)
		}
	}
	return nil
}

func validateSlotReplicaAdditionRequest(st state.ClusterState, assignment state.SlotAssignment, req SlotReplicaMoveRequest) error {
	if req.TargetReplicaCount <= st.Config.ReplicaCount {
		return fmt.Errorf("controller: target replica count must increase current replica count")
	}
	if req.TargetReplicaCount != 3 {
		return fmt.Errorf("controller: only the supported replica count transition to 3 is allowed")
	}
	if len(st.Controllers) != 1 {
		return fmt.Errorf("controller: slot replica count transition requires exactly one Controller voter before promotion")
	}
	if st.SlotReplicaCountTransition != nil &&
		(st.SlotReplicaCountTransition.SourceReplicaCount != st.Config.ReplicaCount ||
			st.SlotReplicaCountTransition.TargetReplicaCount != req.TargetReplicaCount ||
			!sameUint64Set(st.SlotReplicaCountTransition.TargetNodeIDs, req.TransitionTargetNodes)) {
		return fmt.Errorf("controller: another slot replica count transition is active")
	}
	if len(req.TransitionTargetNodes) != int(req.TargetReplicaCount) ||
		!containsSlotReplicaMovePeer(req.TransitionTargetNodes, req.TargetNode) {
		return fmt.Errorf("controller: exact transition target nodes are required")
	}
	for _, peer := range assignment.DesiredPeers {
		if !containsSlotReplicaMovePeer(req.TransitionTargetNodes, peer) {
			return fmt.Errorf("controller: slot %d peer %d is outside transition target nodes", req.SlotID, peer)
		}
	}
	want := append(append([]uint64(nil), assignment.DesiredPeers...), req.TargetNode)
	if !sameUint64Set(req.TargetPeers, want) {
		return fmt.Errorf("controller: target peers must append target node %d", req.TargetNode)
	}
	if len(req.TargetPeers) > int(req.TargetReplicaCount) {
		return fmt.Errorf("controller: target peers exceed target replica count")
	}
	return nil
}

func findSlotReplicaMoveTask(tasks []state.ReconcileTask, slotID uint32) (state.ReconcileTask, bool) {
	for _, task := range tasks {
		if task.SlotID == slotID {
			return task, true
		}
	}
	return state.ReconcileTask{}, false
}

func sameSlotReplicaMoveIntent(task state.ReconcileTask, req SlotReplicaMoveRequest) bool {
	return task.Kind == state.TaskKindSlotReplicaMove &&
		task.SlotID == req.SlotID && task.SourceNode == req.SourceNode &&
		task.TargetNode == req.TargetNode && task.ConfigEpoch == req.ConfigEpoch &&
		sameUint64Set(task.TargetPeers, req.TargetPeers)
}

func findSlotReplicaMoveAssignment(st state.ClusterState, slotID uint32) (state.SlotAssignment, bool) {
	for _, assignment := range st.Slots {
		if assignment.SlotID == slotID {
			return assignment, true
		}
	}
	return state.SlotAssignment{}, false
}

func activeDataNodeForSlotReplicaMove(nodes []state.Node, nodeID uint64) bool {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return node.JoinState == state.NodeJoinStateActive && node.HasRole(state.NodeRoleData)
		}
	}
	return false
}

func replaceSlotReplicaMovePeer(peers []uint64, source uint64, target uint64) []uint64 {
	out := append([]uint64(nil), peers...)
	for i, peer := range out {
		if peer == source {
			out[i] = target
			break
		}
	}
	return out
}

func containsSlotReplicaMovePeer(peers []uint64, want uint64) bool {
	for _, peer := range peers {
		if peer == want {
			return true
		}
	}
	return false
}
