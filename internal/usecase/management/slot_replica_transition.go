package management

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
)

var (
	// ErrSlotReplicaTransitionUnavailable reports missing control dependencies.
	ErrSlotReplicaTransitionUnavailable = errors.New("internal/usecase/management: slot replica transition unavailable")
	// ErrSlotReplicaTransitionInvalid reports a topology that cannot safely enter
	// the supported single-voter to three-voter transition.
	ErrSlotReplicaTransitionInvalid = errors.New("internal/usecase/management: invalid slot replica transition")
)

// SlotReplicaTransitionRequest selects the exact three data nodes that every
// physical Slot must use after the transition.
type SlotReplicaTransitionRequest struct {
	TargetNodeIDs []uint64
}

// SlotReplicaTransitionResponse is one idempotent transition advance result.
type SlotReplicaTransitionResponse struct {
	GeneratedAt        time.Time
	StateRevision      uint64
	SourceReplicaCount uint16
	TargetReplicaCount uint16
	CompletedSlots     int
	TotalSlots         int
	Phase              string
	Created            bool
	Task               *SlotTask
}

// AdvanceSlotReplicaTransition advances at most one durable Slot membership
// task. Repeated calls are idempotent; a failed task is re-armed at its last
// proven phase for forward recovery.
func (a *App) AdvanceSlotReplicaTransition(ctx context.Context, req SlotReplicaTransitionRequest) (SlotReplicaTransitionResponse, error) {
	if a == nil || a.cluster == nil || a.slotReplicaMove == nil {
		return SlotReplicaTransitionResponse{}, ErrSlotReplicaTransitionUnavailable
	}
	snapshot, err := a.cluster.LocalControlSnapshot(ctx)
	if err != nil {
		return SlotReplicaTransitionResponse{}, err
	}
	targets, err := validateSlotReplicaTransitionTargets(snapshot, req.TargetNodeIDs)
	if err != nil {
		return SlotReplicaTransitionResponse{}, err
	}
	response := slotReplicaTransitionStatus(a.now().UTC(), snapshot)
	if response.Phase == "complete" {
		return response, nil
	}

	for _, task := range snapshot.Tasks {
		if task.Kind != control.TaskKindSlotReplicaMove {
			return SlotReplicaTransitionResponse{}, fmt.Errorf("%w: active non-transition task %s", ErrSlotReplicaTransitionInvalid, task.TaskID)
		}
		response.Task = slotTaskFromControlPtr(&task)
		response.Phase = "task_active"
		if task.SourceNode != 0 || task.Status != control.TaskStatusFailed {
			return response, nil
		}
		result, err := a.slotReplicaMove.RequestSlotReplicaMove(ctx, control.SlotReplicaMoveRequest{
			SlotID:                task.SlotID,
			TargetNode:            task.TargetNode,
			TargetPeers:           append([]uint64(nil), task.TargetPeers...),
			ConfigEpoch:           task.ConfigEpoch,
			StateRevision:         snapshot.Revision,
			TargetReplicaCount:    3,
			TransitionTargetNodes: append([]uint64(nil), targets...),
		})
		if err != nil {
			return SlotReplicaTransitionResponse{}, err
		}
		response.Created = result.Created
		response.Task = slotTaskFromControlPtr(result.Task)
		response.Phase = "task_resumed"
		return response, nil
	}

	slots := append([]control.SlotAssignment(nil), snapshot.Slots...)
	sort.Slice(slots, func(i, j int) bool { return slots[i].SlotID < slots[j].SlotID })
	for _, slot := range slots {
		if len(slot.DesiredPeers) >= 3 {
			continue
		}
		targetNode := firstMissingReplicaTransitionTarget(slot.DesiredPeers, targets)
		if targetNode == 0 {
			return SlotReplicaTransitionResponse{}, fmt.Errorf("%w: slot %d peers are not a subset of target nodes", ErrSlotReplicaTransitionInvalid, slot.SlotID)
		}
		targetPeers := append(append([]uint64(nil), slot.DesiredPeers...), targetNode)
		result, err := a.slotReplicaMove.RequestSlotReplicaMove(ctx, control.SlotReplicaMoveRequest{
			SlotID:                slot.SlotID,
			TargetNode:            targetNode,
			TargetPeers:           targetPeers,
			ConfigEpoch:           slot.ConfigEpoch,
			StateRevision:         snapshot.Revision,
			TargetReplicaCount:    3,
			TransitionTargetNodes: append([]uint64(nil), targets...),
		})
		if err != nil {
			return SlotReplicaTransitionResponse{}, err
		}
		response.Created = result.Created
		response.Task = slotTaskFromControlPtr(result.Task)
		response.Phase = "task_created"
		return response, nil
	}
	response.Phase = "converging"
	return response, nil
}

func validateSlotReplicaTransitionTargets(snapshot control.Snapshot, raw []uint64) ([]uint64, error) {
	targets := append([]uint64(nil), raw...)
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	if len(targets) != 3 || targets[0] == 0 || targets[0] == targets[1] || targets[1] == targets[2] {
		return nil, fmt.Errorf("%w: exactly three unique target node IDs are required", ErrSlotReplicaTransitionInvalid)
	}
	if snapshot.SlotReplicaCount != 1 && snapshot.SlotReplicaCount != 3 {
		return nil, fmt.Errorf("%w: source replica count must be 1 or already complete at 3", ErrSlotReplicaTransitionInvalid)
	}
	controllerCount := 0
	byID := make(map[uint64]control.Node, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		byID[node.NodeID] = node
		for _, role := range node.Roles {
			if role == control.RoleController {
				controllerCount++
				break
			}
		}
	}
	if snapshot.SlotReplicaCount != 3 && controllerCount != 1 {
		return nil, fmt.Errorf("%w: exactly one Controller voter is required until Slot expansion completes", ErrSlotReplicaTransitionInvalid)
	}
	if snapshot.SlotReplicaCountTransition != nil &&
		!sameReplicaTransitionNodes(snapshot.SlotReplicaCountTransition.TargetNodeIDs, targets) {
		return nil, fmt.Errorf("%w: requested target nodes do not match the active transition", ErrSlotReplicaTransitionInvalid)
	}
	for _, nodeID := range targets {
		node, ok := byID[nodeID]
		if !ok || !control.NodeSchedulableForPlacement(node) {
			return nil, fmt.Errorf("%w: target node %d is not an active healthy data node", ErrSlotReplicaTransitionInvalid, nodeID)
		}
	}
	for _, slot := range snapshot.Slots {
		for _, peer := range slot.DesiredPeers {
			if !containsReplicaTransitionNode(targets, peer) {
				return nil, fmt.Errorf("%w: slot %d peer %d is outside target topology", ErrSlotReplicaTransitionInvalid, slot.SlotID, peer)
			}
		}
	}
	return targets, nil
}

func sameReplicaTransitionNodes(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func slotReplicaTransitionStatus(now time.Time, snapshot control.Snapshot) SlotReplicaTransitionResponse {
	response := SlotReplicaTransitionResponse{
		GeneratedAt: now, StateRevision: snapshot.Revision,
		SourceReplicaCount: snapshot.SlotReplicaCount,
		TargetReplicaCount: 3, TotalSlots: len(snapshot.Slots), Phase: "not_started",
	}
	for _, slot := range snapshot.Slots {
		if len(slot.DesiredPeers) == 3 {
			response.CompletedSlots++
		}
	}
	if snapshot.SlotReplicaCountTransition != nil {
		response.SourceReplicaCount = snapshot.SlotReplicaCountTransition.SourceReplicaCount
		response.TargetReplicaCount = snapshot.SlotReplicaCountTransition.TargetReplicaCount
		response.Phase = "expanding"
	}
	if snapshot.SlotReplicaCount == 3 && snapshot.SlotReplicaCountTransition == nil && response.CompletedSlots == response.TotalSlots {
		response.SourceReplicaCount = 1
		response.Phase = "complete"
	}
	return response
}

func firstMissingReplicaTransitionTarget(peers, targets []uint64) uint64 {
	for _, target := range targets {
		if !containsReplicaTransitionNode(peers, target) {
			return target
		}
	}
	return 0
}

func containsReplicaTransitionNode(nodes []uint64, want uint64) bool {
	for _, node := range nodes {
		if node == want {
			return true
		}
	}
	return false
}
