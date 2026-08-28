package management

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
)

func TestAdvanceSlotReplicaTransitionCreatesOneFencedAddition(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	snapshot := control.Snapshot{
		Revision: 11, SlotReplicaCount: 1,
		Nodes: []control.Node{
			replicaTransitionNode(1, true), replicaTransitionNode(2, false), replicaTransitionNode(3, false),
		},
		Slots: []control.SlotAssignment{
			{SlotID: 2, DesiredPeers: []uint64{1}, ConfigEpoch: 4, PreferredLeader: 1},
			{SlotID: 1, DesiredPeers: []uint64{1}, ConfigEpoch: 7, PreferredLeader: 1},
		},
	}
	task := control.ReconcileTask{TaskID: "add", SlotID: 1, Kind: control.TaskKindSlotReplicaMove, TargetNode: 2}
	writer := &fakeSlotReplicaMoveWriter{result: control.SlotReplicaMoveResult{Created: true, Task: &task}}
	app := New(Options{Cluster: fakeNodeSnapshotReader{snapshot: snapshot}, SlotReplicaMove: writer, Now: func() time.Time { return now }})

	response, err := app.AdvanceSlotReplicaTransition(context.Background(), SlotReplicaTransitionRequest{TargetNodeIDs: []uint64{3, 1, 2}})
	if err != nil {
		t.Fatalf("AdvanceSlotReplicaTransition() error = %v", err)
	}
	if !response.Created || response.Phase != "task_created" || len(writer.requests) != 1 {
		t.Fatalf("response=%#v requests=%#v", response, writer.requests)
	}
	request := writer.requests[0]
	if request.SlotID != 1 || request.SourceNode != 0 || request.TargetNode != 2 || request.TargetReplicaCount != 3 || request.StateRevision != 11 || !sameUint64Slice(request.TargetPeers, []uint64{1, 2}) || !sameUint64Slice(request.TransitionTargetNodes, []uint64{1, 2, 3}) {
		t.Fatalf("request = %#v, want first Slot fenced addition", request)
	}
}

func TestAdvanceSlotReplicaTransitionRearmsFailedTask(t *testing.T) {
	snapshot := control.Snapshot{
		Revision: 20, SlotReplicaCount: 1,
		SlotReplicaCountTransition: &control.SlotReplicaCountTransition{SourceReplicaCount: 1, TargetReplicaCount: 3, StartedAtRevision: 11, TargetNodeIDs: []uint64{1, 2, 3}},
		Nodes:                      []control.Node{replicaTransitionNode(1, true), replicaTransitionNode(2, false), replicaTransitionNode(3, false)},
		Slots:                      []control.SlotAssignment{{SlotID: 1, DesiredPeers: []uint64{1}, ConfigEpoch: 7, PreferredLeader: 1}},
		Tasks: []control.ReconcileTask{{
			TaskID: "add", SlotID: 1, Kind: control.TaskKindSlotReplicaMove, Step: control.TaskStepPromoteLearner,
			TargetNode: 2, TargetPeers: []uint64{1, 2}, ConfigEpoch: 7, Attempt: 1, Status: control.TaskStatusFailed,
		}},
	}
	writer := &fakeSlotReplicaMoveWriter{result: control.SlotReplicaMoveResult{Created: true, Task: &snapshot.Tasks[0]}}
	app := New(Options{Cluster: fakeNodeSnapshotReader{snapshot: snapshot}, SlotReplicaMove: writer})

	response, err := app.AdvanceSlotReplicaTransition(context.Background(), SlotReplicaTransitionRequest{TargetNodeIDs: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatalf("AdvanceSlotReplicaTransition() error = %v", err)
	}
	if response.Phase != "task_resumed" || !response.Created || len(writer.requests) != 1 || writer.requests[0].StateRevision != 20 {
		t.Fatalf("response=%#v requests=%#v", response, writer.requests)
	}
}

func replicaTransitionNode(nodeID uint64, controller bool) control.Node {
	roles := []control.Role{control.RoleData}
	if controller {
		roles = append(roles, control.RoleController)
	}
	return control.Node{
		NodeID: nodeID, Roles: roles, JoinState: control.NodeJoinStateActive,
		Health: control.NodeHealth{Freshness: control.NodeHealthFresh, Status: control.NodeAlive, RuntimeReady: true},
	}
}
