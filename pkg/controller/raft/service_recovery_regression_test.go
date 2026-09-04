package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/controller/command"
	"github.com/WuKongIM/WuKongIM/pkg/controller/raft/raftstore"
	"github.com/WuKongIM/WuKongIM/pkg/controller/state"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRecoverStartupRebuildsMissingMaterializedStateFromRetainedWAL(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	raftDir := filepath.Join(dir, "controller-raft")
	peers := []Peer{{NodeID: 1, Addr: "n1"}}

	store := openRecoveryRegressionStore(t, raftDir)
	entries := []raftpb.Entry{
		recoveryRegressionConfChangeEntry(t, 1, 1),
		recoveryRegressionCommandEntry(t, 2, testInitCommand("wk-recover-retained", peers)),
		recoveryRegressionCommandEntry(t, 3, recoveryRegressionUpsertNodeCommand(1, 1, "node-1-recovered")),
	}
	require.NoError(t, store.SaveReady(ctx, raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, entries, raftpb.Snapshot{}))
	require.NoError(t, store.MarkAppliedBatch(ctx, 3))
	require.NoError(t, store.Close())
	store = openRecoveryRegressionStore(t, raftDir)

	sm := newTestStateMachine(t, filepath.Join(dir, "cluster-state.json"))
	service := &Service{cfg: Config{StateMachine: sm}}
	startup, err := service.recoverStartup(ctx, store)
	require.NoError(t, err)
	require.Equal(t, uint64(3), startup.AppliedIndex)
	recovered := sm.Snapshot(ctx)
	require.Equal(t, uint64(2), recovered.Revision)
	require.Equal(t, uint64(3), recovered.AppliedRaftIndex)
	require.Equal(t, "node-1-recovered", recoveryRegressionNodeName(t, recovered, 1))
}

func TestRecoverStartupFailsClosedWhenSnapshotHasNoRecoverableData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	raftDir := filepath.Join(dir, "controller-raft")
	peers := []Peer{{NodeID: 1, Addr: "n1"}}

	store := openRecoveryRegressionStore(t, raftDir)
	entries := []raftpb.Entry{
		recoveryRegressionConfChangeEntry(t, 1, 1),
		recoveryRegressionCommandEntry(t, 2, testInitCommand("wk-recover-no-snapshot-data", peers)),
		recoveryRegressionCommandEntry(t, 3, recoveryRegressionUpsertNodeCommand(1, 1, "node-1-after-snapshot")),
	}
	require.NoError(t, store.SaveReady(ctx, raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, entries, raftpb.Snapshot{}))
	require.NoError(t, store.MarkAppliedBatch(ctx, 3))
	require.NoError(t, store.SaveSnapshot(ctx, raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index: 2,
		Term:  1,
		ConfState: raftpb.ConfState{
			Voters: []uint64{1},
		},
	}}))
	require.NoError(t, store.Close())
	store = openRecoveryRegressionStore(t, raftDir)

	sm := newTestStateMachine(t, filepath.Join(dir, "cluster-state.json"))
	service := &Service{cfg: Config{StateMachine: sm}}
	_, err := service.recoverStartup(ctx, store)
	require.ErrorContains(t, err, "has no recoverable data")
}

func openRecoveryRegressionStore(t *testing.T, dir string) *raftstore.Store {
	t.Helper()
	store, err := raftstore.Open(context.Background(), raftstore.Config{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func recoveryRegressionConfChangeEntry(t *testing.T, index, nodeID uint64) raftpb.Entry {
	t.Helper()
	data, err := (&raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: nodeID}).Marshal()
	require.NoError(t, err)
	return raftpb.Entry{Type: raftpb.EntryConfChange, Term: 1, Index: index, Data: data}
}

func recoveryRegressionCommandEntry(t *testing.T, index uint64, cmd command.Command) raftpb.Entry {
	t.Helper()
	data, err := command.Encode(cmd)
	require.NoError(t, err)
	return raftpb.Entry{Type: raftpb.EntryNormal, Term: 1, Index: index, Data: data}
}

func recoveryRegressionUpsertNodeCommand(expectedRevision, nodeID uint64, name string) command.Command {
	node := state.Node{
		NodeID: nodeID,
		Name:   name,
		Addr:   fmt.Sprintf("n%d", nodeID),
		Roles: []state.NodeRole{
			state.NodeRoleControllerVoter,
			state.NodeRoleData,
		},
		JoinState:      state.NodeJoinStateActive,
		Status:         state.NodeStatusAlive,
		CapacityWeight: 10,
	}
	return command.Command{Kind: command.KindUpsertNode, ExpectedRevision: &expectedRevision, Node: &node}
}

func recoveryRegressionNodeName(t *testing.T, st state.ClusterState, nodeID uint64) string {
	t.Helper()
	for _, node := range st.Nodes {
		if node.NodeID == nodeID {
			return node.Name
		}
	}
	t.Fatalf("node %d not found", nodeID)
	return ""
}
