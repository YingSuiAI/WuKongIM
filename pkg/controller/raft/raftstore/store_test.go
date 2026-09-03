package raftstore

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestStoreSaveReadyReopenAndServeStorage(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "controller-raft")
	store, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	hs := raftpb.HardState{Term: 2, Vote: 1, Commit: 3}
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryConfChange, Data: mustConfChangeData(t, 1)},
		{Index: 2, Term: 2, Type: raftpb.EntryNormal, Data: []byte("two")},
		{Index: 3, Term: 2, Type: raftpb.EntryNormal, Data: []byte("three")},
	}
	require.NoError(t, store.SaveReady(ctx, hs, entries, raftpb.Snapshot{}))
	require.NoError(t, store.MarkAppliedBatch(ctx, 2))
	require.NoError(t, store.Close())

	reopened, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	defer reopened.Close()
	gotHS, conf, err := reopened.InitialState()
	require.NoError(t, err)
	require.Equal(t, hs, gotHS)
	require.Contains(t, conf.Voters, uint64(1))
	last, err := reopened.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), last)
	gotEntries, err := reopened.Entries(2, 4, 0)
	require.NoError(t, err)
	require.Equal(t, entries[1:], gotEntries)
	require.Equal(t, uint64(2), reopened.AppliedIndex())
}

func TestStoreSnapshotAndCompactBoundsStartupSuffix(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "controller-raft")
	store, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 512})
	require.NoError(t, err)
	for i := uint64(1); i <= 10; i++ {
		hs := raftpb.HardState{Term: 1, Vote: 1, Commit: i}
		require.NoError(t, store.SaveReady(ctx, hs, []raftpb.Entry{{Index: i, Term: 1, Data: []byte{byte(i)}}}, raftpb.Snapshot{}))
	}
	snap := raftpb.Snapshot{Data: []byte(`{"revision":1}`), Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
	require.NoError(t, store.SaveSnapshot(ctx, snap))
	require.NoError(t, store.Compact(ctx, 6))
	require.NoError(t, store.Close())

	reopened, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 512})
	require.NoError(t, err)
	defer reopened.Close()
	first, err := reopened.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(9), first)
	loaded, err := reopened.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(8), loaded.Metadata.Index)
}

func TestStoreCompactKeepsSuffixInSegmentCrossingBoundary(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "controller-raft")
	store, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 64})
	require.NoError(t, err)
	require.NoError(t, store.SaveReady(ctx,
		raftpb.HardState{Term: 1, Vote: 1, Commit: 3},
		[]raftpb.Entry{{Index: 1, Term: 1, Data: make([]byte, 80)}, {Index: 2, Term: 1}, {Index: 3, Term: 1}},
		raftpb.Snapshot{},
	))
	require.NoError(t, store.SaveReady(ctx,
		raftpb.HardState{Term: 1, Vote: 1, Commit: 7},
		[]raftpb.Entry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1, Data: make([]byte, 80)}},
		raftpb.Snapshot{},
	))
	require.NoError(t, store.SaveReady(ctx,
		raftpb.HardState{Term: 1, Vote: 1, Commit: 8},
		[]raftpb.Entry{{Index: 8, Term: 1}},
		raftpb.Snapshot{},
	))
	require.NoError(t, store.Compact(ctx, 5))
	require.NoError(t, store.Close())

	reopened, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 64})
	require.NoError(t, err)
	defer reopened.Close()
	entries, err := reopened.Entries(6, 9, 0)
	require.NoError(t, err)
	require.Equal(t, []uint64{6, 7, 8}, []uint64{entries[0].Index, entries[1].Index, entries[2].Index})
}

func TestStoreSaveReadySkipsMetadataRewriteWhenReadyHasNoDurableState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("read-only directory permissions are platform-specific")
	}
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "controller-raft")
	store, err := Open(ctx, Config{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o755)
		_ = store.Close()
	})
	require.NoError(t, os.Chmod(dir, 0o555))

	require.NoError(t, store.SaveReady(ctx, raftpb.HardState{}, nil, raftpb.Snapshot{}))
}

func mustConfChangeData(t *testing.T, nodeID uint64) []byte {
	t.Helper()
	data, err := (&raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: nodeID}).Marshal()
	require.NoError(t, err)
	return data
}
