package raftstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestWALAppendReadReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	entries := []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("cmd")}}
	hs := raftpb.HardState{Term: 1, Vote: 1, Commit: 1}
	require.NoError(t, w.appendReady(context.Background(), hs, entries, raftpb.SnapshotMetadata{}))
	require.NoError(t, w.close())

	reopened, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	defer reopened.close()
	state, err := reopened.replay()
	require.NoError(t, err)
	require.Equal(t, hs, state.HardState)
	require.Equal(t, entries, state.Entries)
}

func TestWALCutsSegmentsAndRecoversCompleteRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 256})
	require.NoError(t, err)
	for i := uint64(1); i <= 20; i++ {
		hs := raftpb.HardState{Term: 1, Vote: 1, Commit: i}
		require.NoError(t, w.appendReady(context.Background(), hs, []raftpb.Entry{{Index: i, Term: 1, Data: bytes.Repeat([]byte{'x'}, 64)}}, raftpb.SnapshotMetadata{}))
	}
	require.NoError(t, w.close())
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	require.Greater(t, len(files), 1)

	tail := files[len(files)-1]
	f, err := os.OpenFile(tail, os.O_RDWR, 0)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	require.NoError(t, f.Truncate(info.Size()-3))
	require.NoError(t, f.Close())

	reopened, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 256})
	require.NoError(t, err)
	defer reopened.close()
	state, err := reopened.replay()
	require.NoError(t, err)
	require.NotEmpty(t, state.Entries)
	require.LessOrEqual(t, state.HardState.Commit, uint64(20))
}

func TestWALReleasePrefixReopensFromRetainedSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 64})
	require.NoError(t, err)
	for i := uint64(1); i <= 20; i++ {
		hs := raftpb.HardState{Term: 1, Vote: 3, Commit: i}
		require.NoError(t, w.appendReady(context.Background(), hs, []raftpb.Entry{{Index: i, Term: 1, Data: bytes.Repeat([]byte{'x'}, 80)}}, raftpb.SnapshotMetadata{}))
	}
	before, err := walSegmentFiles(dir)
	require.NoError(t, err)
	require.Greater(t, len(before), 2)
	require.NoError(t, w.releaseBefore(20))
	after, err := walSegmentFiles(dir)
	require.NoError(t, err)
	require.Less(t, len(after), len(before))
	require.NoError(t, w.close())

	reopened, err := openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 64})
	require.NoError(t, err)
	defer reopened.close()
	state, err := reopened.replay()
	require.NoError(t, err)
	require.Equal(t, uint64(20), state.HardState.Commit)
}

func TestWALRetainedSegmentStillRejectsLaterChecksumCorruption(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 64})
	require.NoError(t, err)
	for i := uint64(1); i <= 5; i++ {
		require.NoError(t, w.appendReady(context.Background(), raftpb.HardState{Term: 1, Vote: 3, Commit: i}, []raftpb.Entry{{Index: i, Term: 1, Data: bytes.Repeat([]byte{'x'}, 80)}}, raftpb.SnapshotMetadata{}))
	}
	require.NoError(t, w.releaseBefore(5))
	remaining, err := walSegmentFiles(dir)
	require.NoError(t, err)
	require.NoError(t, w.close())

	tail := remaining[len(remaining)-1]
	data, err := os.ReadFile(tail)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(tail, data, 0o644))
	_, err = openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 64})
	require.True(t, errors.Is(err, ErrCRCMismatch), "error = %v", err)
}

func TestWALRetainedSegmentRequiresValidHeader(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "record type", mutate: func(data []byte) { data[4] = byte(recordEntries) }},
		{name: "payload length", mutate: func(data []byte) { data[3]-- }},
		{name: "node id", mutate: func(data []byte) { data[16] ^= 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			w, err := openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 64})
			require.NoError(t, err)
			for i := uint64(1); i <= 5; i++ {
				require.NoError(t, w.appendReady(context.Background(), raftpb.HardState{Term: 1, Vote: 3, Commit: i}, []raftpb.Entry{{Index: i, Term: 1, Data: bytes.Repeat([]byte{'x'}, 80)}}, raftpb.SnapshotMetadata{}))
			}
			require.NoError(t, w.releaseBefore(5))
			remaining, err := walSegmentFiles(dir)
			require.NoError(t, err)
			require.NoError(t, w.close())

			data, err := os.ReadFile(remaining[0])
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(data), 17)
			tt.mutate(data)
			require.NoError(t, os.WriteFile(remaining[0], data, 0o644))
			_, err = openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 64})
			require.Error(t, err)
		})
	}
}

func TestWALFirstSegmentStillValidatesHeaderChecksum(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, w.close())
	files, err := walSegmentFiles(dir)
	require.NoError(t, err)
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	data[5] ^= 1
	require.NoError(t, os.WriteFile(files[0], data, 0o644))
	_, err = openWAL(walConfig{Dir: dir, NodeID: 3, SegmentSize: 1 << 20})
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestWALTruncatedTailRecoveryRemainsAppendSafe(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, w.appendReady(context.Background(), raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("one")}}, raftpb.SnapshotMetadata{}))
	require.NoError(t, w.close())

	files, err := walSegmentFiles(dir)
	require.NoError(t, err)
	tail := files[len(files)-1]
	info, err := os.Stat(tail)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(tail, info.Size()-3))

	recovered, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, recovered.appendReady(context.Background(), raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{{Index: 2, Term: 1, Data: []byte("two")}}, raftpb.SnapshotMetadata{}))
	require.NoError(t, recovered.close())

	reopened, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	defer reopened.close()
	state, err := reopened.replay()
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.HardState.Commit)
	require.Equal(t, []uint64{1, 2}, []uint64{state.Entries[0].Index, state.Entries[1].Index})
}

func TestWALRejectsChecksumCorruptionWithoutTruncation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, w.appendReady(context.Background(), raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("one")}}, raftpb.SnapshotMetadata{}))
	require.NoError(t, w.close())

	files, err := walSegmentFiles(dir)
	require.NoError(t, err)
	tail := files[len(files)-1]
	data, err := os.ReadFile(tail)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(tail, data, 0o644))

	_, err = openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.ErrorIs(t, err, ErrCRCMismatch)
	info, statErr := os.Stat(tail)
	require.NoError(t, statErr)
	require.Equal(t, int64(len(data)), info.Size())
}

func TestWALRejectsEmptyNewestSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, w.appendReady(context.Background(), raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("one")}}, raftpb.SnapshotMetadata{}))
	require.NoError(t, w.close())

	emptyTail := filepath.Join(dir, segmentName(1, 2))
	require.NoError(t, os.WriteFile(emptyTail, nil, 0o644))
	_, err = openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.ErrorIs(t, err, ErrTruncatedRecord)
	info, statErr := os.Stat(emptyTail)
	require.NoError(t, statErr)
	require.Zero(t, info.Size())
}

func TestWALRejectsEmptyOlderSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	emptyOld := filepath.Join(dir, segmentName(0, 1))
	require.NoError(t, os.WriteFile(emptyOld, nil, 0o644))
	tail, err := os.OpenFile(filepath.Join(dir, segmentName(1, 2)), os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	require.NoError(t, writeRecord(tail, walRecord{Type: recordSegmentHeader, Payload: marshalUint64(1)}, 0))
	require.NoError(t, tail.Close())

	_, err = openWAL(walConfig{Dir: dir, NodeID: 1, SegmentSize: 1 << 20})
	require.ErrorIs(t, err, ErrTruncatedRecord)
	info, statErr := os.Stat(emptyOld)
	require.NoError(t, statErr)
	require.Zero(t, info.Size())
}
