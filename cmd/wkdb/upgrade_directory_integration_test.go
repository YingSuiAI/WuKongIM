//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	"github.com/WuKongIM/WuKongIM/pkg/controller/state"
	"github.com/WuKongIM/WuKongIM/pkg/controller/statefile"
	"github.com/WuKongIM/WuKongIM/pkg/db"
	"github.com/WuKongIM/WuKongIM/pkg/db/message"
	channel "github.com/WuKongIM/WuKongIM/pkg/db/message/channelcompat"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDirectoryUpgradeCLIInterruptedSnapshotResumesAndRestarts(t *testing.T) {
	ctx := context.Background()
	dir := seedDirectoryUpgrade(t, 3, 3)
	controllerBefore, err := os.ReadFile(filepath.Join(dir, "controller", "cluster-state.json"))
	require.NoError(t, err)
	before := directoryRawRows(t, dir)
	var output bytes.Buffer
	args := []string{"--data-dir", dir, "upgrade-person-directory", "--node-id", "1", "--cluster-id", "upgrade-test"}
	require.Equal(t, exitOK, runWithStreams(append(args, "--dry-run"), nil, &output, &output), output.String())
	require.Equal(t, before, directoryRawRows(t, dir))
	calls := 0
	restore := raftlog.TestingSetSnapshotWriteFileHook(func(path string, data []byte) error {
		calls++
		if calls == 2 {
			return errors.New("injected interruption after first Slot snapshot")
		}
		return os.WriteFile(path, data, 0o600)
	})
	code := runWithStreams(args, nil, &output, &output)
	restore()
	require.Equal(t, exitInternal, code, output.String())
	_, err = os.Stat(filepath.Join(dir, metadb.DirectoryProjectionUpgradePendingFile))
	require.NoError(t, err)
	require.Equal(t, exitOK, runWithStreams(args, nil, &output, &output), output.String())
	require.Equal(t, exitOK, runWithStreams(args, nil, &output, &output), output.String())
	_, err = os.Stat(filepath.Join(dir, metadb.DirectoryProjectionUpgradePendingFile))
	require.True(t, os.IsNotExist(err))
	controllerAfter, err := os.ReadFile(filepath.Join(dir, "controller", "cluster-state.json"))
	require.NoError(t, err)
	require.Equal(t, controllerBefore, controllerAfter)
	after := directoryRawRows(t, dir)
	require.Len(t, after, len(before))
	for key, value := range before {
		if len(key) >= 9 && binary.BigEndian.Uint32([]byte(key)[5:9]) == metadb.TableIDChannel {
			require.Len(t, after[key], 72)
			require.Equal(t, value[:56], after[key][:56])
		} else {
			require.Equal(t, value, after[key], "non-channel authority changed at %x", key)
		}
	}
	// Fresh physical opens must restore only current snapshots, never replay the
	// removed command-58 or incompatible command-59 fixtures in the old WAL.
	for restart := 0; restart < 2; restart++ {
		metaDB, err := metadb.Open(filepath.Join(dir, "slotmeta"))
		require.NoError(t, err)
		raftDB, err := raftlog.Open(filepath.Join(dir, "slotraft"), raftlog.Options{})
		require.NoError(t, err)
		runtime, err := multiraft.New(multiraft.Options{NodeID: 1, TickInterval: time.Hour, Workers: 1, Transport: upgradeTestTransport{}, Raft: multiraft.RaftOptions{ElectionTick: 10, HeartbeatTick: 1}})
		require.NoError(t, err)
		for slot := uint64(1); slot <= 2; slot++ {
			storage := raftDB.ForSlot(slot)
			snapshot, err := storage.Snapshot(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(3), snapshot.Metadata.Index)
			first, err := storage.FirstIndex(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(4), first)
			machine, err := metafsm.NewStateMachineWithHashSlots(metaDB, slot, []uint16{uint16(slot - 1)})
			require.NoError(t, err)
			require.NoError(t, runtime.OpenSlot(ctx, multiraft.SlotOptions{ID: multiraft.SlotID(slot), Storage: storage, StateMachine: machine}))
		}
		channel, err := metaDB.ForHashSlot(0).GetChannel(ctx, "alice@bob", 1)
		require.NoError(t, err)
		require.Equal(t, metadb.DirectoryProjectionReady, channel.DirectoryProjectionState)
		require.Equal(t, uint64(1), channel.DirectoryProjectionGeneration)
		user, err := metaDB.ForHashSlot(0).GetUser(ctx, "alice")
		require.NoError(t, err)
		require.Equal(t, "must-preserve-token", user.Token)
		member, err := metaDB.ForHashSlot(0).GetUserChannelMembership(ctx, "alice", "alice@bob", 1)
		require.NoError(t, err)
		require.Equal(t, uint64(13), member.ReadSeq)
		require.True(t, member.Tombstone)
		require.NoError(t, runtime.Close())
		require.NoError(t, raftDB.Close())
		require.NoError(t, metaDB.Close())
	}
}

func TestDirectoryUpgradeCLIRejectsUnappliedAndUncommittedTails(t *testing.T) {
	for _, tc := range []struct {
		name            string
		applied, commit uint64
	}{{"unapplied", 2, 3}, {"uncommitted", 2, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := seedDirectoryUpgrade(t, tc.applied, tc.commit)
			before := directoryRawRows(t, dir)
			var output bytes.Buffer
			code := runWithStreams([]string{"--data-dir", dir, "upgrade-person-directory", "--node-id", "1", "--cluster-id", "upgrade-test"}, nil, &output, &output)
			require.Equal(t, exitInternal, code)
			require.Contains(t, output.String(), "requires drained Slot")
			require.Equal(t, before, directoryRawRows(t, dir))
			_, err := os.Stat(filepath.Join(dir, metadb.DirectoryProjectionUpgradePendingFile))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestDirectoryUpgradeCLIRepairsMessageProofUsingDurableRouting(t *testing.T) {
	ctx := context.Background()
	dir := seedDirectoryUpgrade(t, 3, 3)
	store, err := db.OpenNodeStore(db.NodeStoreOptions{MetaPath: filepath.Join(dir, "slotmeta"), MessagePath: filepath.Join(dir, "messages")})
	require.NoError(t, err)
	hashSlot := routing.HashSlotForKey("proof", 2)
	authority := metadb.ChannelRuntimeMeta{ChannelID: "proof", ChannelType: 1, ChannelEpoch: 3, LeaderEpoch: 5, RouteGeneration: 11, WriteFenceVersion: 7, Leader: 1, Replicas: []uint64{1}, ISR: []uint64{1}, MinISR: 1}
	_, err = store.Meta().HashSlot(hashSlot).UpsertChannelRuntimeMeta(ctx, authority)
	require.NoError(t, err)
	log, err := store.Messages().Channel("1:proof", message.ChannelID{ID: "proof", Type: 1})
	require.NoError(t, err)
	_, err = log.Append(ctx, []message.Record{{ID: 91, ClientMsgNo: "retained", FromUID: "alice", Payload: []byte("retained history")}}, message.AppendOptions{})
	require.NoError(t, err)
	require.NoError(t, log.StoreCheckpoint(ctx, message.Checkpoint{Epoch: 3, HW: 1}))
	require.NoError(t, log.Close())
	require.NoError(t, store.Close())
	readProof := func() (message.DurableRecoveryState, error) {
		engine, err := message.Open(filepath.Join(dir, "messages"))
		require.NoError(t, err)
		defer engine.Close()
		log, err := engine.ForChannel("1:proof", channel.ChannelID{ID: "proof", Type: 1})
		require.NoError(t, err)
		defer log.Close()
		return log.LoadDurableRecovery(ctx, []uint64{1})
	}
	_, err = readProof()
	require.Error(t, err, "released rows without proposal proof must reproduce recovery failure")
	var output bytes.Buffer
	args := []string{"--data-dir", dir, "upgrade-person-directory", "--node-id", "1", "--cluster-id", "upgrade-test"}
	require.Equal(t, exitOK, runWithStreams(args, nil, &output, &output), output.String())
	proof, err := readProof()
	require.NoError(t, err)
	require.Equal(t, uint64(1), proof.LEO)
	require.Equal(t, uint64(1), proof.Committed)
	require.Equal(t, uint64(3), proof.TailIdentity.ChannelEpoch)
	require.Equal(t, uint64(5), proof.TailIdentity.LeaderTerm)
	require.Equal(t, uint64(11), proof.TailIdentity.FenceVersion, "RouteGeneration, not WriteFenceVersion, is recovery authority")
	require.Equal(t, exitOK, runWithStreams(args, nil, &output, &output), output.String())
	repeated, err := readProof()
	require.NoError(t, err)
	require.Equal(t, proof, repeated)
}

func seedDirectoryUpgrade(t *testing.T, applied, commit uint64) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	messages, err := message.Open(filepath.Join(dir, "messages"))
	require.NoError(t, err)
	require.NoError(t, messages.Close())
	require.NoError(t, os.Mkdir(filepath.Join(dir, "controller"), 0o700))
	table, err := state.BuildInitialHashSlotTable(2, 2)
	require.NoError(t, err)
	control := state.ClusterState{SchemaVersion: 1, ClusterID: "upgrade-test", Revision: 1, AppliedRaftIndex: 1,
		Config:      state.ClusterConfig{SlotCount: 2, HashSlotCount: 2, ReplicaCount: 1},
		Controllers: []state.ControllerVoter{{NodeID: 1, Addr: "n1", Role: state.ControllerRoleVoter}},
		Nodes:       []state.Node{{NodeID: 1, Addr: "n1", Roles: []state.NodeRole{state.NodeRoleData, state.NodeRoleControllerVoter}, JoinState: state.NodeJoinStateActive, Status: state.NodeStatusAlive}},
		Slots:       []state.SlotAssignment{{SlotID: 1, DesiredPeers: []uint64{1}, ConfigEpoch: 1}, {SlotID: 2, DesiredPeers: []uint64{1}, ConfigEpoch: 1}}, HashSlots: table}
	require.NoError(t, statefile.New(filepath.Join(dir, "controller", "cluster-state.json")).Save(ctx, control))
	metaDB, err := metadb.Open(filepath.Join(dir, "slotmeta"))
	require.NoError(t, err)
	shard := metaDB.ForHashSlot(0)
	require.NoError(t, shard.CreateUser(ctx, metadb.User{UID: "alice", Token: "must-preserve-token"}))
	require.NoError(t, shard.UpsertChannel(ctx, metadb.Channel{ChannelID: "alice@bob", ChannelType: 1, Ban: 1, SendBan: 1, DirectoryProjectionState: metadb.DirectoryProjectionReady, DirectoryProjectionGeneration: 1}))
	require.NoError(t, shard.UpsertUserChannelMembership(ctx, metadb.UserChannelMembership{UID: "alice", ChannelID: "alice@bob", ChannelType: 1, JoinSeq: 4, ReadSeq: 13, DeletedToSeq: 10, Tombstone: true, SourceVersion: 7}))
	require.NoError(t, metaDB.Close())
	// Exact released channel layout, independent of the target encoder.
	raw, err := pebble.Open(filepath.Join(dir, "slotmeta"), &pebble.Options{})
	require.NoError(t, err)
	keys := make([][]byte, 0, 2)
	iter, err := raw.NewIter(nil)
	require.NoError(t, err)
	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if len(key) >= 9 && binary.BigEndian.Uint32(key[5:9]) == metadb.TableIDChannel {
			keys = append(keys, bytes.Clone(key))
		}
	}
	require.NoError(t, iter.Close())
	for _, key := range keys {
		value := make([]byte, 64)
		for i, n := range []uint64{1, 0, 1, 0, 27, 2, 0, 1} {
			binary.BigEndian.PutUint64(value[i*8:], n)
		}
		require.NoError(t, raw.Set(key, value, pebble.Sync))
	}
	require.NoError(t, raw.Close())
	metaDB, err = metadb.Open(filepath.Join(dir, "slotmeta"))
	require.NoError(t, err)
	defer metaDB.Close()
	raftDB, err := raftlog.Open(filepath.Join(dir, "slotraft"), raftlog.Options{})
	require.NoError(t, err)
	defer raftDB.Close()
	for slot := uint64(1); slot <= 2; slot++ {
		machine, err := metafsm.NewStateMachineWithHashSlots(metaDB, slot, []uint16{uint16(slot - 1)})
		require.NoError(t, err)
		oldSnapshot, err := machine.Snapshot(ctx)
		require.NoError(t, err)
		storage := raftDB.ForSlot(slot)
		require.NoError(t, storage.Save(ctx, multiraft.PersistentState{Snapshot: &raftpb.Snapshot{Data: oldSnapshot.Data, Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}}))
		require.NoError(t, storage.Save(ctx, multiraft.PersistentState{HardState: &raftpb.HardState{Term: 1, Vote: 1, Commit: commit}, Entries: []raftpb.Entry{
			{Index: 2, Term: 1, Data: []byte{0, 0, 1, 59, 1, 0, 0, 0}},
			{Index: 3, Term: 1, Data: []byte{0, 0, 1, 58, 1, 0, 0, 0}},
		}}))
		require.NoError(t, storage.MarkApplied(ctx, applied))
	}
	return dir
}

func directoryRawRows(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	db, err := pebble.Open(filepath.Join(dir, "slotmeta"), &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	iter, err := db.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	rows := map[string][]byte{}
	for ok := iter.First(); ok; ok = iter.Next() {
		value, err := iter.ValueAndErr()
		require.NoError(t, err)
		rows[string(iter.Key())] = bytes.Clone(value)
	}
	require.NoError(t, iter.Error())
	return rows
}

type upgradeTestTransport struct{}

func (upgradeTestTransport) Send(context.Context, []multiraft.Envelope) error { return nil }
