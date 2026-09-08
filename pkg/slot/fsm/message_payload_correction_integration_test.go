//go:build integration

package fsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/stretchr/testify/require"
)

func TestPayloadCorrectionCreateReplayConflictAndRealSnapshot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	meta := metadb.ChannelRuntimeMeta{ChannelID: "correction", ChannelType: 2, ChannelEpoch: 1, LeaderEpoch: 1,
		Replicas: []uint64{1}, ISR: []uint64{1}, Leader: 1, MinISR: 1}
	if _, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: 1, Term: 1, Data: EncodeUpsertChannelRuntimeMetaCommand(meta)}); err != nil {
		t.Fatal(err)
	}
	original, corrected := sha256.Sum256([]byte("old")), sha256.Sum256([]byte("canonical"))
	value := metadb.MessagePayloadCorrection{ChannelID: "correction", ChannelType: 2, MessageSeq: 9, MessageID: 71,
		FromUID: "sender", ClientMsgNo: "client", OperationID: "operation", OriginalPayloadSHA256: hex.EncodeToString(original[:]),
		CorrectedPayloadSHA256: hex.EncodeToString(corrected[:]), CorrectedPayload: []byte("canonical")}
	index := uint64(1)
	apply := func(machine multiraft.StateMachine, value metadb.MessagePayloadCorrection, want metadb.PayloadCorrectionStatus) {
		t.Helper()
		data, err := EncodeMessagePayloadCorrectionCommand(value)
		if err != nil {
			t.Fatal(err)
		}
		index++
		encoded, err := machine.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: index, Term: 1, Data: data})
		if err != nil {
			t.Fatalf("expected result must not fail Slot: %v", err)
		}
		result, err := DecodeMessagePayloadCorrectionResult(encoded)
		if err != nil || result != want {
			t.Fatalf("result=%v err=%v want=%v", result, err, want)
		}
	}
	apply(sm, value, metadb.PayloadCorrectionApplied)
	apply(sm, value, metadb.PayloadCorrectionReplayed)
	conflicting := value
	conflicting.OperationID = "different-operation"
	apply(sm, conflicting, metadb.PayloadCorrectionConflict)
	apply(sm, value, metadb.PayloadCorrectionReplayed)
	snapshot, err := sm.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restoredDB := openTestDB(t)
	restored := mustNewStateMachine(t, restoredDB, 11)
	if err := restored.Restore(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	got, found, err := restoredDB.ForSlot(11).GetMessagePayloadCorrection(ctx, value.Key())
	if err != nil || !found || got.OperationID != value.OperationID || got.OriginalPayloadSHA256 != value.OriginalPayloadSHA256 ||
		got.CorrectedPayloadSHA256 != value.CorrectedPayloadSHA256 || !bytes.Equal(got.CorrectedPayload, value.CorrectedPayload) {
		t.Fatalf("restored correction missing identity/body: found=%t err=%v", found, err)
	}
	apply(restored, value, metadb.PayloadCorrectionReplayed)
	apply(restored, conflicting, metadb.PayloadCorrectionConflict)
}

func TestPayloadCorrectionPurgeIsReplicatedAndSurvivesSnapshotRestore(t *testing.T) {
	for _, kind := range []string{"retention", "disband", "channel-delete", "runtime-delete"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			db := openTestDB(t)
			sm := mustNewStateMachine(t, db, 11)
			index := uint64(0)
			apply := func(data []byte) []byte {
				index++
				result, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: index, Term: 1, Data: data})
				require.NoError(t, err)
				return result
			}
			meta := metadb.ChannelRuntimeMeta{ChannelID: "purge", ChannelType: 2, ChannelEpoch: 1, LeaderEpoch: 1, Replicas: []uint64{1}, ISR: []uint64{1}, Leader: 1, MinISR: 1}
			apply(EncodeUpsertChannelRuntimeMetaCommand(meta))
			apply(EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "purge", ChannelType: 2}))
			old, current := sha256.Sum256([]byte("old")), sha256.Sum256([]byte("current"))
			value := metadb.MessagePayloadCorrection{ChannelID: "purge", ChannelType: 2, MessageID: 80, MessageSeq: 9, FromUID: "human", ClientMsgNo: "original", OperationID: "purge-op", OriginalPayloadSHA256: hex.EncodeToString(old[:]), CorrectedPayloadSHA256: hex.EncodeToString(current[:]), CorrectedPayload: []byte("current")}
			command, err := EncodeMessagePayloadCorrectionCommand(value)
			require.NoError(t, err)
			status, err := DecodeMessagePayloadCorrectionResult(apply(command))
			require.NoError(t, err)
			require.Equal(t, metadb.PayloadCorrectionApplied, status)
			switch kind {
			case "retention":
				apply(EncodeAdvanceChannelRetentionThroughSeqCommand(metadb.ChannelRetentionAdvance{ChannelID: "purge", ChannelType: 2, ExpectedChannelEpoch: 1, ExpectedLeaderEpoch: 1, ExpectedLeader: 1, RetentionThroughSeq: 9, RetentionUpdatedAtMS: 1}))
			case "disband":
				apply(EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "purge", ChannelType: 2, Disband: 1}))
			case "channel-delete":
				apply(EncodeDeleteChannelCommand("purge", 2))
			case "runtime-delete":
				apply(EncodeDeleteChannelRuntimeMetaCommand("purge", 2))
			}
			_, found, err := db.ForSlot(11).GetMessagePayloadCorrection(ctx, value.Key())
			require.NoError(t, err)
			require.False(t, found)
			snapshot, err := sm.Snapshot(ctx)
			require.NoError(t, err)
			restoredDB := openTestDB(t)
			restored := mustNewStateMachine(t, restoredDB, 11)
			require.NoError(t, restored.Restore(ctx, snapshot))
			_, found, err = restoredDB.ForSlot(11).GetMessagePayloadCorrection(ctx, value.Key())
			require.NoError(t, err)
			require.False(t, found, "snapshot must not retain deleted corrected content")
			if kind != "channel-delete" {
				_, _, err := restoredDB.ForSlot(11).ReadCurrentPayloadCorrection(ctx, value.Key())
				require.ErrorIs(t, err, metadb.ErrNotFound, "purged current projection is unreadable, not uncorrected")
				index++
				result, err := restored.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: index, Term: 1, Data: command})
				require.NoError(t, err)
				status, err := DecodeMessagePayloadCorrectionResult(result)
				require.NoError(t, err)
				require.Equal(t, metadb.PayloadCorrectionNotFound, status, "deleted base must not resurrect on exact retry")
			}
		})
	}
}
