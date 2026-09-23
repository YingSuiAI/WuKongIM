//go:build integration

package fsm

import (
	"context"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestPlatformRejoinRebasesVisibilityAtJoinedSequence(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	apply := func(index uint64, data []byte) string {
		t.Helper()
		result, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: index, Term: 1, Data: data})
		if err != nil {
			t.Fatalf("Apply(%d): %v", index, err)
		}
		return string(result)
	}
	initial := metadb.UserChannelMembership{UID: "u1", ChannelID: "g1", ChannelType: 2, JoinSeq: 1, DeletedToSeq: 30, Tombstone: true, TombstoneAt: 100, SourceVersion: 4, UpdatedAt: 100}
	if got := apply(1, EncodeUpsertUserChannelMembershipsCommand([]metadb.UserChannelMembership{initial})); got != ApplyResultOK {
		t.Fatal(got)
	}
	rejoin := metadb.PlatformMembershipRejoin{UID: "u1", ChannelID: "g1", ChannelType: 2, MembershipEpoch: 7, JoinedSeq: 50, PreviousRemovedSeq: 30, SourceVersion: 5, UpdatedAt: 200}
	encode := func(req metadb.PlatformMembershipRejoin) []byte {
		t.Helper()
		data, err := EncodeRejoinUserChannelMembershipCommandChecked(req)
		if err != nil {
			t.Fatalf("EncodeRejoinUserChannelMembershipCommandChecked: %v", err)
		}
		return data
	}
	if got := apply(2, encode(rejoin)); got != ApplyResultOK {
		t.Fatalf("rejoin result = %q", got)
	}
	row, err := db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if row.Tombstone || row.JoinSeq != 51 || row.DeletedToSeq != 50 || row.PlatformMembershipEpoch != 7 || row.SourceVersion != 5 {
		t.Fatalf("rejoined membership = %+v", row)
	}
	if got := apply(3, encode(rejoin)); got != ApplyResultOK {
		t.Fatalf("same epoch replay = %q", got)
	}
	stale := rejoin
	stale.MembershipEpoch = 6
	if got := apply(4, encode(stale)); got != ApplyResultMembershipEpochConflict {
		t.Fatalf("old epoch result = %q", got)
	}
	wrong := rejoin
	wrong.JoinedSeq = 60
	if got := apply(5, encode(wrong)); got != ApplyResultMembershipEpochConflict {
		t.Fatalf("same epoch wrong floor result = %q", got)
	}
	if got := apply(6, EncodeNoopCommand()); got != ApplyResultOK {
		t.Fatalf("noop result = %q", got)
	}
	if got := apply(7, EncodeDeleteUserChannelMembershipsCommand([]metadb.UserChannelMembership{{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, TombstoneAt: 300, SourceVersion: 4, UpdatedAt: 300,
	}})); got != ApplyResultOK {
		t.Fatalf("late old tombstone result = %q", got)
	}
	row, err = db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || row.Tombstone || row.JoinSeq != 51 || row.DeletedToSeq != 50 {
		t.Fatalf("membership after conflicts = %+v err=%v", row, err)
	}
	if got := apply(8, EncodeDeleteUserChannelMembershipsCommand([]metadb.UserChannelMembership{{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, TombstoneAt: 400, SourceVersion: 6, UpdatedAt: 400,
	}})); got != ApplyResultOK {
		t.Fatalf("new removal result = %q", got)
	}
	if got := apply(9, encode(rejoin)); got != ApplyResultMembershipEpochConflict {
		t.Fatalf("replay after new removal result = %q", got)
	}
	// A legacy ordinary add must not revive a Platform-owned tombstone.
	if got := apply(10, EncodeUpsertUserChannelMembershipsCommand([]metadb.UserChannelMembership{{
		UID: "u1", ChannelID: "g1", ChannelType: 2, JoinSeq: 91, SourceVersion: 7, UpdatedAt: 500,
	}})); got != ApplyResultOK {
		t.Fatalf("premature ordinary add result = %q", got)
	}
	row, err = db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || !row.Tombstone || row.PlatformMembershipEpoch != 7 || row.SourceVersion != 6 {
		t.Fatalf("membership after new removal = %+v err=%v", row, err)
	}
	newEpoch := rejoin
	newEpoch.MembershipEpoch = 8
	newEpoch.JoinedSeq = 80
	newEpoch.PreviousRemovedSeq = 50
	newEpoch.SourceVersion = 6
	if got := apply(11, encode(newEpoch)); got != ApplyResultMembershipEpochConflict {
		t.Fatalf("new epoch with stale subscriber source = %q", got)
	}
	newEpoch.SourceVersion = 7
	if got := apply(12, encode(newEpoch)); got != ApplyResultOK {
		t.Fatalf("new epoch result = %q", got)
	}
	row, err = db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || row.Tombstone || row.JoinSeq != 81 || row.DeletedToSeq != 80 || row.PlatformMembershipEpoch != 8 {
		t.Fatalf("membership after new epoch = %+v err=%v", row, err)
	}
	results, err := sm.(multiraft.BatchStateMachine).ApplyBatch(ctx, []multiraft.Command{
		{SlotID: 11, HashSlot: 11, Index: 13, Term: 1, Data: encode(stale)},
		{SlotID: 11, HashSlot: 11, Index: 14, Term: 1, Data: EncodeNoopCommand()},
	})
	if err != nil || len(results) != 2 || string(results[0]) != ApplyResultMembershipEpochConflict || string(results[1]) != ApplyResultOK {
		t.Fatalf("mixed stale and normal batch = %q, %v", results, err)
	}
	if got := apply(15, EncodeDeleteUserChannelMembershipsCommand([]metadb.UserChannelMembership{{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, TombstoneAt: 600, SourceVersion: 8, UpdatedAt: 600,
	}})); got != ApplyResultOK {
		t.Fatalf("same epoch later tombstone result = %q", got)
	}
	repair := newEpoch
	repair.SourceVersion = 9
	if got := apply(16, encode(repair)); got != ApplyResultMembershipEpochConflict {
		t.Fatalf("unmarked same epoch repair result = %q", got)
	}
	repair.RepairSameEpoch = true
	if got := apply(17, encode(repair)); got != ApplyResultOK {
		t.Fatalf("marked same epoch repair result = %q", got)
	}
	row, err = db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || row.Tombstone || row.JoinSeq != 81 || row.DeletedToSeq != 80 || row.PlatformMembershipEpoch != 8 || row.SourceVersion != 9 {
		t.Fatalf("membership after same epoch repair = %+v err=%v", row, err)
	}
}

func TestPlatformRejoinCorrectsLegacyLiveEqualSourceVersion(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	apply := func(index uint64, data []byte) string {
		t.Helper()
		result, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: index, Term: 1, Data: data})
		if err != nil {
			t.Fatal(err)
		}
		return string(result)
	}
	if got := apply(1, EncodeUpsertUserChannelMembershipsCommand([]metadb.UserChannelMembership{{UID: "u1", ChannelID: "g1", ChannelType: 2, JoinSeq: 1, SourceVersion: 5, UpdatedAt: 1}})); got != ApplyResultOK {
		t.Fatal(got)
	}
	rejoin := metadb.PlatformMembershipRejoin{UID: "u1", ChannelID: "g1", ChannelType: 2, MembershipEpoch: 3, JoinedSeq: 50, PreviousRemovedSeq: 30, RepairSameEpoch: true, SourceVersion: 5, UpdatedAt: 2}
	data, err := EncodeRejoinUserChannelMembershipCommandChecked(rejoin)
	if err != nil {
		t.Fatal(err)
	}
	if got := apply(2, data); got != ApplyResultOK {
		t.Fatalf("equal version live correction=%q", got)
	}
	row, err := db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || row.PlatformMembershipEpoch != 3 || row.JoinSeq != 51 || row.SourceVersion != 5 {
		t.Fatalf("corrected row=%+v err=%v", row, err)
	}
}
