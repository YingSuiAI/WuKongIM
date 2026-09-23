//go:build integration

package fsm

import (
	"context"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestSameEpochLiveRepairFencesDelayedTombstone(t *testing.T) {
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
	encode := func(rejoin metadb.PlatformMembershipRejoin) []byte {
		t.Helper()
		data, err := EncodeRejoinUserChannelMembershipCommandChecked(rejoin)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	initial := metadb.UserChannelMembership{UID: "u1", ChannelID: "g1", ChannelType: 2, JoinSeq: 1, DeletedToSeq: 30, Tombstone: true, SourceVersion: 10, UpdatedAt: 100}
	if got := apply(1, EncodeUpsertUserChannelMembershipsCommand([]metadb.UserChannelMembership{initial})); got != ApplyResultOK {
		t.Fatal(got)
	}
	rejoin := metadb.PlatformMembershipRejoin{UID: "u1", ChannelID: "g1", ChannelType: 2, MembershipEpoch: 7, JoinedSeq: 50, PreviousRemovedSeq: 30, SourceVersion: 11, UpdatedAt: 200}
	if got := apply(2, encode(rejoin)); got != ApplyResultOK {
		t.Fatal(got)
	}
	if got := apply(3, EncodeAdvanceUserChannelMembershipReadSeqCommand([]metadb.UserChannelMembership{{UID: "u1", ChannelID: "g1", ChannelType: 2, ReadSeq: 55, UpdatedAt: 250}})); got != ApplyResultOK {
		t.Fatal(got)
	}
	repair := rejoin
	repair.RepairSameEpoch = true
	repair.SourceVersion = 12
	repair.UpdatedAt = 300
	if got := apply(4, encode(repair)); got != ApplyResultOK {
		t.Fatalf("live same-epoch repair = %q", got)
	}
	row, err := db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || row.Tombstone || row.SourceVersion != 12 || row.JoinSeq != 51 || row.DeletedToSeq != 50 || row.ReadSeq != 55 {
		t.Fatalf("repaired membership = %+v err=%v", row, err)
	}
	if got := apply(5, EncodeDeleteUserChannelMembershipsCommand([]metadb.UserChannelMembership{{UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, SourceVersion: 11, UpdatedAt: 400}})); got != ApplyResultOK {
		t.Fatal(got)
	}
	row, err = db.ForSlot(11).GetUserChannelMembership(ctx, "u1", "g1", 2)
	if err != nil || row.Tombstone || row.SourceVersion != 12 || row.JoinSeq != 51 || row.DeletedToSeq != 50 || row.ReadSeq != 55 {
		t.Fatalf("late tombstone changed repaired membership = %+v err=%v", row, err)
	}
}
