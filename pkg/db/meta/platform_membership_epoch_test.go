package meta

import "testing"

func TestUserChannelMembershipPlatformEpochOptionalValue(t *testing.T) {
	old := UserChannelMembership{UID: "u", ChannelID: "g", ChannelType: 2, JoinSeq: 4, SourceVersion: 3, UpdatedAt: 10}
	oldValue := encodeUserChannelMembershipValue(old)
	if len(oldValue) != 57 {
		t.Fatalf("old value length = %d, want 57", len(oldValue))
	}
	got, err := decodeUserChannelMembershipValue(old.UID, old.ChannelID, old.ChannelType, oldValue)
	if err != nil || got != old {
		t.Fatalf("old value decode = %+v, %v", got, err)
	}
	newRow := old
	newRow.PlatformMembershipEpoch = 8
	newValue := encodeUserChannelMembershipValue(newRow)
	if len(newValue) != 65 {
		t.Fatalf("new value length = %d, want 65", len(newValue))
	}
	got, err = decodeUserChannelMembershipValue(newRow.UID, newRow.ChannelID, newRow.ChannelType, newValue)
	if err != nil || got != newRow {
		t.Fatalf("new value decode = %+v, %v", got, err)
	}
	if _, err = decodeUserChannelMembershipValue(newRow.UID, newRow.ChannelID, newRow.ChannelType, append(newValue, 0)); err == nil {
		t.Fatal("malformed future tail must be rejected")
	}
}

func TestSubscriberProjectionPreservesPlatformEpochAndRejoinFloor(t *testing.T) {
	existing := UserChannelMembership{UID: "u", ChannelID: "g", ChannelType: 2, JoinSeq: 51, ReadSeq: 50, DeletedToSeq: 50, SourceVersion: 5, PlatformMembershipEpoch: 7, UpdatedAt: 100}
	incoming := UserChannelMembership{UID: "u", ChannelID: "g", ChannelType: 2, JoinSeq: 91, SourceVersion: 6, UpdatedAt: 200}
	got := resolveUserChannelMembership(existing, true, incoming)
	if got.JoinSeq != 51 || got.ReadSeq != 50 || got.DeletedToSeq != 50 || got.PlatformMembershipEpoch != 7 || got.SourceVersion != 6 {
		t.Fatalf("repeated subscriber add changed rejoin floor: %+v", got)
	}
	existing.Tombstone = true
	existing.SourceVersion = 6
	incoming.SourceVersion = 7
	got = resolveUserChannelMembership(existing, true, incoming)
	if !got.Tombstone || got.PlatformMembershipEpoch != 7 || got.JoinSeq != 51 {
		t.Fatalf("ordinary projection revived Platform tombstone: %+v", got)
	}
}
