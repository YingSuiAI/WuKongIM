package fsm

import (
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestPlatformRejoinCommandIsDistinctAndStrict(t *testing.T) {
	rejoin := metadb.PlatformMembershipRejoin{UID: "u", ChannelID: "g", ChannelType: 2, MembershipEpoch: 3, JoinedSeq: 50, PreviousRemovedSeq: 30, SourceVersion: 9, UpdatedAt: 123}
	data, err := EncodeRejoinUserChannelMembershipCommandChecked(rejoin)
	if err != nil {
		t.Fatal(err)
	}
	if !IsRejoinUserChannelMembershipCommand(data) || IsRejoinUserChannelMembershipCommand(EncodeNoopCommand()) {
		t.Fatal("rejoin command must have its own type")
	}
	decoded, err := decodeCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	if got := decoded.(*rejoinUserChannelMembershipCmd).rejoin; got != rejoin {
		t.Fatalf("decoded = %+v", got)
	}
	if _, err := decodeCommand(data[:len(data)-13]); err == nil {
		t.Fatal("truncated command accepted")
	}
	duplicate := append([]byte(nil), data...)
	duplicate = appendStringTLVField(duplicate, tagPlatformRejoinUID, "u")
	if _, err := decodeCommand(duplicate); err == nil {
		t.Fatal("duplicate UID accepted")
	}
	bad := rejoin
	bad.PreviousRemovedSeq = 51
	if _, err := EncodeRejoinUserChannelMembershipCommandChecked(bad); err == nil {
		t.Fatal("invalid previous removed boundary accepted")
	}
}
