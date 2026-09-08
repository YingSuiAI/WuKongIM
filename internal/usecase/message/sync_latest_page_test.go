package message

import (
	"context"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestLatestPageSentinelIsIndependentOfMembershipVisibilityFloor(t *testing.T) {
	for _, pull := range []PullMode{PullModeDown, PullModeUp} {
		reader := &recordingChannelMessageReader{}
		app := New(Options{
			Reader: reader,
			Memberships: &recordingSyncMembershipStore{row: metadb.UserChannelMembership{
				UID: "u1", ChannelID: "g1", ChannelType: 2, JoinSeq: 8, DeletedToSeq: 10,
			}, ok: true},
			MembershipAuthority: allowLiveMembershipAuthority(),
		})
		_, err := app.SyncChannelMessages(context.Background(), SyncChannelMessagesQuery{
			LoginUID: "u1", ChannelID: "g1", ChannelType: 2, Limit: 30, PullMode: pull,
		})
		if err != nil || len(reader.queries) != 1 {
			t.Fatalf("latest read: queries=%+v err=%v", reader.queries, err)
		}
		if query := reader.queries[0]; query.StartSeq != 0 || query.EndSeq != 0 || query.MinSeq != 11 {
			t.Fatalf("pull=%d rewrote latest sentinel or lost visibility floor: %+v", pull, query)
		}
	}
}
