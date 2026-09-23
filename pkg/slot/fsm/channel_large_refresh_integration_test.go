//go:build integration

package fsm

import (
	"context"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestRefreshChannelLargeUsesLatestSlotRowAndReturnsIt(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	apply := func(index uint64, data []byte) []byte {
		t.Helper()
		result, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, HashSlot: 11, Index: index, Term: 1, Data: data})
		if err != nil {
			t.Fatalf("Apply(%d): %v", index, err)
		}
		return result
	}
	apply(1, EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "g", ChannelType: 2, Ban: 1, SubscriberMutationVersion: 4}))
	apply(2, EncodeAddSubscribersCommand("g", 2, []string{"a", "b"}, 5))
	apply(3, EncodePatchChannelBusinessFlagsCommand("g", 2, metadb.ChannelBusinessFlags{Disband: 1, SendBan: 1}))
	result, err := DecodeChannelLargeRefreshResult(apply(4, EncodeRefreshChannelLargeCommand("g", 2, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.Channel.Large != 1 || result.Channel.SubscriberCount != 2 || result.Channel.Disband != 1 || result.Channel.SendBan != 1 || result.Channel.SubscriberMutationVersion != 5 {
		t.Fatalf("refresh result = %+v", result)
	}
	apply(5, EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "g", ChannelType: 2, Large: 1}))
	channel, err := db.ForSlot(11).GetChannel(ctx, "g", 2)
	if err != nil || channel.Disband != 1 {
		t.Fatalf("terminal disband cleared: %+v err=%v", channel, err)
	}
	result, err = DecodeChannelLargeRefreshResult(apply(6, EncodeRefreshChannelLargeCommand("g", 2, 2)))
	if err != nil || !result.Found || result.Channel.Large != 0 || result.Channel.Disband != 1 {
		t.Fatalf("after threshold change = %+v err=%v", result, err)
	}
	channel, err = db.ForSlot(11).GetChannel(ctx, "g", 2)
	if err != nil || channel != result.Channel {
		t.Fatalf("durable=%+v returned=%+v err=%v", channel, result, err)
	}
}
