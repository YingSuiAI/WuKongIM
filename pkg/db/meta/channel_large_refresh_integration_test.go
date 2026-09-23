//go:build integration

package meta

import (
	"context"
	"testing"
)

func TestRefreshChannelLargeUsesCurrentCountAndPreservesFlags(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	shard := db.ForHashSlot(7)
	seed := Channel{ChannelID: "g", ChannelType: 2, Ban: 1, Disband: 1, SendBan: 1, AllowStranger: 1, SubscriberMutationVersion: 8}
	if err := shard.UpsertChannel(ctx, seed); err != nil {
		t.Fatal(err)
	}
	if err := shard.AddSubscribers(ctx, "g", 2, []string{"a", "b"}, 9); err != nil {
		t.Fatal(err)
	}
	batch := db.NewWriteBatch()
	result, err := batch.RefreshChannelLarge(7, "g", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
	got := result.Channel
	if !result.Found || got.Large != 1 || got.SubscriberCount != 2 || got.Ban != 1 || got.Disband != 1 || got.SendBan != 1 || got.AllowStranger != 1 || got.SubscriberMutationVersion != 9 {
		t.Fatalf("refreshed channel = %+v", result)
	}
	if err := shard.RemoveSubscribers(ctx, "g", 2, []string{"b"}, 11); err != nil {
		t.Fatal(err)
	}
	batch = db.NewWriteBatch()
	result, err = batch.RefreshChannelLarge(7, "g", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.Channel.Large != 0 || result.Channel.SubscriberCount != 1 || result.Channel.Disband != 1 {
		t.Fatalf("after remove = %+v", result)
	}
	got, err = shard.GetChannel(ctx, "g", 2)
	if err != nil || got != result.Channel {
		t.Fatalf("durable=%+v returned=%+v err=%v", got, result, err)
	}
}

func TestWriteBatchUpsertCannotClearDisband(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ForHashSlot(7).UpsertChannel(ctx, Channel{ChannelID: "g", ChannelType: 2, Disband: 1}); err != nil {
		t.Fatal(err)
	}
	batch := db.NewWriteBatch()
	if err := batch.UpsertChannel(7, Channel{ChannelID: "g", ChannelType: 2}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := db.ForHashSlot(7).GetChannel(ctx, "g", 2)
	if err != nil || got.Disband != 1 {
		t.Fatalf("disband cleared: %+v err=%v", got, err)
	}
}
