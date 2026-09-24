//go:build integration

package meta

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestSubscriberAddListContainsAndRemove(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	shard := store.db.HashSlot(1)
	channel := Channel{ChannelID: "group-sub", ChannelType: 1}
	if err := shard.CreateChannel(context.Background(), channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}

	if err := shard.AddSubscribers(context.Background(), channel.ChannelID, channel.ChannelType, []string{"u2", "u1", "u1"}, 7); err != nil {
		t.Fatalf("AddSubscribers(): %v", err)
	}
	list, cursor, done, err := shard.ListSubscribersPage(context.Background(), channel.ChannelID, channel.ChannelType, "", 1)
	if err != nil {
		t.Fatalf("ListSubscribersPage(): %v", err)
	}
	if done || cursor != "u1" || len(list) != 1 || list[0] != "u1" {
		t.Fatalf("first page = %+v cursor=%q done=%v", list, cursor, done)
	}
	list, cursor, done, err = shard.ListSubscribersPage(context.Background(), channel.ChannelID, channel.ChannelType, cursor, 10)
	if err != nil {
		t.Fatalf("ListSubscribersPage(next): %v", err)
	}
	if !done || cursor != "" || len(list) != 1 || list[0] != "u2" {
		t.Fatalf("next page = %+v cursor=%q done=%v", list, cursor, done)
	}
	ok, err := shard.ContainsSubscriber(context.Background(), channel.ChannelID, channel.ChannelType, "u2")
	if err != nil || !ok {
		t.Fatalf("ContainsSubscriber(u2) = %v err %v, want true", ok, err)
	}
	has, err := shard.HasSubscribers(context.Background(), channel.ChannelID, channel.ChannelType)
	if err != nil || !has {
		t.Fatalf("HasSubscribers() = %v err %v, want true", has, err)
	}
	snapshot, err := shard.SnapshotSubscribers(context.Background(), channel.ChannelID, channel.ChannelType)
	if err != nil {
		t.Fatalf("SnapshotSubscribers(): %v", err)
	}
	if len(snapshot) != 2 || snapshot[0] != "u1" || snapshot[1] != "u2" {
		t.Fatalf("snapshot = %+v, want [u1 u2]", snapshot)
	}

	if err := shard.RemoveSubscribers(context.Background(), channel.ChannelID, channel.ChannelType, []string{"u1"}, 9); err != nil {
		t.Fatalf("RemoveSubscribers(): %v", err)
	}
	ok, err = shard.ContainsSubscriber(context.Background(), channel.ChannelID, channel.ChannelType, "u1")
	if err != nil || ok {
		t.Fatalf("ContainsSubscriber(u1) = %v err %v, want false", ok, err)
	}
	snapshot, err = shard.SnapshotSubscribers(context.Background(), channel.ChannelID, channel.ChannelType)
	if err != nil {
		t.Fatalf("SnapshotSubscribers(after remove): %v", err)
	}
	if len(snapshot) != 1 || snapshot[0] != "u2" {
		t.Fatalf("snapshot after remove = %+v, want [u2]", snapshot)
	}
}

func TestSubscriberMutationsMaintainChannelSubscriberCount(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	shard := store.db.HashSlot(4)
	ctx := context.Background()
	channel := Channel{ChannelID: "group-count-sub", ChannelType: 1}
	if err := shard.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}

	if err := shard.AddSubscribers(ctx, channel.ChannelID, channel.ChannelType, []string{"u2", "u1", "u1"}, 1); err != nil {
		t.Fatalf("AddSubscribers(first): %v", err)
	}
	got, ok, err := shard.GetChannel(ctx, channel.ChannelID, channel.ChannelType)
	if err != nil || !ok {
		t.Fatalf("GetChannel(after add) ok=%v err=%v", ok, err)
	}
	if got.SubscriberCount != 2 {
		t.Fatalf("SubscriberCount after first add = %d, want 2", got.SubscriberCount)
	}

	if err := shard.AddSubscribers(ctx, channel.ChannelID, channel.ChannelType, []string{"u2", "u3"}, 2); err != nil {
		t.Fatalf("AddSubscribers(second): %v", err)
	}
	got, ok, err = shard.GetChannel(ctx, channel.ChannelID, channel.ChannelType)
	if err != nil || !ok {
		t.Fatalf("GetChannel(after second add) ok=%v err=%v", ok, err)
	}
	if got.SubscriberCount != 3 {
		t.Fatalf("SubscriberCount after second add = %d, want 3", got.SubscriberCount)
	}

	if err := shard.RemoveSubscribers(ctx, channel.ChannelID, channel.ChannelType, []string{"missing", "u1"}, 3); err != nil {
		t.Fatalf("RemoveSubscribers(): %v", err)
	}
	got, ok, err = shard.GetChannel(ctx, channel.ChannelID, channel.ChannelType)
	if err != nil || !ok {
		t.Fatalf("GetChannel(after remove) ok=%v err=%v", ok, err)
	}
	if got.SubscriberCount != 2 {
		t.Fatalf("SubscriberCount after remove = %d, want 2", got.SubscriberCount)
	}
}

func TestWriteBatchSubscriberMutationsMaintainChannelSubscriberCount(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	}()
	shard := db.ForHashSlot(5)
	channel := Channel{ChannelID: "group-batch-count-sub", ChannelType: 1}
	if err := shard.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}

	batch := db.NewWriteBatch()
	defer batch.Close()
	if err := batch.AddSubscribers(5, channel.ChannelID, channel.ChannelType, []string{"u1", "u2"}, 1); err != nil {
		t.Fatalf("AddSubscribers(stage): %v", err)
	}
	if err := batch.RemoveSubscribers(5, channel.ChannelID, channel.ChannelType, []string{"u1", "missing"}, 2); err != nil {
		t.Fatalf("RemoveSubscribers(stage): %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit(): %v", err)
	}

	got, err := shard.GetChannel(ctx, channel.ChannelID, channel.ChannelType)
	if err != nil {
		t.Fatalf("GetChannel(after batch): %v", err)
	}
	if got.SubscriberCount != 1 {
		t.Fatalf("SubscriberCount after batch = %d, want 1", got.SubscriberCount)
	}
	snapshot, err := shard.ListSubscribersSnapshot(ctx, channel.ChannelID, channel.ChannelType)
	if err != nil {
		t.Fatalf("ListSubscribersSnapshot(after batch): %v", err)
	}
	if len(snapshot) != 1 || snapshot[0] != "u2" {
		t.Fatalf("snapshot after batch = %+v, want [u2]", snapshot)
	}
}

func TestWriteBatchSubscriberMutationsReportExactChangedCount(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	}()
	shard := db.ForHashSlot(5)
	channel := Channel{ChannelID: "group-batch-changed-count", ChannelType: 2}
	if err := shard.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}
	if err := shard.AddSubscribers(ctx, channel.ChannelID, channel.ChannelType, []string{"existing"}, 1); err != nil {
		t.Fatalf("seed AddSubscribers(): %v", err)
	}

	batch := db.NewWriteBatch()
	defer batch.Close()
	addResult, err := batch.AddSubscribersCounted(5, channel.ChannelID, channel.ChannelType, []string{"existing", "new", "new"}, 2)
	if err != nil {
		t.Fatalf("AddSubscribersCounted(): %v", err)
	}
	removeResult, err := batch.RemoveSubscribersCounted(5, channel.ChannelID, channel.ChannelType, []string{"missing", "existing", "missing"}, 2)
	if err != nil {
		t.Fatalf("RemoveSubscribersCounted(): %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit(): %v", err)
	}

	if addResult.RequestedCount != 2 || addResult.ChangedCount != 1 || addResult.Version != 2 {
		t.Fatalf("add result = %#v, want requested=2 changed=1 version=2", addResult)
	}
	if removeResult.RequestedCount != 2 || removeResult.ChangedCount != 1 || removeResult.Version != 3 {
		t.Fatalf("remove result = %#v, want requested=2 changed=1 version=3", removeResult)
	}
}

func TestConditionalSubscriberCompensationCannotRemoveNewerRejoin(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	shard := db.ForHashSlot(5)
	const channelID = "conditional-rejoin"
	if err := shard.CreateChannel(ctx, Channel{ChannelID: channelID, ChannelType: 2}); err != nil {
		t.Fatal(err)
	}
	if err := shard.AddSubscribers(ctx, channelID, 2, []string{"u1"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := shard.AddSubscribers(ctx, channelID, 2, []string{"u1"}, 2); err != nil {
		t.Fatal(err)
	}
	if generation, found, err := shard.SubscriberGeneration(ctx, channelID, 2, "u1"); err != nil || !found || generation != 1 {
		t.Fatalf("repeated ordinary Add rewrote existing generation: generation=%d found=%t err=%v", generation, found, err)
	}
	// An unrelated member advances the Channel version, but u1 still belongs
	// to add generation 1 and can be compensated safely.
	if err := shard.AddSubscribers(ctx, channelID, 2, []string{"u2"}, 3); err != nil {
		t.Fatal(err)
	}
	unrelated := db.NewWriteBatch()
	unrelatedResult, err := unrelated.RemoveSubscribersIfVersion(5, channelID, 2, []string{"u1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := unrelated.Commit(); err != nil {
		t.Fatal(err)
	}
	unrelated.Close()
	if !unrelatedResult.Applied || unrelatedResult.Version != 4 {
		t.Fatalf("unrelated version blocked compensation: %+v", unrelatedResult)
	}
	u2, err := shard.ContainsSubscriber(ctx, channelID, 2, "u2")
	if err != nil || !u2 {
		t.Fatalf("unrelated member removed: member=%t err=%v", u2, err)
	}
	// A later strict rejoin force-writes generation 5 before generation 1's
	// delayed compensation runs.
	rejoin := db.NewWriteBatch()
	_, err = rejoin.AddSubscribersCountedForceGeneration(5, channelID, 2, []string{"u1"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := rejoin.Commit(); err != nil {
		t.Fatal(err)
	}
	rejoin.Close()
	stale := db.NewWriteBatch()
	staleResult, err := stale.RemoveSubscribersIfVersion(5, channelID, 2, []string{"u1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Commit(); err != nil {
		t.Fatal(err)
	}
	stale.Close()
	if staleResult.Applied {
		t.Fatalf("stale compensation applied: %+v", staleResult)
	}
	member, err := shard.ContainsSubscriber(ctx, channelID, 2, "u1")
	if err != nil || !member {
		t.Fatalf("newer rejoin was removed: member=%t err=%v", member, err)
	}
	current := db.NewWriteBatch()
	currentResult, err := current.RemoveSubscribersIfVersion(5, channelID, 2, []string{"u1"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.Commit(); err != nil {
		t.Fatal(err)
	}
	current.Close()
	if !currentResult.Applied || currentResult.Version != 6 {
		t.Fatalf("current compensation result: %+v", currentResult)
	}
	member, err = shard.ContainsSubscriber(ctx, channelID, 2, "u1")
	if err != nil || member {
		t.Fatalf("current compensation did not remove: member=%t err=%v", member, err)
	}
	invalid := db.NewWriteBatch()
	defer invalid.Close()
	if _, err := invalid.RemoveSubscribersIfVersion(5, channelID, 2, []string{"u1"}, ^uint64(0)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("max version error=%v", err)
	}
}

func TestConditionalSubscriberCompensationHandlesLegacyNilGeneration(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	shard := db.ForHashSlot(5)
	seedLegacy := func(channelID string) {
		t.Helper()
		if err := shard.CreateChannel(ctx, Channel{ChannelID: channelID, ChannelType: 2, SubscriberCount: 1, SubscriberMutationVersion: 1}); err != nil {
			t.Fatal(err)
		}
		key, err := subscriberRowKey(5, channelID, 2, "u1")
		if err != nil {
			t.Fatal(err)
		}
		batch := shard.db.engine.NewBatch()
		defer batch.Close()
		if err := batch.Set(key, nil); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		generation, found, err := shard.SubscriberGeneration(ctx, channelID, 2, "u1")
		if err != nil || !found || generation != 0 {
			t.Fatalf("legacy generation=%d found=%t err=%v", generation, found, err)
		}
	}
	seedLegacy("legacy-compensate")
	remove := db.NewWriteBatch()
	result, err := remove.RemoveSubscribersIfVersion(5, "legacy-compensate", 2, []string{"u1"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := remove.Commit(); err != nil {
		t.Fatal(err)
	}
	remove.Close()
	if !result.Applied || result.ChangedCount != 1 {
		t.Fatalf("legacy compensation result=%+v", result)
	}
	seedLegacy("legacy-rejoined")
	rejoin := db.NewWriteBatch()
	_, err = rejoin.AddSubscribersCountedForceGeneration(5, "legacy-rejoined", 2, []string{"u1"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := rejoin.Commit(); err != nil {
		t.Fatal(err)
	}
	rejoin.Close()
	stale := db.NewWriteBatch()
	staleResult, err := stale.RemoveSubscribersIfVersion(5, "legacy-rejoined", 2, []string{"u1"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Commit(); err != nil {
		t.Fatal(err)
	}
	stale.Close()
	if staleResult.Applied {
		t.Fatalf("legacy compensation removed newer generation: %+v", staleResult)
	}
	generation, found, err := shard.SubscriberGeneration(ctx, "legacy-rejoined", 2, "u1")
	if err != nil || !found || generation != 2 {
		t.Fatalf("new generation=%d found=%t err=%v", generation, found, err)
	}
}

func TestSubscriberTableKeepsLegacyRowLayout(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	shard := store.db.HashSlot(2)
	ctx := context.Background()
	channel := Channel{ChannelID: "group-layout-sub", ChannelType: 1}
	if err := shard.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}
	if err := shard.AddSubscribers(ctx, channel.ChannelID, channel.ChannelType, []string{"u1"}, 1); err != nil {
		t.Fatalf("AddSubscribers(): %v", err)
	}

	legacyKey := encodeSubscriberRowKey(2, channel.ChannelID, channel.ChannelType, "u1", subscriberPrimaryFamilyID)
	if _, ok, err := store.engine.Get(legacyKey); err != nil || !ok {
		t.Fatalf("legacy subscriber key ok=%v err=%v, want ok", ok, err)
	}
	runtimeKey, err := encodeTablePrimaryRowKey(2, TableIDSubscriber, KeyParts{String(channel.ChannelID), Int64Ordered(channel.ChannelType), String("u1")}, subscriberPrimaryFamilyID)
	if err != nil {
		t.Fatalf("runtime subscriber key: %v", err)
	}
	if !bytes.Equal(runtimeKey, legacyKey) {
		t.Fatalf("runtime key %x, want legacy key %x", runtimeKey, legacyKey)
	}
}

func TestSubscriberPageLimitDoesNotDecodeTail(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	shard := store.db.HashSlot(3)
	ctx := context.Background()
	channel := Channel{ChannelID: "group-tail-sub", ChannelType: 1}
	if err := shard.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}
	if err := shard.AddSubscribers(ctx, channel.ChannelID, channel.ChannelType, []string{"u1"}, 1); err != nil {
		t.Fatalf("AddSubscribers(): %v", err)
	}
	malformedTail, err := encodeKeyParts(encodeSubscriberRowPrefix(3, channel.ChannelID, channel.ChannelType), KeyParts{String("u2")})
	if err != nil {
		t.Fatalf("encode malformed tail: %v", err)
	}
	batch := store.engine.NewBatch()
	defer batch.Close()
	if err := batch.Set(malformedTail, nil); err != nil {
		t.Fatalf("Set(malformed tail): %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("Commit(malformed tail): %v", err)
	}

	list, cursor, done, err := shard.ListSubscribersPage(ctx, channel.ChannelID, channel.ChannelType, "", 1)
	if err != nil {
		t.Fatalf("ListSubscribersPage(): %v", err)
	}
	if done || cursor != "u1" || len(list) != 1 || list[0] != "u1" {
		t.Fatalf("page = %+v cursor=%q done=%v, want u1 and more", list, cursor, done)
	}
}

func TestSubscriberMutationVersionAdvancesAndInvalidatesChannelCache(t *testing.T) {
	store := openTestMetaStore(t)
	defer store.close(t)
	shard := store.db.HashSlot(1)
	channel := Channel{ChannelID: "group-cache-sub", ChannelType: 1, SubscriberMutationVersion: 5}
	if err := shard.CreateChannel(context.Background(), channel); err != nil {
		t.Fatalf("CreateChannel(): %v", err)
	}
	if _, ok, err := shard.GetChannel(context.Background(), channel.ChannelID, channel.ChannelType); err != nil || !ok {
		t.Fatalf("GetChannel() ok=%v err=%v", ok, err)
	}
	if got := store.db.channelCacheSize(); got != 1 {
		t.Fatalf("cache size after read = %d, want 1", got)
	}
	if err := shard.AddSubscribers(context.Background(), channel.ChannelID, channel.ChannelType, []string{"u1"}, 4); err != nil {
		t.Fatalf("AddSubscribers(low version): %v", err)
	}
	if got := store.db.channelCacheSize(); got != 0 {
		t.Fatalf("cache size after subscribers = %d, want 0", got)
	}
	got, ok, err := shard.GetChannel(context.Background(), channel.ChannelID, channel.ChannelType)
	if err != nil || !ok {
		t.Fatalf("GetChannel(after low version) ok=%v err=%v", ok, err)
	}
	if got.SubscriberMutationVersion != 5 {
		t.Fatalf("version after low request = %d, want 5", got.SubscriberMutationVersion)
	}
	if err := shard.AddSubscribers(context.Background(), channel.ChannelID, channel.ChannelType, []string{"u2"}, 8); err != nil {
		t.Fatalf("AddSubscribers(high version): %v", err)
	}
	got, ok, err = shard.GetChannel(context.Background(), channel.ChannelID, channel.ChannelType)
	if err != nil || !ok {
		t.Fatalf("GetChannel(after high version) ok=%v err=%v", ok, err)
	}
	if got.SubscriberMutationVersion != 8 {
		t.Fatalf("version after high request = %d, want 8", got.SubscriberMutationVersion)
	}
}
