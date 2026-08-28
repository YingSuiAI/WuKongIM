package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/engine"
)

func TestDirectoryProjectionUpgradeReadsOldChannel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta")
	eng, err := engine.Open(path, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// The released f65b writer emitted eight big-endian uint64 fields.
	value := make([]byte, 64)
	for i, v := range []uint64{1, 1, 1, 1, 27, 2, 1, 1} {
		binary.BigEndian.PutUint64(value[i*8:], v)
	}
	key := encodeChannelRowKey(7, "alice@bob", 1, channelPrimaryFamilyID)
	batch := eng.NewBatch()
	if err := batch.Set(key, value); err != nil {
		t.Fatal(err)
	}
	index := append(encodeIndexPrefix(7, TableIDChannel, channelIDIndexID), key[len(encodeRowPrefix(7, TableIDChannel)):len(key)-2]...)
	if err := batch.Set(index, value); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if count, err := db.UpgradeDirectoryProjection(context.Background()); err != nil || count != 1 {
		t.Fatalf("upgrade: count=%d err=%v", count, err)
	}
	if count, err := db.UpgradeDirectoryProjection(context.Background()); err != nil || count != 0 {
		t.Fatalf("repeat: count=%d err=%v", count, err)
	}
	channel, err := db.ForHashSlot(7).GetChannel(context.Background(), "alice@bob", 1)
	if err != nil {
		t.Fatal(err)
	}
	if channel.DirectoryProjectionState != DirectoryProjectionReady || channel.DirectoryProjectionGeneration != 1 || channel.SubscriberMutationVersion != 27 || channel.SubscriberCount != 2 || channel.Ban != 1 || channel.Disband != 1 || channel.SendBan != 1 {
		t.Fatalf("lost authority: %+v", channel)
	}
	// The existing projection consumer must recognize old ready=true as completed,
	// not as the new enum's pending=1, and must not recreate a task/membership.
	wb := db.NewWriteBatch()
	defer wb.Close()
	if err := wb.EnsurePersonDirectoryTask(7, PersonDirectoryTask{ChannelID: "alice@bob", ChannelType: 1, Generation: normalizeChannelRuntimeMeta(ChannelRuntimeMeta{ChannelType: 1}).DirectoryGeneration}); err != nil {
		t.Fatal(err)
	}
	if err := wb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := db.meta.HashSlot(7).GetPersonDirectoryTask(context.Background(), "alice@bob", 1); err != nil || found {
		t.Fatalf("recreated completed projection: found=%v err=%v", found, err)
	}
	indexed, err := db.meta.HashSlot(7).ListChannelsByChannelID(context.Background(), "alice@bob")
	if err != nil || len(indexed) != 1 || indexed[0] != channel {
		t.Fatalf("covering index=%+v err=%v", indexed, err)
	}
}

func TestDirectoryProjectionUpgradeValueFormats(t *testing.T) {
	for _, ready := range []uint64{0, 1} {
		old := make([]byte, 64)
		binary.BigEndian.PutUint64(old[56:], ready)
		upgraded, err := upgradeReleasedChannelValue("person", 1, old)
		if err != nil {
			t.Fatal(err)
		}
		channel, err := decodeChannelValue("person", 1, upgraded)
		if err != nil {
			t.Fatal(err)
		}
		if channel.DirectoryProjectionGeneration != ready || uint64(channel.DirectoryProjectionState) != ready*2 {
			t.Fatalf("ready=%d channel=%+v", ready, channel)
		}
	}
	current := encodeChannelValue(Channel{DirectoryProjectionState: DirectoryProjectionPending, DirectoryProjectionGeneration: 9})
	if value, err := upgradeReleasedChannelValue("person", 1, current); err != nil || !bytes.Equal(value, current) {
		t.Fatalf("current changed: %x %v", value, err)
	}
	for _, length := range []int{0, 56, 63, 65, 71, 73} {
		if _, err := upgradeReleasedChannelValue("person", 1, make([]byte, length)); err == nil {
			t.Fatalf("accepted corrupt length %d", length)
		}
	}
	invalid := make([]byte, 64)
	binary.BigEndian.PutUint64(invalid[56:], 2)
	if _, err := upgradeReleasedChannelValue("person", 1, invalid); err == nil {
		t.Fatal("accepted invalid released ready flag")
	}
}

func TestDirectoryProjectionUpgradeRejectsBeforeAnyRewrite(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	good := encodeChannelRowKey(0, "a", 1, channelPrimaryFamilyID)
	for _, id := range []string{"a", "z"} {
		key := encodeChannelRowKey(0, id, 1, channelPrimaryFamilyID)
		index := append(encodeIndexPrefix(0, TableIDChannel, channelIDIndexID), key[9:len(key)-2]...)
		batch := db.engine.NewBatch()
		if err := batch.Set(key, make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
		value := make([]byte, 64)
		if id == "z" {
			value[0] = 1
		}
		if err := batch.Set(index, value); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		batch.Close()
	}
	if _, err := db.UpgradeDirectoryProjection(context.Background()); err == nil {
		t.Fatal("accepted mismatched covering index")
	}
	value, found, err := db.engine.Get(good)
	if err != nil || !found || len(value) != 64 {
		t.Fatalf("preflight failure modified first row: %d %v %v", len(value), found, err)
	}
}
