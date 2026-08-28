package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/engine"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/keycodec"
)

// DirectoryProjectionUpgradePendingFile fences server startup while the offline
// directory format upgrade is converting metadata and replacing Raft snapshots.
const DirectoryProjectionUpgradePendingFile = "directory-projection-upgrade.pending"

// ValidateDirectoryProjectionUpgrade checks every channel row and covering index
// before an offline upgrade writes anything. It accepts only released or current
// values, never malformed intermediate formats.
func (db *DB) ValidateDirectoryProjectionUpgrade(ctx context.Context) error {
	return db.walkDirectoryUpgrade(ctx, nil)
}

// UpgradeDirectoryProjection converts released channel rows to the sole current
// format. Callers MUST exclusively own the stopped node and subsequently replace
// its Slot recovery snapshots before allowing startup. Each row and covering
// index commit atomically; an interrupted invocation can be repeated.
func (db *DB) UpgradeDirectoryProjection(ctx context.Context) (uint64, error) {
	if err := db.ValidateDirectoryProjectionUpgrade(ctx); err != nil {
		return 0, err
	}
	var converted uint64
	err := db.walkDirectoryUpgrade(ctx, func(key, index, value []byte) error {
		batch := db.engine.NewBatch()
		defer batch.Close()
		if err := batch.Set(key, value); err != nil {
			return err
		}
		if err := batch.Set(index, value); err != nil {
			return err
		}
		if err := batch.Commit(true); err != nil {
			return err
		}
		db.meta.forgetChannel(key)
		converted++
		return nil
	})
	return converted, err
}

func (db *DB) walkDirectoryUpgrade(ctx context.Context, rewrite func([]byte, []byte, []byte) error) error {
	if db == nil || db.engine == nil {
		return ErrInvalidArgument
	}
	prefix := []byte{byte(keycodec.DomainMeta), byte(keycodec.PartitionHashSlot)}
	span := keycodec.NewPrefixSpan(prefix)
	iter, err := db.engine.NewIter(engine.Span{Start: span.Start, End: span.End}, engine.IterOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		if len(key) < 9 || binary.BigEndian.Uint32(key[5:9]) != TableIDChannel {
			continue
		}
		space := keycodec.Space(key[4])
		offset := 9
		if space == keycodec.SpaceIndex {
			if len(key) < 11 {
				return ErrCorruptValue
			}
			if binary.BigEndian.Uint16(key[9:11]) != channelIDIndexID {
				continue
			}
			offset = 11
		} else if space != keycodec.SpaceRow {
			continue
		}
		parts, rest, err := decodeKeyParts(key[offset:], channelTable.spec.Primary.Layout)
		if err != nil {
			return err
		}
		if space == keycodec.SpaceRow {
			if len(rest) != 2 || binary.BigEndian.Uint16(rest) != channelPrimaryFamilyID {
				return ErrCorruptValue
			}
		} else if len(rest) != 0 {
			return ErrCorruptValue
		}
		hashSlot := binary.BigEndian.Uint16(key[2:4])
		primary := encodeChannelRowKey(hashSlot, parts[0].S, parts[1].I64, channelPrimaryFamilyID)
		index, err := encodeKeyParts(encodeIndexPrefix(hashSlot, TableIDChannel, channelIDIndexID), parts)
		if err != nil {
			return err
		}
		value, err := iter.Value()
		if err != nil {
			return err
		}
		otherKey := index
		if space == keycodec.SpaceIndex {
			otherKey = primary
		}
		other, found, err := db.engine.Get(otherKey)
		if err != nil {
			return err
		}
		converted, err := upgradeReleasedChannelValue(parts[0].S, parts[1].I64, value)
		if err != nil {
			return fmt.Errorf("channel %q/%d: %w", parts[0].S, parts[1].I64, err)
		}
		otherConverted, otherErr := upgradeReleasedChannelValue(parts[0].S, parts[1].I64, other)
		if !found || otherErr != nil || !bytes.Equal(converted, otherConverted) {
			return fmt.Errorf("%w: channel primary/index mismatch", ErrCorruptValue)
		}
		if space == keycodec.SpaceRow && rewrite != nil && (!bytes.Equal(value, converted) || !bytes.Equal(other, converted)) {
			if err := rewrite(primary, index, converted); err != nil {
				return err
			}
		}
	}
	return iter.Error()
}

func upgradeReleasedChannelValue(id string, typ int64, value []byte) ([]byte, error) {
	if len(value) == 64 {
		ready := binary.BigEndian.Uint64(value[56:64])
		if ready > 1 {
			return nil, ErrCorruptValue
		}
		out := append([]byte(nil), value[:56]...)
		state, generation := uint64(DirectoryProjectionNone), uint64(0)
		if ready == 1 {
			// Released runtime metadata has no generation column. The current
			// runtime-meta normalizer assigns that existing incarnation generation 1.
			state, generation = uint64(DirectoryProjectionReady), 1
		}
		value = binary.BigEndian.AppendUint64(out, state)
		value = binary.BigEndian.AppendUint64(value, generation)
	}
	if _, err := decodeChannelValue(id, typ, value); err != nil {
		return nil, err
	}
	return value, nil
}
