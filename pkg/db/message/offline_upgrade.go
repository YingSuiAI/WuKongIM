package message

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
	channel "github.com/WuKongIM/WuKongIM/pkg/db/message/channelcompat"
	"github.com/WuKongIM/WuKongIM/pkg/quorumlog"
)

// LegacyProposalAuthority is the stopped single-node cluster's durable Channel
// authority. The caller must resolve it from metadata, never from desired config.
type LegacyProposalAuthority struct {
	ChannelEpoch uint64
	LeaderTerm   uint64
	// FenceVersion is the Channel route generation, not its write-fence version.
	FenceVersion uint64
	Leader       uint64
	Voters       []uint64
}

// LegacyProposalUpgradeStats counts inspected Channels and missing identities.
// During preflight, upgrade counts describe the writes that apply would perform.
type LegacyProposalUpgradeStats struct {
	ChannelsScanned  uint64
	ChannelsUpgraded uint64
	EntriesUpgraded  uint64
}

// UpgradeLegacyProposals seals pre-quorum message rows into the current exact
// log format without changing rows, sequence numbers, checkpoints, or metadata.
// It is exclusively an offline operation: callers must stop every server and
// keep a durable startup fence until the complete upgrade succeeds. Preflight
// (apply=false) performs every validation without writing. Apply is resumable:
// each bounded batch atomically persists paired manifests and entry identities.
func (e *Engine) UpgradeLegacyProposals(ctx context.Context, localNode uint64,
	resolve func(ChannelCatalogEntry) (LegacyProposalAuthority, error), apply bool,
) (LegacyProposalUpgradeStats, error) {
	var stats LegacyProposalUpgradeStats
	if ctx == nil || localNode == 0 || resolve == nil {
		return stats, channel.ErrInvalidArgument
	}
	var cursor ChannelKey
	for {
		catalog, next, more, err := e.ListChannelsPage(ctx, cursor, 128)
		if err != nil {
			return stats, err
		}
		for _, entry := range catalog {
			stats.ChannelsScanned++
			store, err := e.ForChannel(channel.ChannelKey(entry.Key), channel.ChannelID{ID: entry.ID.ID, Type: entry.ID.Type})
			if err != nil {
				return stats, err
			}
			count, upgradeErr := store.upgradeLegacyProposals(ctx, localNode, func() (LegacyProposalAuthority, error) { return resolve(entry) }, apply)
			closeErr := store.Close()
			if upgradeErr != nil {
				return stats, fmt.Errorf("legacy Channel %q: %w", entry.Key, upgradeErr)
			}
			if closeErr != nil {
				return stats, closeErr
			}
			if count > 0 {
				stats.ChannelsUpgraded++
				stats.EntriesUpgraded += count
			}
		}
		if !more {
			return stats, nil
		}
		cursor = next
	}
}

func (s *ChannelStore) upgradeLegacyProposals(ctx context.Context, localNode uint64, resolve func() (LegacyProposalAuthority, error), apply bool) (uint64, error) {
	if err := s.beginUse(); err != nil {
		return 0, err
	}
	defer s.endUse()
	s.log.appendMu.Lock()
	defer s.log.appendMu.Unlock()
	s.log.checkpointMu.Lock()
	defer s.log.checkpointMu.Unlock()
	leo, err := s.log.loadLEOLocked(ctx)
	if err != nil {
		return 0, err
	}
	cp, present, err := s.log.loadCheckpoint(ctx)
	if err != nil || (present && cp.HW > leo) {
		if err == nil {
			err = dberrors.ErrCorruptState
		}
		return 0, err
	}
	if leo == 0 {
		return 0, nil
	}
	// A complete current-format tail needs no conversion. A partial interrupted
	// conversion has no tail manifest and is validated entry by entry below.
	tail, found, err := loadDurableProposalPairByLast(s.log.db.engine, s.log.key, leo)
	if err != nil {
		return 0, err
	}
	if found {
		entry, present, err := loadDurableEntryIdentityFrom(s.log.db.engine, s.log.key, leo)
		if err != nil {
			return 0, err
		}
		if !present || tail.manifest.LastOffset != leo || !durableProposalTailConsistent(tail, entry) {
			return 0, dberrors.ErrCorruptState
		}
		return 0, nil
	}
	authority, err := resolve()
	if err != nil {
		return 0, err
	}
	if authority.ChannelEpoch == 0 || authority.LeaderTerm == 0 || authority.FenceVersion == 0 ||
		authority.Leader != localNode || len(authority.Voters) != 1 || authority.Voters[0] != localNode {
		return 0, fmt.Errorf("requires exact single-node authority: %w", channel.ErrInvalidArgument)
	}
	var previous quorumlog.EntryIdentity
	var upgraded uint64
	for first := uint64(1); first <= leo; {
		rows, err := s.log.readRows(ctx, first, leo, ReadOptions{Limit: 256, MaxBytes: 4 << 20})
		if err != nil {
			return upgraded, err
		}
		if len(rows) == 0 {
			return upgraded, fmt.Errorf("missing message at sequence %d: %w", first, dberrors.ErrCorruptState)
		}
		batch := s.log.db.engine.NewBatch()
		pageCount := uint64(0)
		for _, row := range rows {
			if row.MessageSeq != previous.Index+1 {
				err = fmt.Errorf("non-contiguous legacy sequence %d after %d: %w", row.MessageSeq, previous.Index, dberrors.ErrCorruptState)
				break
			}
			manifest, identity, sealErr := sealLegacyProposal(s.log.key, authority, previous, row)
			if sealErr != nil {
				err = sealErr
				break
			}
			missing, checkErr := s.checkLegacyProposal(manifest, identity)
			if checkErr != nil {
				err = checkErr
				break
			}
			if missing {
				pageCount++
				if apply {
					value := encodeDurableProposalRecord(durableProposalRecord{manifest: manifest})
					for _, item := range []struct{ key, value []byte }{
						{encodeProposalByLastKey(s.log.key, row.MessageSeq), value},
						{encodeProposalByCommandKey(s.log.key, manifest.CommandID), value},
						{encodeEntryIdentityKey(s.log.key, row.MessageSeq), encodeDurableEntryIdentity(identity)},
					} {
						if err = batch.Set(item.key, item.value); err != nil {
							break
						}
					}
					if err != nil {
						break
					}
				}
			}
			previous = identity
		}
		if err == nil && apply && pageCount > 0 {
			err = batch.Commit(true)
		}
		_ = batch.Close()
		if err != nil {
			return upgraded, err
		}
		upgraded += pageCount
		if previous.Index == leo {
			break
		}
		first = previous.Index + 1
	}
	return upgraded, nil
}

func sealLegacyProposal(key ChannelKey, authority LegacyProposalAuthority, previous quorumlog.EntryIdentity, row messageRow) (DurableProposalManifest, quorumlog.EntryIdentity, error) {
	commandBytes := append([]byte("wukongim:offline-legacy-proposal:v1\x00"), []byte(key)...)
	commandBytes = binary.BigEndian.AppendUint64(commandBytes, row.MessageSeq)
	manifest := DurableProposalManifest{
		Version: DurableProposalManifestVersion, ChannelEpoch: authority.ChannelEpoch,
		LeaderTerm: authority.LeaderTerm, FenceVersion: authority.FenceVersion,
		CommandID: sha256.Sum256(commandBytes), BaseOffset: previous.Index, LastOffset: row.MessageSeq,
		PreviousIndex: previous.Index, PreviousTerm: previous.LeaderTerm, PreviousDigest: previous.Digest,
	}
	entries, ok := deriveDurableProposalEntries(manifest, []channel.Record{{Epoch: authority.ChannelEpoch}}, []messageRow{row})
	if !ok {
		return DurableProposalManifest{}, quorumlog.EntryIdentity{}, fmt.Errorf("cannot seal legacy sequence %d: %w", row.MessageSeq, dberrors.ErrCorruptState)
	}
	manifest.Digest = entries[0].Digest
	return manifest, entries[0], nil
}

// checkLegacyProposal permits only an absent triple or the exact triple from
// an interrupted upgrade. Never repair a conflicting or partially lost proof.
func (s *ChannelStore) checkLegacyProposal(manifest DurableProposalManifest, identity quorumlog.EntryIdentity) (bool, error) {
	last, hasLast, err := loadDurableProposalFrom(s.log.db.engine, encodeProposalByLastKey(s.log.key, manifest.LastOffset))
	if err != nil {
		return false, err
	}
	command, hasCommand, err := loadDurableProposalFrom(s.log.db.engine, encodeProposalByCommandKey(s.log.key, manifest.CommandID))
	if err != nil {
		return false, err
	}
	entry, hasEntry, err := loadDurableEntryIdentityFrom(s.log.db.engine, s.log.key, identity.Index)
	if err != nil {
		return false, err
	}
	if !hasLast && !hasCommand && !hasEntry {
		return true, nil
	}
	if !hasLast || !hasCommand || !hasEntry || last.manifest != manifest || command.manifest != manifest || entry != identity {
		return false, dberrors.ErrCorruptState
	}
	return false, nil
}
