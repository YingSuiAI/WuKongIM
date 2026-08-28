//go:build integration

package message

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
	channel "github.com/WuKongIM/WuKongIM/pkg/db/message/channelcompat"
	"github.com/WuKongIM/WuKongIM/pkg/quorumlog"
)

func legacyUpgradeAuthority(ChannelCatalogEntry) (LegacyProposalAuthority, error) {
	return LegacyProposalAuthority{ChannelEpoch: 1, LeaderTerm: 2, FenceVersion: 3, Leader: 1, Voters: []uint64{1}}, nil
}

func TestUpgradeLegacyProposalsPreservesRowsAndSequenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := channel.ChannelKey("1:offline-upgrade")
	id := channel.ChannelID{ID: "offline-upgrade", Type: 1}
	store := mustForChannel(t, db, key, id)
	records := make([]channel.Record, 513)
	for i := range records {
		records[i] = compatTestRecord(t, uint64(i+100), id.ID, fmt.Sprintf("old-%d", i))
	}
	if _, err := store.Append(records); err != nil {
		t.Fatal(err)
	}
	cp := channel.Checkpoint{Epoch: 1, HW: 510}
	if err := store.StoreCheckpoint(cp); err != nil {
		t.Fatal(err)
	}
	before, err := store.Read(0, 600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadDurableFrontier(context.Background()); !errors.Is(err, channel.ErrCorruptState) {
		t.Fatalf("old frontier must reproduce corrupt state, got %v", err)
	}
	preflight, err := db.UpgradeLegacyProposals(context.Background(), 1, legacyUpgradeAuthority, false)
	if err != nil || preflight.EntriesUpgraded != 513 {
		t.Fatalf("preflight=%+v err=%v", preflight, err)
	}
	if _, err := store.LoadDurableFrontier(context.Background()); !errors.Is(err, channel.ErrCorruptState) {
		t.Fatalf("preflight mutated frontier: %v", err)
	}
	stats, err := db.UpgradeLegacyProposals(context.Background(), 1, legacyUpgradeAuthority, true)
	if err != nil || stats != preflight {
		t.Fatalf("apply=%+v err=%v", stats, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = mustForChannel(t, db, key, id)
	defer store.Close()
	after, err := store.Read(0, 600)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rows changed across upgrade: %v", err)
	}
	gotCP, err := store.LoadCheckpoint()
	if err != nil || gotCP != cp {
		t.Fatalf("checkpoint=%+v err=%v", gotCP, err)
	}
	frontier, err := store.LoadDurableRecovery(context.Background(), []uint64{1, 256, 257, 513})
	if err != nil || frontier.LEO != 513 || frontier.Committed != 510 {
		t.Fatalf("frontier=%+v err=%v", frontier, err)
	}
	newRecord := compatExactTestRecord(t, 1, 1000, id.ID, "new-after-upgrade")
	manifest := sealCompatProposalManifest(t, DurableProposalManifest{
		Version: DurableProposalManifestVersion, ChannelEpoch: 1, LeaderTerm: 2, FenceVersion: 3,
		CommandID: quorumlog.CommandID{1}, BaseOffset: 513, LastOffset: 514,
		PreviousIndex: 513, PreviousTerm: frontier.TailIdentity.LeaderTerm, PreviousDigest: frontier.TailIdentity.Digest,
	}, []channel.Record{newRecord})
	result := StoreAppendBatch(context.Background(), []AppendBatchItem{{Store: store, Records: []channel.Record{newRecord}, ExactBaseOffset: true, ExpectedBaseOffset: 513, Proposal: manifest}})
	if len(result) != 1 || result[0].Err != nil {
		t.Fatalf("new append=%+v", result)
	}
	if leo, err := store.LEOWithError(); err != nil || leo != 514 {
		t.Fatalf("leo=%d err=%v", leo, err)
	}
	stats, err = db.UpgradeLegacyProposals(context.Background(), 1, func(ChannelCatalogEntry) (LegacyProposalAuthority, error) {
		t.Fatal("current-format Channel must not resolve legacy authority")
		return LegacyProposalAuthority{}, nil
	}, true)
	if err != nil || stats.EntriesUpgraded != 0 {
		t.Fatalf("repeat=%+v err=%v", stats, err)
	}
}

func TestUpgradeLegacyProposalsRejectsUnprovenOrDamagedHistory(t *testing.T) {
	for _, scenario := range []string{"remote-leader", "multiple-voters", "missing-authority", "sequence-gap", "orphan-proof", "changed-proof"} {
		t.Run(scenario, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := mustForChannel(t, db, "1:reject-upgrade", channel.ChannelID{ID: "reject-upgrade", Type: 1})
			defer store.Close()
			if _, err := store.Append([]channel.Record{compatTestRecord(t, 1, "reject-upgrade", "old1"), compatTestRecord(t, 2, "reject-upgrade", "old2")}); err != nil {
				t.Fatal(err)
			}
			resolve := legacyUpgradeAuthority
			switch scenario {
			case "remote-leader", "multiple-voters", "missing-authority":
				resolve = func(c ChannelCatalogEntry) (LegacyProposalAuthority, error) {
					a, _ := legacyUpgradeAuthority(c)
					if scenario == "remote-leader" {
						a.Leader = 2
					}
					if scenario == "multiple-voters" {
						a.Voters = []uint64{1, 2, 3}
					}
					if scenario == "missing-authority" {
						return a, errors.New("missing authority")
					}
					return a, nil
				}
			case "sequence-gap":
				row, _, _ := store.log.getRowBySeq(context.Background(), 1)
				batch := store.log.db.engine.NewBatch()
				if err := store.log.stageDeleteMessage(batch, messageFromRow(row)); err != nil {
					t.Fatal(err)
				}
				if err := batch.Commit(true); err != nil {
					t.Fatal(err)
				}
				_ = batch.Close()
			case "orphan-proof", "changed-proof":
				stageLegacyTestPrefix(t, store, scenario == "orphan-proof")
				if scenario == "changed-proof" {
					resolve = func(c ChannelCatalogEntry) (LegacyProposalAuthority, error) {
						a, _ := legacyUpgradeAuthority(c)
						a.LeaderTerm++
						return a, nil
					}
				}
			}
			if _, err := db.UpgradeLegacyProposals(context.Background(), 1, resolve, false); err == nil {
				t.Fatal("preflight accepted invalid legacy log")
			}
			if _, present, err := loadDurableProposalPairByLast(store.log.db.engine, store.log.key, 2); err != nil || present {
				t.Fatalf("preflight wrote tail: present=%v err=%v", present, err)
			}
		})
	}
}

func TestUpgradeLegacyProposalsResumesExactPrefixAndKeepsMissingCheckpoint(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := mustForChannel(t, db, "1:resume-upgrade", channel.ChannelID{ID: "resume-upgrade", Type: 1})
	defer store.Close()
	if _, err := store.Append([]channel.Record{compatTestRecord(t, 1, "resume-upgrade", "old1"), compatTestRecord(t, 2, "resume-upgrade", "old2")}); err != nil {
		t.Fatal(err)
	}
	stageLegacyTestPrefix(t, store, false)
	stats, err := db.UpgradeLegacyProposals(context.Background(), 1, legacyUpgradeAuthority, false)
	if err != nil || stats.EntriesUpgraded != 1 {
		t.Fatalf("preflight=%+v err=%v", stats, err)
	}
	stats, err = db.UpgradeLegacyProposals(context.Background(), 1, legacyUpgradeAuthority, true)
	if err != nil || stats.EntriesUpgraded != 1 {
		t.Fatalf("resume=%+v err=%v", stats, err)
	}
	if _, err := store.LoadCheckpoint(); !errors.Is(err, channel.ErrEmptyState) {
		t.Fatalf("upgrade invented checkpoint: %v", err)
	}
	frontier, err := store.LoadDurableFrontier(context.Background())
	if err != nil || frontier.LEO != 2 || frontier.Committed != 0 {
		t.Fatalf("frontier=%+v err=%v", frontier, err)
	}
}

func stageLegacyTestPrefix(t *testing.T, store *ChannelStore, orphan bool) {
	t.Helper()
	a, _ := legacyUpgradeAuthority(ChannelCatalogEntry{})
	row, _, err := store.log.getRowBySeq(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	manifest, identity, err := sealLegacyProposal(store.log.key, a, quorumlog.EntryIdentity{}, row)
	if err != nil {
		t.Fatal(err)
	}
	batch := store.log.db.engine.NewBatch()
	defer batch.Close()
	if err := batch.Set(encodeEntryIdentityKey(store.log.key, 1), encodeDurableEntryIdentity(identity)); err != nil {
		t.Fatal(err)
	}
	if !orphan {
		value := encodeDurableProposalRecord(durableProposalRecord{manifest: manifest})
		if err := batch.Set(encodeProposalByLastKey(store.log.key, 1), value); err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(encodeProposalByCommandKey(store.log.key, manifest.CommandID), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeLegacyProposalsRejectsCheckpointBeyondTail(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := mustForChannel(t, db, "1:bad-checkpoint", channel.ChannelID{ID: "bad-checkpoint", Type: 1})
	defer store.Close()
	if _, err := store.Append([]channel.Record{compatTestRecord(t, 1, "bad-checkpoint", "old1")}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreCheckpoint(channel.Checkpoint{HW: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpgradeLegacyProposals(context.Background(), 1, legacyUpgradeAuthority, false); !errors.Is(err, dberrors.ErrCorruptState) {
		t.Fatalf("err=%v", err)
	}
}
