package message

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
	channel "github.com/WuKongIM/WuKongIM/pkg/db/message/channelcompat"
	"github.com/WuKongIM/WuKongIM/pkg/quorumlog"
)

func TestReplaceRecoverySuffixRejectsCorruptRetainedProposalTail(t *testing.T) {
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	const channelKey = channel.ChannelKey("recovery-boundary")
	store, err := engine.ForChannel(channelKey, channel.ChannelID{ID: "recovery-boundary", Type: 1})
	if err != nil {
		t.Fatalf("ForChannel(): %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	first, firstTail := recoveryProposalFixture(t, 1, 0, quorumlog.EntryIdentity{})
	second, _ := recoveryProposalFixture(t, 2, 1, firstTail)
	for _, proposal := range []RecoveryProposal{first, second} {
		results := StoreAppendBatch(context.Background(), []AppendBatchItem{{
			Store:              store,
			Records:            proposal.Records,
			ExactBaseOffset:    true,
			ExpectedBaseOffset: proposal.Manifest.BaseOffset,
			Proposal:           proposal.Manifest,
		}})
		if len(results) != 1 || results[0].Err != nil || results[0].Outcome != quorumlog.AppendOutcomeDurable {
			t.Fatalf("StoreAppendBatch(base=%d) = %+v, want durable", proposal.Manifest.BaseOffset, results)
		}
	}
	frontier, err := store.LoadDurableFrontier(context.Background())
	if err != nil {
		t.Fatalf("LoadDurableFrontier(): %v", err)
	}

	corruptTail := firstTail
	corruptTail.Digest[0] ^= 0xff
	batch := engine.engine.NewBatch()
	if err := batch.Set(encodeEntryIdentityKey(ChannelKey(channelKey), 1), encodeDurableEntryIdentity(corruptTail)); err != nil {
		_ = batch.Close()
		t.Fatalf("corrupt retained tail: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		_ = batch.Close()
		t.Fatalf("commit corrupt retained tail: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("close corruption batch: %v", err)
	}

	result, err := store.ReplaceRecoverySuffix(context.Background(), ReplaceRecoverySuffixRequest{
		Expected: frontier, KeepThrough: 1,
	})
	if !errors.Is(err, channel.ErrCorruptState) || result.Outcome != quorumlog.AppendOutcomeConflict {
		t.Fatalf("ReplaceRecoverySuffix() = %+v, %v; want corrupt-state conflict", result, err)
	}
	if _, present, err := store.GetMessageBySeq(2); err != nil || !present {
		t.Fatalf("GetMessageBySeq(2) after rejected replacement = present %v, error %v; want unchanged suffix", present, err)
	}
}

func TestReplaceRecoverySuffixRejectsBrokenReplacementWithoutMutatingSuffix(t *testing.T) {
	for _, test := range []struct {
		name         string
		replacements func(*testing.T, quorumlog.EntryIdentity) []RecoveryProposal
	}{
		{
			name: "forged predecessor",
			replacements: func(t *testing.T, retainedTail quorumlog.EntryIdentity) []RecoveryProposal {
				forgedPrevious := retainedTail
				forgedPrevious.Digest[0] ^= 0xff
				forged, _ := recoveryProposalFixture(t, 3, 1, forgedPrevious)
				return []RecoveryProposal{forged}
			},
		},
		{
			name: "reused command identity",
			replacements: func(t *testing.T, retainedTail quorumlog.EntryIdentity) []RecoveryProposal {
				commandID := quorumlog.CommandID{31: 9}
				first, firstTail := recoveryProposalFixtureWithCommand(t, 3, 1, retainedTail, commandID)
				second, _ := recoveryProposalFixtureWithCommand(t, 4, 2, firstTail, commandID)
				return []RecoveryProposal{first, second}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, store, expected, retainedTail, oldSuffix := recoveryReplacementStore(t)

			result, err := store.ReplaceRecoverySuffix(context.Background(), ReplaceRecoverySuffixRequest{
				Expected: expected, KeepThrough: 1, Proposals: test.replacements(t, retainedTail), Committed: 1,
			})
			if !errors.Is(err, channel.ErrCorruptState) || result.Outcome != quorumlog.AppendOutcomeConflict {
				t.Fatalf("ReplaceRecoverySuffix() = %+v, %v; want corrupt-state conflict", result, err)
			}
			assertRecoverySuffixUnchanged(t, store, expected, oldSuffix)

			// The assertion above uses public compatibility APIs. Also prove the
			// rejected call did not remove either durable index for the old suffix.
			persisted, present, err := loadDurableProposalPairByLast(engine.engine, ChannelKey("recovery-boundary"), oldSuffix.Manifest.LastOffset)
			if err != nil || !present || persisted.manifest != oldSuffix.Manifest {
				t.Fatalf("old durable proposal pair = %+v, present %v, error %v", persisted, present, err)
			}
		})
	}
}

func TestReplaceRecoverySuffixAcceptsRetainedBoundaryAndMultipleProposals(t *testing.T) {
	_, store, expected, retainedTail, oldSuffix := recoveryReplacementStore(t)
	first, firstTail := recoveryProposalFixture(t, 3, 1, retainedTail)
	second, _ := recoveryProposalFixture(t, 4, 2, firstTail)

	result, err := store.ReplaceRecoverySuffix(context.Background(), ReplaceRecoverySuffixRequest{
		Expected: expected, KeepThrough: 1,
		Proposals: []RecoveryProposal{first, second}, Committed: 3,
	})
	if err != nil || result.Outcome != quorumlog.AppendOutcomeDurable || result.LastOffset != 3 {
		t.Fatalf("ReplaceRecoverySuffix() = %+v, %v; want durable offset 3", result, err)
	}
	for seq, messageID := range map[uint64]uint64{1: 8001, 2: 8003, 3: 8004} {
		message, present, err := store.GetMessageBySeq(seq)
		if err != nil || !present || message.MessageID != messageID {
			t.Fatalf("GetMessageBySeq(%d) = %+v, present %v, error %v; want message id %d", seq, message, present, err, messageID)
		}
	}
	if _, present, err := store.GetMessageByMessageID(oldSuffix.Records[0].ID); err != nil || present {
		t.Fatalf("old suffix message lookup = present %v, error %v; want removed", present, err)
	}
	frontier, err := store.LoadDurableFrontier(context.Background())
	if err != nil || frontier.LEO != 3 || frontier.Committed != 3 || frontier.Manifest != second.Manifest {
		t.Fatalf("LoadDurableFrontier() = %+v, %v; want second replacement proposal", frontier, err)
	}
}

func TestRecoverySuffixChainRejectsBrokenTopology(t *testing.T) {
	first, firstTail := recoveryProposalFixture(t, 21, 0, quorumlog.EntryIdentity{})
	forgedPrevious := firstTail
	forgedPrevious.Digest[0] ^= 0xff
	forged, _ := recoveryProposalFixture(t, 23, 1, forgedPrevious)
	reusedCommand, _ := recoveryProposalFixtureWithCommand(t, 24, 1, firstTail, first.Manifest.CommandID)

	for _, test := range []struct {
		name      string
		candidate RecoveryProposal
	}{
		{name: "forged predecessor", candidate: forged},
		{name: "reused command identity", candidate: reusedCommand},
	} {
		t.Run(test.name, func(t *testing.T) {
			chain, err := newRecoverySuffixChain(0, durableProposalRecord{}, quorumlog.EntryIdentity{}, 2)
			if err != nil {
				t.Fatalf("newRecoverySuffixChain(): %v", err)
			}
			if err := chain.admit(first.Manifest, len(first.Records)); err != nil {
				t.Fatalf("admit(first): %v", err)
			}
			if err := chain.admit(test.candidate.Manifest, len(test.candidate.Records)); !errors.Is(err, dberrors.ErrConflict) {
				t.Fatalf("admit(second) error = %v, want conflict", err)
			}
		})
	}
}

func recoveryReplacementStore(t *testing.T) (*Engine, *ChannelStore, DurableFrontier, quorumlog.EntryIdentity, RecoveryProposal) {
	t.Helper()
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	store, err := engine.ForChannel("recovery-boundary", channel.ChannelID{ID: "recovery-boundary", Type: 1})
	if err != nil {
		t.Fatalf("ForChannel(): %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	retained, retainedTail := recoveryProposalFixture(t, 1, 0, quorumlog.EntryIdentity{})
	oldSuffix, _ := recoveryProposalFixture(t, 2, 1, retainedTail)
	for _, proposal := range []RecoveryProposal{retained, oldSuffix} {
		results := StoreAppendBatch(context.Background(), []AppendBatchItem{{
			Store: store, Records: proposal.Records, ExactBaseOffset: true,
			ExpectedBaseOffset: proposal.Manifest.BaseOffset, Proposal: proposal.Manifest,
		}})
		if len(results) != 1 || results[0].Err != nil || results[0].Outcome != quorumlog.AppendOutcomeDurable {
			t.Fatalf("StoreAppendBatch(base=%d) = %+v, want durable", proposal.Manifest.BaseOffset, results)
		}
	}
	expected, err := store.LoadDurableFrontier(context.Background())
	if err != nil {
		t.Fatalf("LoadDurableFrontier(): %v", err)
	}
	return engine, store, expected, retainedTail, oldSuffix
}

func assertRecoverySuffixUnchanged(t *testing.T, store *ChannelStore, expected DurableFrontier, oldSuffix RecoveryProposal) {
	t.Helper()
	message, present, err := store.GetMessageBySeq(oldSuffix.Manifest.LastOffset)
	if err != nil || !present || message.MessageID != oldSuffix.Records[0].ID {
		t.Fatalf("old suffix message = %+v, present %v, error %v", message, present, err)
	}
	frontier, err := store.LoadDurableFrontier(context.Background())
	if err != nil || frontier != expected {
		t.Fatalf("frontier after rejection = %+v, %v; want unchanged %+v", frontier, err, expected)
	}
}

func recoveryProposalFixture(t *testing.T, marker byte, base uint64, previous quorumlog.EntryIdentity) (RecoveryProposal, quorumlog.EntryIdentity) {
	t.Helper()
	return recoveryProposalFixtureWithCommand(t, marker, base, previous, quorumlog.CommandID{31: marker})
}

func recoveryProposalFixtureWithCommand(t *testing.T, marker byte, base uint64, previous quorumlog.EntryIdentity, commandID quorumlog.CommandID) (RecoveryProposal, quorumlog.EntryIdentity) {
	t.Helper()
	row := messageRow{
		MessageID: uint64(8_000) + uint64(marker), ClientMsgNo: fmt.Sprintf("recovery-%d", marker),
		ChannelID: "recovery-boundary", ChannelType: 1, FromUID: "sender",
		ServerTimestampMS: 1_700_000_000_000 + int64(marker), Payload: []byte{marker},
	}
	record, err := compatibilityRecordFromRow(row)
	if err != nil {
		t.Fatalf("compatibilityRecordFromRow(): %v", err)
	}
	record.Epoch = 3
	manifest := DurableProposalManifest{
		Version: DurableProposalManifestVersion, ChannelEpoch: 3, LeaderTerm: 5, FenceVersion: 7,
		CommandID: commandID, BaseOffset: base, LastOffset: base + 1,
		PreviousTerm: previous.LeaderTerm, PreviousIndex: base, PreviousDigest: previous.Digest,
	}
	rows, err := compatibilityRowsFromRecords(base+1, []channel.Record{record})
	if err != nil {
		t.Fatalf("compatibilityRowsFromRecords(): %v", err)
	}
	entries, ok := deriveDurableProposalEntries(manifest, []channel.Record{record}, rows)
	if !ok || len(entries) != 1 {
		t.Fatal("deriveDurableProposalEntries() failed")
	}
	manifest.Digest = entries[0].Digest
	return RecoveryProposal{Manifest: manifest, Records: []channel.Record{record}}, entries[0]
}
