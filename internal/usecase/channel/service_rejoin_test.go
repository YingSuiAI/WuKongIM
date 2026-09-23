package channel

import (
	"context"
	"errors"
	"fmt"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

type serviceRejoinTestStore struct {
	*recordingStore
	member bool
}

func (s *serviceRejoinTestStore) AddChannelSubscribersCounted(ctx context.Context, channelID string, channelType int64, uids []string, version ...uint64) (metadb.SubscriberMutationResult, error) {
	result, err := s.recordingStore.AddChannelSubscribersCounted(ctx, channelID, channelType, uids, version...)
	if err == nil {
		s.member = true
	}
	return result, err
}

func (s *serviceRejoinTestStore) AddChannelSubscribersForRejoinCounted(ctx context.Context, channelID string, channelType int64, uids []string, version uint64) (metadb.SubscriberMutationResult, error) {
	return s.AddChannelSubscribersCounted(ctx, channelID, channelType, uids, version)
}

func (s *serviceRejoinTestStore) ContainsChannelSubscriber(context.Context, string, int64, string) (bool, error) {
	return s.member, nil
}

func (s *serviceRejoinTestStore) SubscriberGeneration(context.Context, string, int64, string) (uint64, bool, error) {
	if !s.member {
		return 0, false, nil
	}
	if n := len(s.addSubscribers); n > 0 {
		return s.addSubscribers[n-1].version, true, nil
	}
	return s.channels[recordingChannelKey("g1", 2)].SubscriberMutationVersion, true, nil
}

func (s *serviceRejoinTestStore) RemoveChannelSubscribersCounted(ctx context.Context, channelID string, channelType int64, uids []string, version ...uint64) (metadb.SubscriberMutationResult, error) {
	result, err := s.recordingStore.RemoveChannelSubscribersCounted(ctx, channelID, channelType, uids, version...)
	if err == nil {
		s.member = false
	}
	return result, err
}

func (s *serviceRejoinTestStore) RemoveChannelSubscribersIfVersion(ctx context.Context, channelID string, channelType int64, uids []string, expectedVersion uint64) (metadb.SubscriberMutationResult, error) {
	result, err := s.recordingStore.RemoveChannelSubscribersIfVersion(ctx, channelID, channelType, uids, expectedVersion)
	if err == nil {
		s.member = false
	}
	return result, err
}

type serviceRejoinTestIndex struct {
	*recordingMembershipIndex
	row             metadb.UserChannelMembership
	calls           int
	fail            error
	failAfterCommit bool
}

type protectedOrdinaryTestIndex struct {
	*recordingMembershipIndex
	row     metadb.UserChannelMembership
	missing bool
	rows    map[string]metadb.UserChannelMembership
	readErr error
}

func (i *protectedOrdinaryTestIndex) UpsertChannelMemberships(context.Context, string, int64, []string, uint64, uint64, int64) error {
	return metadb.ErrPlatformMembershipProtected
}

func (i *protectedOrdinaryTestIndex) GetUserChannelMembership(_ context.Context, uid string, _ string, _ int64) (metadb.UserChannelMembership, bool, error) {
	if i.readErr != nil {
		return metadb.UserChannelMembership{}, false, i.readErr
	}
	if i.rows != nil {
		row, found := i.rows[uid]
		return row, found, nil
	}
	return i.row, !i.missing, nil
}

func TestOrdinaryAddCompensatesProtectedPlatformTombstone(t *testing.T) {
	store := &recordingStore{channels: map[string]metadb.Channel{recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8}}}
	index := &protectedOrdinaryTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, PlatformMembershipEpoch: 3, JoinSeq: 51, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	err := app.AddSubscribers(context.Background(), SubscriberCommand{ChannelID: "g1", ChannelType: 2, Subscribers: []string{"u1"}})
	if !errors.Is(err, metadb.ErrPlatformMembershipProtected) || len(store.addSubscribers) != 1 || len(store.removeSubscribers) != 1 || store.removeSubscribers[0].uids[0] != "u1" {
		t.Fatalf("ordinary protected add err=%v adds=%+v removes=%+v", err, store.addSubscribers, store.removeSubscribers)
	}
}

func TestOrdinaryAddCompensatesAnyUnprojectedUID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing bool
		row     metadb.UserChannelMembership
	}{
		{name: "absent", missing: true},
		{name: "epoch_zero_tombstone", row: metadb.UserChannelMembership{Tombstone: true, SourceVersion: 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingStore{channels: map[string]metadb.Channel{recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8}}}
			index := &protectedOrdinaryTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: tc.row, missing: tc.missing}
			app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
			if err := app.AddSubscribers(context.Background(), SubscriberCommand{ChannelID: "g1", ChannelType: 2, Subscribers: []string{"u1"}}); err == nil {
				t.Fatal("failed UID projection returned success")
			}
			if len(store.removeSubscribers) != 1 || store.removeSubscribers[0].version <= store.addSubscribers[0].version {
				t.Fatalf("unprojected UID not compensated: add=%+v remove=%+v", store.addSubscribers, store.removeSubscribers)
			}
		})
	}
}

func TestOrdinaryAddMixedChunkPreservesExistingLiveMember(t *testing.T) {
	store := &recordingStore{channels: map[string]metadb.Channel{recordingChannelKey("g1", 2): {
		ChannelID: "g1", ChannelType: 2, SubscriberCount: 1, SubscriberMutationVersion: 8,
	}}}
	index := &protectedOrdinaryTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, rows: map[string]metadb.UserChannelMembership{
		"existing":  {UID: "existing", ChannelID: "g1", ChannelType: 2, SourceVersion: 8},
		"protected": {UID: "protected", ChannelID: "g1", ChannelType: 2, Tombstone: true, PlatformMembershipEpoch: 3, SourceVersion: 8},
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	if err := app.AddSubscribers(context.Background(), SubscriberCommand{ChannelID: "g1", ChannelType: 2, Subscribers: []string{"existing", "protected"}}); err == nil {
		t.Fatal("protected UID returned success")
	}
	if len(store.removeSubscribers) != 1 || !equalStrings(store.removeSubscribers[0].uids, []string{"protected"}) {
		t.Fatalf("existing live UID was removed: %+v", store.removeSubscribers)
	}
}

func TestOrdinaryAddRemovesRogueProtectedSubscriberGeneration(t *testing.T) {
	for _, oldGeneration := range []uint64{0, 5} {
		t.Run(fmt.Sprintf("generation_%d", oldGeneration), func(t *testing.T) {
			store := &recordingStore{channels: map[string]metadb.Channel{recordingChannelKey("g1", 2): {
				ChannelID: "g1", ChannelType: 2, SubscriberCount: 1, SubscriberMutationVersion: 8,
			}}, subscriberGenerations: map[string]uint64{"u1": oldGeneration}}
			index := &protectedOrdinaryTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
				UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, PlatformMembershipEpoch: 3, SourceVersion: 8,
			}}
			app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
			if err := app.AddSubscribers(context.Background(), SubscriberCommand{ChannelID: "g1", ChannelType: 2, Subscribers: []string{"u1"}}); !errors.Is(err, metadb.ErrPlatformMembershipProtected) {
				t.Fatalf("protected Add error=%v", err)
			}
			if len(store.removeSubscribers) != 1 || !equalStrings(store.removeSubscribers[0].uids, []string{"u1"}) {
				t.Fatalf("rogue generation %d remained: removes=%+v", oldGeneration, store.removeSubscribers)
			}
		})
	}
}

type interleavedRejoinGenerationStore struct {
	*recordingStore
	once bool
}

func (s *interleavedRejoinGenerationStore) SubscriberGeneration(ctx context.Context, channelID string, channelType int64, uid string) (uint64, bool, error) {
	generation, found, err := s.recordingStore.SubscriberGeneration(ctx, channelID, channelType, uid)
	if !s.once && err == nil && found {
		s.once = true
		s.subscriberGenerations[uid] = 10
		channel := s.channels[recordingChannelKey(channelID, channelType)]
		channel.SubscriberMutationVersion = 10
		s.channels[recordingChannelKey(channelID, channelType)] = channel
	}
	return generation, found, err
}

func TestOldOrdinaryCompensationCannotRemoveInterleavedStrictRejoin(t *testing.T) {
	store := &interleavedRejoinGenerationStore{recordingStore: &recordingStore{
		channels:              map[string]metadb.Channel{recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberCount: 1, SubscriberMutationVersion: 8}},
		subscriberGenerations: map[string]uint64{"u1": 5},
	}}
	index := &protectedOrdinaryTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, PlatformMembershipEpoch: 3, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	if err := app.AddSubscribers(context.Background(), SubscriberCommand{ChannelID: "g1", ChannelType: 2, Subscribers: []string{"u1"}}); err == nil {
		t.Fatal("old failed Add returned success")
	}
	if len(store.removeSubscribers) != 0 || store.subscriberGenerations["u1"] != 10 {
		t.Fatalf("old compensation revoked new rejoin: removes=%+v generation=%d", store.removeSubscribers, store.subscriberGenerations["u1"])
	}
}

func TestOrdinaryCompensationReadFailureKeepsExistingGeneration(t *testing.T) {
	store := &recordingStore{channels: map[string]metadb.Channel{recordingChannelKey("g1", 2): {
		ChannelID: "g1", ChannelType: 2, SubscriberCount: 1, SubscriberMutationVersion: 8,
	}}, subscriberGenerations: map[string]uint64{"existing": 5}}
	readFailure := errors.New("injected UID readback timeout")
	index := &protectedOrdinaryTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, readErr: readFailure}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	err := app.AddSubscribers(context.Background(), SubscriberCommand{ChannelID: "g1", ChannelType: 2, Subscribers: []string{"existing", "new"}})
	if !errors.Is(err, readFailure) {
		t.Fatalf("readback error=%v", err)
	}
	if len(store.removeSubscribers) != 1 || !equalStrings(store.removeSubscribers[0].uids, []string{"new"}) {
		t.Fatalf("readback fallback revoked existing member: %+v", store.removeSubscribers)
	}
}

func (s *serviceRejoinTestIndex) GetUserChannelMembership(context.Context, string, string, int64) (metadb.UserChannelMembership, bool, error) {
	return s.row, true, nil
}

func (s *serviceRejoinTestIndex) RejoinUserChannelMembership(_ context.Context, rejoin metadb.PlatformMembershipRejoin) error {
	s.calls++
	if s.fail != nil && !s.failAfterCommit {
		return s.fail
	}
	s.row.Tombstone = false
	s.row.PlatformMembershipEpoch = rejoin.MembershipEpoch
	s.row.JoinSeq = rejoin.JoinedSeq + 1
	if s.row.DeletedToSeq < rejoin.JoinedSeq {
		s.row.DeletedToSeq = rejoin.JoinedSeq
	}
	s.row.SourceVersion = rejoin.SourceVersion
	if s.fail != nil {
		return s.fail
	}
	return nil
}

func TestServiceRejoinUsesTrustedJoinedBoundaryAndExactReadback(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8},
	}}}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2,
		Tombstone: true, PlatformMembershipEpoch: 2, JoinSeq: 11, DeletedToSeq: 90, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50}
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || state.MembershipEpoch != 3 || state.JoinSeq != 51 || state.DeletedToSeq != 90 || state.SourceVersion != 9 {
		t.Fatalf("rejoin state=%+v err=%v", state, err)
	}
	if index.calls != 1 || len(store.addSubscribers) != 1 {
		t.Fatalf("rejoin calls index=%d channel=%d", index.calls, len(store.addSubscribers))
	}
	state, err = app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || index.calls != 1 || len(store.addSubscribers) != 1 {
		t.Fatalf("idempotent replay state=%+v err=%v index=%d channel=%d", state, err, index.calls, len(store.addSubscribers))
	}
	cmd.JoinedMessageSeq = 49
	if _, err := app.RejoinSubscriber(context.Background(), cmd); !errors.Is(err, ErrServiceRejoinConflict) {
		t.Fatalf("same-epoch different floor error=%v", err)
	}
}

func TestServiceRejoinCorrectsPrematureOrdinaryAddAndChecksBoundary(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2},
	}}}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, SourceVersion: 1,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50}
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || state.MembershipEpoch != 3 || state.JoinSeq != 51 {
		t.Fatalf("live old epoch correction state=%+v err=%v", state, err)
	}
	index.row.Tombstone = true
	cmd.PreviousRemovedMessageSeq = 51
	if _, err := app.RejoinSubscriber(context.Background(), cmd); !errors.Is(err, ErrServiceRejoinInvalid) {
		t.Fatalf("reversed boundary error=%v", err)
	}
	cmd.PreviousRemovedMessageSeq = 30
	cmd.JoinedMessageSeq = 101
	if _, err := app.RejoinSubscriber(context.Background(), cmd); !errors.Is(err, ErrServiceRejoinConflict) {
		t.Fatalf("future joined sequence error=%v", err)
	}
	if len(store.addSubscribers) != 1 || index.calls != 1 {
		t.Fatalf("invalid rejoin mutated channel=%d index=%d", len(store.addSubscribers), index.calls)
	}
}

func TestServiceRejoinCompensatesFailedUIDProjection(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8},
	}}}
	projectionFailure := errors.New("injected UID Slot failure")
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, fail: projectionFailure, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, JoinSeq: 11, DeletedToSeq: 30, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50}
	if _, err := app.RejoinSubscriber(context.Background(), cmd); !errors.Is(err, projectionFailure) {
		t.Fatalf("failed UID projection error=%v", err)
	}
	if store.member || len(store.addSubscribers) != 1 || len(store.removeSubscribers) != 1 || !index.row.Tombstone {
		t.Fatalf("compensation member=%t adds=%d removes=%d row=%+v", store.member, len(store.addSubscribers), len(store.removeSubscribers), index.row)
	}
	index.fail = nil
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || state.JoinSeq != 51 || !store.member {
		t.Fatalf("retry state=%+v err=%v member=%t", state, err, store.member)
	}
}

func TestServiceRejoinDoesNotAcceptOlderUIDProjectionAfterChannelReadd(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{
		channels: map[string]metadb.Channel{
			recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 12},
		},
		addSubscribers: []subscriberCall{{channelID: "g1", channelType: 2, uids: []string{"u1"}, version: 12}},
	}, member: true}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, PlatformMembershipEpoch: 3,
		JoinSeq: 51, DeletedToSeq: 50, SourceVersion: 10,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3,
		PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50, RepairSameEpoch: true}
	check, err := app.CheckRejoinSubscriber(context.Background(), cmd)
	if err != nil || check.Ready {
		t.Fatalf("old UID projection was accepted: state=%+v err=%v", check, err)
	}
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || state.SourceVersion < 13 || index.calls != 1 {
		t.Fatalf("repair did not fence old UID projection: state=%+v calls=%d err=%v", state, index.calls, err)
	}
}

func TestServiceRejoinCommittedUIDWithLostAckUsesReadback(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8},
	}}}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, fail: errors.New("lost UID reply"), failAfterCommit: true, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, JoinSeq: 11, DeletedToSeq: 30, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50, RepairSameEpoch: true}
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || !store.member || len(store.removeSubscribers) != 0 {
		t.Fatalf("lost acknowledgement state=%+v err=%v member=%t removes=%d", state, err, store.member, len(store.removeSubscribers))
	}
}

func TestServiceRejoinRequiresExplicitSameEpochRepair(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8},
	}}}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, PlatformMembershipEpoch: 3,
		JoinSeq: 51, DeletedToSeq: 90, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50}
	if _, err := app.RejoinSubscriber(context.Background(), cmd); !errors.Is(err, ErrServiceRejoinConflict) {
		t.Fatalf("unmarked same epoch error=%v", err)
	}
	cmd.RepairSameEpoch = true
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || state.JoinSeq != 51 || state.DeletedToSeq != 90 {
		t.Fatalf("same epoch repair state=%+v err=%v", state, err)
	}
}

func TestServiceRejoinRepairsMissingChannelMemberWithLiveUIDEpoch(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberMutationVersion: 8},
	}}}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, PlatformMembershipEpoch: 3,
		JoinSeq: 51, DeletedToSeq: 90, SourceVersion: 8,
	}}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 30, JoinedMessageSeq: 50, RepairSameEpoch: true}
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready || state.JoinSeq != 51 || state.DeletedToSeq != 90 || !store.member {
		t.Fatalf("live UID repair state=%+v err=%v member=%t", state, err, store.member)
	}
}

func TestServiceRejoinRefreshesLargeGroupFlagAndMutationObserver(t *testing.T) {
	store := &serviceRejoinTestStore{recordingStore: &recordingStore{channels: map[string]metadb.Channel{
		recordingChannelKey("g1", 2): {ChannelID: "g1", ChannelType: 2, SubscriberCount: 1, SubscriberMutationVersion: 8},
	}}}
	index := &serviceRejoinTestIndex{recordingMembershipIndex: &recordingMembershipIndex{}, row: metadb.UserChannelMembership{
		UID: "u1", ChannelID: "g1", ChannelType: 2, Tombstone: true, SourceVersion: 8,
	}}
	observer := &recordingSubscriberMutationObserver{}
	app := New(Options{Store: store, MembershipIndex: index, CommittedTail: &recordingCommittedTailReader{tail: 100}, LargeGroupSubscriberThreshold: 1, SubscriberMutationObserver: observer})
	cmd := ServiceRejoinCommand{ChannelID: "g1", ChannelType: 2, UID: "u1", MembershipEpoch: 3, PreviousRemovedMessageSeq: 0, JoinedMessageSeq: 0}
	state, err := app.RejoinSubscriber(context.Background(), cmd)
	if err != nil || !state.Ready {
		t.Fatalf("rejoin state=%+v err=%v", state, err)
	}
	channel := store.channels[recordingChannelKey("g1", 2)]
	if channel.Large != 1 || channel.SubscriberCount != 2 || len(observer.events) != 1 || !observer.events[0].Large || len(observer.events[0].AddedUIDs) != 1 || observer.events[0].AddedUIDs[0] != "u1" {
		t.Fatalf("large group channel=%+v events=%+v", channel, observer.events)
	}
}
