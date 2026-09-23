package channel

import (
	"context"
	"errors"
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

func (s *serviceRejoinTestStore) ContainsChannelSubscriber(context.Context, string, int64, string) (bool, error) {
	return s.member, nil
}

func (s *serviceRejoinTestStore) RemoveChannelSubscribersCounted(ctx context.Context, channelID string, channelType int64, uids []string, version ...uint64) (metadb.SubscriberMutationResult, error) {
	result, err := s.recordingStore.RemoveChannelSubscribersCounted(ctx, channelID, channelType, uids, version...)
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
