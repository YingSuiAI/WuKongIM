package message

import (
	"context"
	"errors"
	"reflect"
	"testing"

	channelmembers "github.com/WuKongIM/WuKongIM/internal/contracts/channelmembers"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

type subscriberCheckMemberships struct {
	rows map[string]metadb.UserChannelMembership
}

func (s subscriberCheckMemberships) GetUserChannelMembership(_ context.Context, uid, _ string, _ int64) (metadb.UserChannelMembership, bool, error) {
	row, ok := s.rows[uid]
	return row, ok, nil
}

func TestCheckChannelSubscribersUsesHistoryMembershipFacts(t *testing.T) {
	authority := &recordingLiveMembershipAuthority{results: []channelmembers.LiveMembershipAuthorityResult{
		{ChannelFound: true, Subscriber: true, SubscriberMutationVersion: 12},
		{ChannelFound: true, Subscriber: false, SubscriberMutationVersion: 11},
		{ChannelFound: true, Subscriber: false, SubscriberMutationVersion: 13},
	}}
	app := New(Options{Memberships: subscriberCheckMemberships{rows: map[string]metadb.UserChannelMembership{
		"ready": {SourceVersion: 12}, "revoked": {SourceVersion: 12}, "removed": {Tombstone: true, SourceVersion: 11},
	}}, MembershipAuthority: authority})
	ready, missing, err := app.CheckChannelSubscribers(context.Background(), "group", 2, []string{"ready", "removed", "revoked"})
	if !errors.Is(err, ErrSubscriberMembershipSplit) || len(ready) != 0 || len(missing) != 0 {
		t.Fatalf("ready=%#v missing=%#v err=%v", ready, missing, err)
	}
	if len(authority.candidates) != 1 || len(authority.candidates[0]) != 3 {
		t.Fatalf("authority candidates=%#v, want all UIDs", authority.candidates)
	}
}

func TestCheckChannelSubscribersRequiresBothFactsForRemoval(t *testing.T) {
	rows := subscriberCheckMemberships{rows: map[string]metadb.UserChannelMembership{"u1": {UID: "u1", Tombstone: false, SourceVersion: 4}}}
	authority := &recordingLiveMembershipAuthority{results: []channelmembers.LiveMembershipAuthorityResult{{ChannelFound: true, Subscriber: false, SubscriberMutationVersion: 5}}}
	app := New(Options{Memberships: rows, MembershipAuthority: authority})
	if ready, missing, err := app.CheckChannelSubscribers(context.Background(), "group", 2, []string{"u1"}); !errors.Is(err, ErrSubscriberMembershipSplit) || len(ready) != 0 || len(missing) != 0 {
		t.Fatalf("Channel missing / UID live ready=%v missing=%v err=%v", ready, missing, err)
	}
	rows.rows["u1"] = metadb.UserChannelMembership{UID: "u1", Tombstone: true, SourceVersion: 5}
	ready, missing, err := app.CheckChannelSubscribers(context.Background(), "group", 2, []string{"u1"})
	if err != nil || len(ready) != 0 || !reflect.DeepEqual(missing, []string{"u1"}) {
		t.Fatalf("both removed ready=%v missing=%v err=%v", ready, missing, err)
	}
	authority.results[0].Subscriber = true
	if ready, missing, err := app.CheckChannelSubscribers(context.Background(), "group", 2, []string{"u1"}); !errors.Is(err, ErrSubscriberMembershipSplit) || len(ready) != 0 || len(missing) != 0 {
		t.Fatalf("Channel live / UID tombstone ready=%v missing=%v err=%v", ready, missing, err)
	}
}

func TestCheckChannelSubscribersFailsClosedOnAuthorityError(t *testing.T) {
	want := errors.New("slot unavailable")
	app := New(Options{Memberships: subscriberCheckMemberships{rows: map[string]metadb.UserChannelMembership{"u1": {}}}, MembershipAuthority: &recordingLiveMembershipAuthority{
		results: []channelmembers.LiveMembershipAuthorityResult{{Err: want}},
	}})
	_, _, err := app.CheckChannelSubscribers(context.Background(), "group", 2, []string{"u1"})
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
}
