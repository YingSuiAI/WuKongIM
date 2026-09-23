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
		{ChannelFound: true, Subscriber: false, SubscriberMutationVersion: 13},
	}}
	app := New(Options{Memberships: subscriberCheckMemberships{rows: map[string]metadb.UserChannelMembership{
		"ready": {SourceVersion: 12}, "revoked": {SourceVersion: 12}, "removed": {Tombstone: true, SourceVersion: 11},
	}}, MembershipAuthority: authority})
	ready, missing, err := app.CheckChannelSubscribers(context.Background(), "group", 2, []string{"ready", "removed", "revoked"})
	if err != nil || !reflect.DeepEqual(ready, []string{"ready"}) || !reflect.DeepEqual(missing, []string{"removed", "revoked"}) {
		t.Fatalf("ready=%#v missing=%#v err=%v", ready, missing, err)
	}
	if len(authority.tombstones) != 1 || authority.tombstones[0].membership.UID != "revoked" || authority.tombstones[0].version != 13 {
		t.Fatalf("tombstones=%#v, want stale UID projection repaired", authority.tombstones)
	}
	if len(authority.candidates) != 1 || len(authority.candidates[0]) != 2 {
		t.Fatalf("authority candidates=%#v, want live UIDs only", authority.candidates)
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
