package message

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	channelmembers "github.com/WuKongIM/WuKongIM/internal/contracts/channelmembers"
)

func TestCheckChannelDenylistUsesOneCurrentAuthoritativeBatch(t *testing.T) {
	base := newFakePermissionStore()
	denyID := channelmembers.DenylistChannelID(channelmembers.ChannelKey{ChannelID: "g1", ChannelType: channelTypeGroup})
	base.members[permissionKey(denyID, int64(channelTypeGroup))] = map[string]bool{"u1": true}
	store := &recordingPermissionBatchStore{base: base}
	app := New(Options{PermissionStore: store, PermissionBatchStore: store, PermissionCacheTTL: time.Hour})
	check := func(wantDenied, wantAllowed []string) {
		t.Helper()
		denied, allowed, err := app.CheckChannelDenylist(context.Background(), "g1", channelTypeGroup, []string{"u1", "u2"})
		if err != nil || !reflect.DeepEqual(denied, wantDenied) || !reflect.DeepEqual(allowed, wantAllowed) {
			t.Fatalf("CheckChannelDenylist() denied=%v allowed=%v err=%v", denied, allowed, err)
		}
	}
	check([]string{"u1"}, []string{"u2"})
	delete(base.members[permissionKey(denyID, int64(channelTypeGroup))], "u1")
	base.members[permissionKey(denyID, int64(channelTypeGroup))]["u2"] = true
	check([]string{"u2"}, []string{"u1"})
	if got := store.batchCalls.Load(); got != 2 {
		t.Fatalf("authoritative read batches = %d, want one per check", got)
	}
	if got := base.getChannelCalls.Load() + base.containsCalls.Load() + base.hasAnyCalls.Load(); got != 0 {
		t.Fatalf("point reads = %d, want batched Slot reads", got)
	}
	for _, read := range store.reads {
		if read.Kind != PermissionReadSubscriberContains || read.ChannelID != denyID || read.ChannelType != int64(channelTypeGroup) {
			t.Fatalf("unexpected read fact: %+v", read)
		}
	}
}

type fixedDenylistBatchStore struct {
	results []PermissionReadResult
}

func (s fixedDenylistBatchStore) ReadPermissionsBatch(context.Context, []PermissionRead) []PermissionReadResult {
	return s.results
}

func TestCheckChannelDenylistFailsClosedOnIncompleteOrFailedAuthority(t *testing.T) {
	wantErr := errors.New("slot unavailable")
	for _, tc := range []struct {
		name    string
		results []PermissionReadResult
		wantErr error
	}{
		{"missing result", []PermissionReadResult{{Value: true}}, ErrRouteNotReady},
		{"failed second result", []PermissionReadResult{{Value: true}, {Err: wantErr}}, wantErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := New(Options{PermissionStore: newFakePermissionStore(), PermissionBatchStore: fixedDenylistBatchStore{results: tc.results}})
			denied, allowed, err := app.CheckChannelDenylist(context.Background(), "g1", channelTypeGroup, []string{"u1", "u2"})
			if !errors.Is(err, tc.wantErr) || denied != nil || allowed != nil {
				t.Fatalf("denied=%v allowed=%v err=%v, want no partial result and %v", denied, allowed, err, tc.wantErr)
			}
		})
	}
}

func TestCheckChannelDenylistRequiresAuthoritativeBatchPort(t *testing.T) {
	app := New(Options{PermissionStore: newFakePermissionStore()})
	denied, allowed, err := app.CheckChannelDenylist(context.Background(), "g1", channelTypeGroup, []string{"u1"})
	if !errors.Is(err, ErrRouteNotReady) || denied != nil || allowed != nil {
		t.Fatalf("denied=%v allowed=%v err=%v, want fail-closed batch-port requirement", denied, allowed, err)
	}
}
