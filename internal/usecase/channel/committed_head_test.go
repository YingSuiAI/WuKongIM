package channel

import (
	"context"
	"errors"
	"testing"
)

type committedHeadStore struct {
	*recordingStore
	key   ChannelKey
	calls int
	seq   uint64
	err   error
}

func (s *committedHeadStore) ReadCommittedHead(_ context.Context, key ChannelKey) (uint64, error) {
	s.key = key
	s.calls++
	return s.seq, s.err
}

func TestReadCommittedHeadDelegatesOpaqueChannelIdentity(t *testing.T) {
	store := &committedHeadStore{recordingStore: &recordingStore{}, seq: 42}
	app := New(Options{Store: store})

	seq, err := app.ReadCommittedHead(context.Background(), ChannelKey{ChannelID: "person@opaque#key", ChannelType: 1})

	if err != nil || seq != 42 || store.calls != 1 || store.key.ChannelID != "person@opaque#key" || store.key.ChannelType != 1 {
		t.Fatalf("seq=%d err=%v calls=%d key=%+v", seq, err, store.calls, store.key)
	}
}

func TestReadCommittedHeadRejectsInvalidInputAndMissingCapability(t *testing.T) {
	store := &committedHeadStore{recordingStore: &recordingStore{}}
	app := New(Options{Store: store})
	for _, key := range []ChannelKey{{}, {ChannelID: "group", ChannelType: 0}, {ChannelID: " group ", ChannelType: 2}} {
		if _, err := app.ReadCommittedHead(context.Background(), key); !errors.Is(err, ErrCommittedHeadUnavailable) {
			t.Fatalf("ReadCommittedHead(%+v) error = %v", key, err)
		}
	}
	if store.calls != 0 {
		t.Fatalf("invalid inputs reached store %d times", store.calls)
	}

	withoutCapability := New(Options{Store: &recordingStore{}})
	if _, err := withoutCapability.ReadCommittedHead(context.Background(), ChannelKey{ChannelID: "group", ChannelType: 2}); !errors.Is(err, ErrCommittedHeadUnavailable) {
		t.Fatalf("missing capability error = %v", err)
	}
}
