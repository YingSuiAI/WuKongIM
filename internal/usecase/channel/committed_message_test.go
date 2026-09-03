package channel

import (
	"context"
	"errors"
	"testing"
)

type committedMessageStore struct {
	Store
	key      ChannelKey
	identity CommittedMessageIdentity
	message  CommittedMessage
	found    bool
	err      error
}

func (s *committedMessageStore) ReadCommittedMessage(_ context.Context, key ChannelKey, identity CommittedMessageIdentity) (CommittedMessage, bool, error) {
	s.key, s.identity = key, identity
	return s.message, s.found, s.err
}

func TestReadCommittedMessageDelegatesExactIdentityAndOwnsPayload(t *testing.T) {
	payload := []byte("proof")
	store := &committedMessageStore{message: CommittedMessage{MessageID: 7, MessageSeq: 3, Payload: payload}, found: true}
	app := New(Options{Store: store})
	key := ChannelKey{ChannelID: "group", ChannelType: 2}
	identity := CommittedMessageIdentity{MessageID: 7, MessageSeq: 3}

	message, found, err := app.ReadCommittedMessage(context.Background(), key, identity)

	if err != nil || !found || store.key != key || store.identity != identity || string(message.Payload) != "proof" {
		t.Fatalf("message=%+v found=%v err=%v key=%+v identity=%+v", message, found, err, store.key, store.identity)
	}
	payload[0] = 'x'
	if string(message.Payload) != "proof" {
		t.Fatalf("returned payload aliases store payload: %q", message.Payload)
	}
}

func TestReadCommittedMessageValidatesIdentityAndCapability(t *testing.T) {
	for _, test := range []struct {
		name     string
		app      *App
		key      ChannelKey
		identity CommittedMessageIdentity
		want     error
	}{
		{name: "empty channel", app: New(Options{Store: &committedMessageStore{}}), key: ChannelKey{ChannelType: 2}, identity: CommittedMessageIdentity{MessageID: 1, MessageSeq: 1}, want: ErrCommittedMessageIdentity},
		{name: "zero message id", app: New(Options{Store: &committedMessageStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, identity: CommittedMessageIdentity{MessageSeq: 1}, want: ErrCommittedMessageIdentity},
		{name: "zero sequence", app: New(Options{Store: &committedMessageStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, identity: CommittedMessageIdentity{MessageID: 1}, want: ErrCommittedMessageIdentity},
		{name: "missing capability", app: New(Options{Store: &recordingStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, identity: CommittedMessageIdentity{MessageID: 1, MessageSeq: 1}, want: ErrCommittedMessageUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := test.app.ReadCommittedMessage(context.Background(), test.key, test.identity)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
}
