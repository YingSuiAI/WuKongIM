package channel

import (
	"context"
	"errors"
	"testing"
)

type committedMessagesStore struct {
	Store
	key   ChannelKey
	query CommittedMessagesQuery
	page  CommittedMessagesPage
	found bool
	err   error
}

func (s *committedMessagesStore) ReadCommittedMessages(_ context.Context, key ChannelKey, query CommittedMessagesQuery) (CommittedMessagesPage, bool, error) {
	s.key, s.query = key, query
	return s.page, s.found, s.err
}

func TestReadCommittedMessagesDelegatesAndOwnsPayloads(t *testing.T) {
	payload := []byte("proof")
	store := &committedMessagesStore{found: true, page: CommittedMessagesPage{
		Messages: []CommittedMessage{{MessageID: 7, MessageSeq: 3, Payload: payload}}, ScanHead: 3, FirstAvailableMessageSeq: 1, NextAfterMessageSeq: 3,
	}}
	app := New(Options{Store: store})
	key := ChannelKey{ChannelID: "group", ChannelType: 2}
	query := CommittedMessagesQuery{AfterMessageSeq: 1, Limit: 10, ScanHead: 3}

	page, found, err := app.ReadCommittedMessages(context.Background(), key, query)

	if err != nil || !found || store.key != key || store.query != query || string(page.Messages[0].Payload) != "proof" {
		t.Fatalf("page=%+v found=%v err=%v key=%+v query=%+v", page, found, err, store.key, store.query)
	}
	payload[0] = 'x'
	if string(page.Messages[0].Payload) != "proof" {
		t.Fatalf("returned payload aliases store payload: %q", page.Messages[0].Payload)
	}
}

func TestReadCommittedMessagesValidatesQueryAndCapability(t *testing.T) {
	for _, test := range []struct {
		name  string
		app   *App
		key   ChannelKey
		query CommittedMessagesQuery
		want  error
	}{
		{name: "empty channel", app: New(Options{Store: &committedMessagesStore{}}), key: ChannelKey{ChannelType: 2}, query: CommittedMessagesQuery{Limit: 1}, want: ErrCommittedMessagesQuery},
		{name: "zero limit", app: New(Options{Store: &committedMessagesStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, want: ErrCommittedMessagesQuery},
		{name: "large limit", app: New(Options{Store: &committedMessagesStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, query: CommittedMessagesQuery{Limit: 101}, want: ErrCommittedMessagesQuery},
		{name: "after above head", app: New(Options{Store: &committedMessagesStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, query: CommittedMessagesQuery{AfterMessageSeq: 2, Limit: 1, ScanHead: 1}, want: ErrCommittedMessagesQuery},
		{name: "missing capability", app: New(Options{Store: &recordingStore{}}), key: ChannelKey{ChannelID: "g", ChannelType: 2}, query: CommittedMessagesQuery{Limit: 1}, want: ErrCommittedMessagesUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := test.app.ReadCommittedMessages(context.Background(), test.key, test.query)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
}
