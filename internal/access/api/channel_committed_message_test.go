package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
)

type committedMessageChannelUsecase struct {
	ChannelUsecase
	key      channelusecase.ChannelKey
	identity channelusecase.CommittedMessageIdentity
	calls    int
	message  channelusecase.CommittedMessage
	found    bool
	err      error
}

func (u *committedMessageChannelUsecase) ReadCommittedMessage(_ context.Context, key channelusecase.ChannelKey, identity channelusecase.CommittedMessageIdentity) (channelusecase.CommittedMessage, bool, error) {
	u.key, u.identity, u.calls = key, identity, u.calls+1
	return u.message, u.found, u.err
}

func TestChannelCommittedMessageRequiresServiceToken(t *testing.T) {
	for _, auth := range []string{"", "secret", "Bearer wrong", "Bearer secret"} {
		t.Run(auth, func(t *testing.T) {
			u := &committedMessageChannelUsecase{found: true, message: channelusecase.CommittedMessage{MessageID: 71, MessageSeq: 9, ChannelID: "opaque", ChannelType: 2}}
			srv := New(Options{Channels: u, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/committed-message", strings.NewReader(`{"channel_id":"opaque","channel_type":2,"message_id":71,"message_seq":9}`))
			req.Header.Set("Authorization", auth)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if auth != "Bearer secret" {
				if rec.Code != http.StatusUnauthorized || u.calls != 0 {
					t.Fatalf("unauthorized status=%d calls=%d", rec.Code, u.calls)
				}
				return
			}
			if rec.Code != http.StatusOK || u.calls != 1 || u.key.ChannelID != "opaque" || u.identity != (channelusecase.CommittedMessageIdentity{MessageID: 71, MessageSeq: 9}) {
				t.Fatalf("status=%d calls=%d key=%+v identity=%+v body=%s", rec.Code, u.calls, u.key, u.identity, rec.Body.String())
			}
		})
	}
}

func TestChannelCommittedMessageReturnsExactHistoryTuple(t *testing.T) {
	u := &committedMessageChannelUsecase{found: true, message: channelusecase.CommittedMessage{
		MessageID: 2093294222990905344, MessageSeq: 8, ChannelID: "group", ChannelType: 2,
		Setting: 3, FromUID: "sender", ClientMsgNo: "client-1", ServerTimestampMS: 1700000000123,
		SyncOnce: true, Payload: []byte("hello"),
	}}
	srv := New(Options{Channels: u, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/committed-message", strings.NewReader(`{"channel_id":"group","channel_type":2,"message_id":2093294222990905344,"message_seq":8}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	want := `{"header":{"no_persist":0,"red_dot":0,"sync_once":1},"setting":3,"message_id":2093294222990905344,"message_idstr":"2093294222990905344","client_msg_no":"client-1","message_seq":8,"from_uid":"sender","channel_id":"group","channel_type":2,"expire":0,"timestamp":1700000000,"payload":"aGVsbG8="}`
	if rec.Code != http.StatusOK || !jsonEqual(rec.Body.String(), want) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestChannelCommittedMessageMapsBadNotFoundAndUnavailable(t *testing.T) {
	tests := []struct {
		name string
		body string
		use  *committedMessageChannelUsecase
		want int
	}{
		{name: "malformed", body: `{`, use: &committedMessageChannelUsecase{}, want: http.StatusBadRequest},
		{name: "empty channel", body: `{"channel_id":"","channel_type":2,"message_id":1,"message_seq":1}`, use: &committedMessageChannelUsecase{}, want: http.StatusBadRequest},
		{name: "zero type", body: `{"channel_id":"g","channel_type":0,"message_id":1,"message_seq":1}`, use: &committedMessageChannelUsecase{}, want: http.StatusBadRequest},
		{name: "zero message id", body: `{"channel_id":"g","channel_type":2,"message_id":0,"message_seq":1}`, use: &committedMessageChannelUsecase{}, want: http.StatusBadRequest},
		{name: "zero sequence", body: `{"channel_id":"g","channel_type":2,"message_id":1,"message_seq":0}`, use: &committedMessageChannelUsecase{}, want: http.StatusBadRequest},
		{name: "not found", body: `{"channel_id":"g","channel_type":2,"message_id":1,"message_seq":1}`, use: &committedMessageChannelUsecase{}, want: http.StatusNotFound},
		{name: "unavailable", body: `{"channel_id":"g","channel_type":2,"message_id":1,"message_seq":1}`, use: &committedMessageChannelUsecase{err: errors.New("offline")}, want: http.StatusServiceUnavailable},
		{name: "identity error", body: `{"channel_id":"g","channel_type":2,"message_id":1,"message_seq":1}`, use: &committedMessageChannelUsecase{err: channelusecase.ErrCommittedMessageIdentity}, want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := New(Options{Channels: test.use, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/committed-message", strings.NewReader(test.body))
			req.Header.Set("Authorization", "Bearer secret")
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != test.want {
				t.Fatalf("status=%d body=%s want=%d", rec.Code, rec.Body.String(), test.want)
			}
		})
	}
}
