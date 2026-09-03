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

type committedMessagesChannelUsecase struct {
	ChannelUsecase
	key   channelusecase.ChannelKey
	query channelusecase.CommittedMessagesQuery
	calls int
	page  channelusecase.CommittedMessagesPage
	found bool
	err   error
}

func (u *committedMessagesChannelUsecase) ReadCommittedMessages(_ context.Context, key channelusecase.ChannelKey, query channelusecase.CommittedMessagesQuery) (channelusecase.CommittedMessagesPage, bool, error) {
	u.key, u.query, u.calls = key, query, u.calls+1
	return u.page, u.found, u.err
}

func TestChannelCommittedMessagesRequiresServiceToken(t *testing.T) {
	for _, auth := range []string{"", "secret", "Bearer wrong", "Bearer secret"} {
		t.Run(auth, func(t *testing.T) {
			u := &committedMessagesChannelUsecase{found: true, page: channelusecase.CommittedMessagesPage{ScanHead: 9, FirstAvailableMessageSeq: 1, NextAfterMessageSeq: 9}}
			srv := New(Options{Channels: u, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/committed-messages", strings.NewReader(`{"channel_id":"opaque","channel_type":2,"after_message_seq":3,"limit":10,"scan_head":9}`))
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
			if rec.Code != http.StatusOK || u.calls != 1 || u.key != (channelusecase.ChannelKey{ChannelID: "opaque", ChannelType: 2}) ||
				u.query != (channelusecase.CommittedMessagesQuery{AfterMessageSeq: 3, Limit: 10, ScanHead: 9}) {
				t.Fatalf("status=%d calls=%d key=%+v query=%+v body=%s", rec.Code, u.calls, u.key, u.query, rec.Body.String())
			}
		})
	}
}

func TestChannelCommittedMessagesReturnsFencedHistoryPage(t *testing.T) {
	u := &committedMessagesChannelUsecase{found: true, page: channelusecase.CommittedMessagesPage{
		ScanHead: 12, FirstAvailableMessageSeq: 5, RetentionGap: true,
		NextAfterMessageSeq: 8, HasMore: true,
		Messages: []channelusecase.CommittedMessage{{
			MessageID: 2093294222990905344, MessageSeq: 8, ChannelID: "group", ChannelType: 2,
			Setting: 3, FromUID: "sender", ClientMsgNo: "client-1", ServerTimestampMS: 1700000000123,
			SyncOnce: true, Payload: []byte("hello"),
		}},
	}}
	srv := New(Options{Channels: u, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/committed-messages", strings.NewReader(`{"channel_id":"group","channel_type":2,"after_message_seq":2,"limit":1}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	want := `{"scan_head":12,"first_available_message_seq":5,"retention_gap":true,"next_after_message_seq":8,"has_more":true,"messages":[{"header":{"no_persist":0,"red_dot":0,"sync_once":1},"setting":3,"message_id":2093294222990905344,"message_idstr":"2093294222990905344","client_msg_no":"client-1","message_seq":8,"from_uid":"sender","channel_id":"group","channel_type":2,"expire":0,"timestamp":1700000000,"payload":"aGVsbG8="}]}`
	if rec.Code != http.StatusOK || !jsonEqual(rec.Body.String(), want) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestChannelCommittedMessagesReturnsTerminalRetentionGapPastScanHead(t *testing.T) {
	u := &committedMessagesChannelUsecase{found: true, page: channelusecase.CommittedMessagesPage{
		ScanHead: 2, FirstAvailableMessageSeq: 5, RetentionGap: true,
		NextAfterMessageSeq: 2, HasMore: false, Messages: []channelusecase.CommittedMessage{},
	}}
	srv := New(Options{Channels: u, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/committed-messages", strings.NewReader(`{"channel_id":"group","channel_type":2,"after_message_seq":1,"limit":10,"scan_head":2}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	want := `{"scan_head":2,"first_available_message_seq":5,"retention_gap":true,"next_after_message_seq":2,"has_more":false,"messages":[]}`
	if rec.Code != http.StatusOK || !jsonEqual(rec.Body.String(), want) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestChannelCommittedMessagesMapsBadUnknownAndUnavailable(t *testing.T) {
	tests := []struct {
		name string
		body string
		use  *committedMessagesChannelUsecase
		want int
	}{
		{name: "malformed", body: `{`, use: &committedMessagesChannelUsecase{}, want: http.StatusBadRequest},
		{name: "empty channel", body: `{"channel_id":"","channel_type":2,"limit":1}`, use: &committedMessagesChannelUsecase{}, want: http.StatusBadRequest},
		{name: "zero type", body: `{"channel_id":"g","channel_type":0,"limit":1}`, use: &committedMessagesChannelUsecase{}, want: http.StatusBadRequest},
		{name: "zero limit", body: `{"channel_id":"g","channel_type":2,"limit":0}`, use: &committedMessagesChannelUsecase{}, want: http.StatusBadRequest},
		{name: "large limit", body: `{"channel_id":"g","channel_type":2,"limit":101}`, use: &committedMessagesChannelUsecase{}, want: http.StatusBadRequest},
		{name: "after above scan head", body: `{"channel_id":"g","channel_type":2,"after_message_seq":3,"limit":1,"scan_head":2}`, use: &committedMessagesChannelUsecase{}, want: http.StatusBadRequest},
		{name: "unknown", body: `{"channel_id":"g","channel_type":2,"limit":1}`, use: &committedMessagesChannelUsecase{}, want: http.StatusNotFound},
		{name: "unavailable", body: `{"channel_id":"g","channel_type":2,"limit":1}`, use: &committedMessagesChannelUsecase{err: errors.New("offline")}, want: http.StatusServiceUnavailable},
		{name: "query error", body: `{"channel_id":"g","channel_type":2,"limit":1}`, use: &committedMessagesChannelUsecase{err: channelusecase.ErrCommittedMessagesQuery}, want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := New(Options{Channels: test.use, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/committed-messages", strings.NewReader(test.body))
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
