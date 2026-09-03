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

type headChannelUsecase struct {
	ChannelUsecase
	key   channelusecase.ChannelKey
	calls int
	seq   uint64
	err   error
}

func (u *headChannelUsecase) ReadCommittedHead(_ context.Context, key channelusecase.ChannelKey) (uint64, error) {
	u.key, u.calls = key, u.calls+1
	return u.seq, u.err
}

func TestChannelCommittedHeadRequiresServiceToken(t *testing.T) {
	for _, auth := range []string{"", "secret", "Bearer wrong", "Bearer secret"} {
		t.Run(auth, func(t *testing.T) {
			u := &headChannelUsecase{seq: 100}
			srv := New(Options{Channels: u, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/committed-head", strings.NewReader(`{"channel_id":"opaque","channel_type":2}`))
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
			if rec.Code != http.StatusOK || u.calls != 1 || u.key.ChannelID != "opaque" || u.key.ChannelType != 2 || rec.Body.String() != `{"channel_id":"opaque","channel_type":2,"committed_message_seq":100}` {
				t.Fatalf("status=%d calls=%d body=%s", rec.Code, u.calls, rec.Body.String())
			}
		})
	}
}

func TestChannelCommittedHeadFailsClosedWithoutConfiguredServiceToken(t *testing.T) {
	u := &headChannelUsecase{seq: 100}
	srv := New(Options{Channels: u})
	req := httptest.NewRequest(http.MethodPost, "/channel/committed-head", strings.NewReader(`{"channel_id":"opaque","channel_type":2}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized || u.calls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, u.calls, rec.Body.String())
	}
}

func TestChannelCommittedHeadValidatesInputAndMapsUnavailable(t *testing.T) {
	tests := []struct {
		name string
		body string
		use  *headChannelUsecase
		want int
	}{
		{name: "malformed", body: `{`, use: &headChannelUsecase{}, want: http.StatusBadRequest},
		{name: "empty id", body: `{"channel_id":"","channel_type":2}`, use: &headChannelUsecase{}, want: http.StatusBadRequest},
		{name: "padded id", body: `{"channel_id":" group ","channel_type":2}`, use: &headChannelUsecase{}, want: http.StatusBadRequest},
		{name: "zero type", body: `{"channel_id":"group","channel_type":0}`, use: &headChannelUsecase{}, want: http.StatusBadRequest},
		{name: "reader unavailable", body: `{"channel_id":"group","channel_type":2}`, use: &headChannelUsecase{err: errors.New("offline")}, want: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := New(Options{Channels: test.use, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/committed-head", strings.NewReader(test.body))
			req.Header.Set("Authorization", "Bearer secret")
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != test.want {
				t.Fatalf("status=%d body=%s, want %d", rec.Code, rec.Body.String(), test.want)
			}
			if test.want == http.StatusBadRequest && test.use.calls != 0 {
				t.Fatalf("invalid request reached usecase %d times", test.use.calls)
			}
		})
	}
}

func TestChannelCommittedHeadFailsClosedWithoutReaderCapability(t *testing.T) {
	srv := New(Options{Channels: &recordingChannelUsecase{}, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/committed-head", strings.NewReader(`{"channel_id":"group","channel_type":2}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestChannelCommittedHeadReturnsZeroForEmptyOrUnknownChannel(t *testing.T) {
	u := &headChannelUsecase{}
	srv := New(Options{Channels: u, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/committed-head", strings.NewReader(`{"channel_id":"not-created-yet","channel_type":2}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != `{"channel_id":"not-created-yet","channel_type":2,"committed_message_seq":0}` {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
