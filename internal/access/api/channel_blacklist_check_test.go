package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recordingBlacklistChecker struct {
	MessageUsecase
	calls   int
	denied  []string
	allowed []string
	err     error
}

func TestChannelBlacklistCheckRejectsOversizedBatch(t *testing.T) {
	uids := make([]string, maxSubscriberCheckUIDs+1)
	for i := range uids {
		uids[i] = fmt.Sprintf("u%d", i)
	}
	body, err := json.Marshal(channelBlacklistCheckRequest{ChannelID: "g", ChannelType: 2, UIDs: uids})
	if err != nil {
		t.Fatal(err)
	}
	checker := &recordingBlacklistChecker{}
	srv := New(Options{Messages: checker, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/blacklist_check", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || checker.calls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, checker.calls, rec.Body.String())
	}
}

func TestChannelBlacklistCheckRequiresConfiguredServiceToken(t *testing.T) {
	checker := &recordingBlacklistChecker{}
	srv := New(Options{Messages: checker})
	req := httptest.NewRequest(http.MethodPost, "/channel/blacklist_check", strings.NewReader(`{"channel_id":"g","channel_type":2,"uids":["u1"]}`))
	req.Header.Set("Authorization", "Bearer any")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || checker.calls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, checker.calls, rec.Body.String())
	}
}

func (r *recordingBlacklistChecker) CheckChannelDenylist(_ context.Context, _ string, _ uint8, _ []string) ([]string, []string, error) {
	r.calls++
	return r.denied, r.allowed, r.err
}

func TestChannelBlacklistCheckRequiresServiceTokenAndValidBatch(t *testing.T) {
	for _, tc := range []struct {
		name, auth, body string
		want             int
		calls            int
	}{
		{"no token", "", `{"channel_id":"g","channel_type":2,"uids":["u1"]}`, http.StatusUnauthorized, 0},
		{"wrong token", "Bearer wrong", `{"channel_id":"g","channel_type":2,"uids":["u1"]}`, http.StatusUnauthorized, 0},
		{"valid", "Bearer secret", `{"channel_id":"g","channel_type":2,"uids":["u1","u2"]}`, http.StatusOK, 1},
		{"duplicate", "Bearer secret", `{"channel_id":"g","channel_type":2,"uids":["u1","u1"]}`, http.StatusBadRequest, 0},
		{"empty", "Bearer secret", `{"channel_id":"g","channel_type":2,"uids":[]}`, http.StatusBadRequest, 0},
		{"person", "Bearer secret", `{"channel_id":"g","channel_type":1,"uids":["u1"]}`, http.StatusBadRequest, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checker := &recordingBlacklistChecker{denied: []string{"u1"}, allowed: []string{"u2"}}
			srv := New(Options{Messages: checker, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/blacklist_check", strings.NewReader(tc.body))
			req.Header.Set("Authorization", tc.auth)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.want || checker.calls != tc.calls {
				t.Fatalf("status=%d calls=%d body=%s", rec.Code, checker.calls, rec.Body.String())
			}
			if tc.want == http.StatusOK && rec.Body.String() != `{"allowed":["u2"],"denied":["u1"]}` {
				t.Fatalf("body=%s, want exact denied/allowed contract", rec.Body.String())
			}
		})
	}
}

func TestChannelBlacklistCheckFailsClosedOnAuthorityError(t *testing.T) {
	checker := &recordingBlacklistChecker{err: errors.New("slot unavailable")}
	srv := New(Options{Messages: checker, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/blacklist_check", strings.NewReader(`{"channel_id":"g","channel_type":2,"uids":["u1"]}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || checker.calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, checker.calls, rec.Body.String())
	}
}
