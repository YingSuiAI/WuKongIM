package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recordingSubscriberChecker struct {
	MessageUsecase
	calls   int
	ready   []string
	missing []string
	err     error
}

func (r *recordingSubscriberChecker) CheckChannelSubscribers(_ context.Context, _ string, _ uint8, _ []string) ([]string, []string, error) {
	r.calls++
	return r.ready, r.missing, r.err
}

func TestChannelSubscriberCheckRequiresServiceTokenAndValidBatch(t *testing.T) {
	for _, tc := range []struct {
		name, auth, body string
		want             int
		calls            int
	}{
		{"no token", "", `{"channel_id":"g","channel_type":2,"subscribers":["u1"]}`, http.StatusUnauthorized, 0},
		{"wrong token", "Bearer wrong", `{"channel_id":"g","channel_type":2,"subscribers":["u1"]}`, http.StatusUnauthorized, 0},
		{"valid", "Bearer secret", `{"channel_id":"g","channel_type":2,"subscribers":["u1","u2"]}`, http.StatusOK, 1},
		{"duplicate", "Bearer secret", `{"channel_id":"g","channel_type":2,"subscribers":["u1","u1"]}`, http.StatusBadRequest, 0},
		{"person", "Bearer secret", `{"channel_id":"g","channel_type":1,"subscribers":["u1"]}`, http.StatusBadRequest, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checker := &recordingSubscriberChecker{ready: []string{"u1"}, missing: []string{"u2"}}
			srv := New(Options{Messages: checker, ServiceToken: "secret"})
			req := httptest.NewRequest(http.MethodPost, "/channel/subscriber_check", strings.NewReader(tc.body))
			req.Header.Set("Authorization", tc.auth)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.want || checker.calls != tc.calls {
				t.Fatalf("status=%d calls=%d body=%s", rec.Code, checker.calls, rec.Body.String())
			}
			if tc.want == http.StatusOK && rec.Body.String() != `{"missing":["u2"],"ready":["u1"]}` {
				t.Fatalf("body=%s, want exact ready/missing contract", rec.Body.String())
			}
		})
	}
}

func TestChannelSubscriberCheckFailsClosedOnAuthorityError(t *testing.T) {
	checker := &recordingSubscriberChecker{err: errors.New("slot unavailable")}
	srv := New(Options{Messages: checker, ServiceToken: "secret"})
	req := httptest.NewRequest(http.MethodPost, "/channel/subscriber_check", strings.NewReader(`{"channel_id":"g","channel_type":2,"subscribers":["u1"]}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || checker.calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, checker.calls, rec.Body.String())
	}
}
