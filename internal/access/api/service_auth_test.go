package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUserTokenRequiresServiceBearerToken(t *testing.T) {
	const secret = "service-secret"
	for _, tt := range []struct {
		name          string
		authorization string
		wantStatus    int
		wantCalls     int
	}{
		{name: "missing", wantStatus: http.StatusUnauthorized},
		{name: "wrong", authorization: "Bearer wrong", wantStatus: http.StatusUnauthorized},
		{name: "correct", authorization: "Bearer " + secret, wantStatus: http.StatusOK, wantCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			users := &recordingUserUsecase{}
			srv := New(Options{Users: users, ServiceToken: secret})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/user/token", bytes.NewBufferString(`{"uid":"u1","token":"t1","device_flag":0,"device_level":1}`))
			req.Header.Set("Content-Type", "application/json")
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d body = %s, want %d", rec.Code, rec.Body.String(), tt.wantStatus)
			}
			if strings.Contains(rec.Body.String(), secret) {
				t.Fatalf("response leaked service token: %s", rec.Body.String())
			}
			if got := len(users.tokenCommands); got != tt.wantCalls {
				t.Fatalf("token command calls = %d, want %d", got, tt.wantCalls)
			}
		})
	}
}

func TestUserTokenFailsClosedWhenServiceTokenUnconfigured(t *testing.T) {
	users := &recordingUserUsecase{}
	srv := New(Options{Users: users})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/user/token", bytes.NewBufferString(`{"uid":"u1","token":"t1"}`))
	req.Header.Set("Authorization", "Bearer anything")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body = %s, want 401", rec.Code, rec.Body.String())
	}
	if len(users.tokenCommands) != 0 {
		t.Fatalf("token command calls = %d, want 0", len(users.tokenCommands))
	}
}
