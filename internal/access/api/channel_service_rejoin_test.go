package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
)

type recordingServiceRejoinChannel struct {
	*recordingChannelUsecase
	state     channelusecase.ServiceRejoinState
	writes    int
	checks    int
	lastWrite channelusecase.ServiceRejoinCommand
}

func (r *recordingServiceRejoinChannel) RejoinSubscriber(_ context.Context, cmd channelusecase.ServiceRejoinCommand) (channelusecase.ServiceRejoinState, error) {
	r.writes++
	r.lastWrite = cmd
	return r.state, nil
}

func (r *recordingServiceRejoinChannel) CheckRejoinSubscriber(_ context.Context, _ channelusecase.ServiceRejoinCommand) (channelusecase.ServiceRejoinState, error) {
	r.checks++
	return r.state, nil
}

func TestServiceRejoinEndpointsRequireTokenAndExactBoundary(t *testing.T) {
	service := &recordingServiceRejoinChannel{
		recordingChannelUsecase: &recordingChannelUsecase{},
		state:                   channelusecase.ServiceRejoinState{Ready: true, MembershipEpoch: 3, JoinSeq: 51, DeletedToSeq: 50, SourceVersion: 9},
	}
	server := New(Options{Channels: service, ServiceToken: "secret"})
	request := func(path, body, token string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		return rec
	}
	writeBody := `{"channel_id":"g1","channel_type":2,"uid":"u1","membership_epoch":3,"previous_removed_message_sequence":30,"joined_message_sequence":50}`
	if rec := request("/channel/subscriber_rejoin", writeBody, ""); rec.Code != http.StatusUnauthorized || service.writes != 0 {
		t.Fatalf("unauthorized write status=%d calls=%d", rec.Code, service.writes)
	}
	if rec := request("/channel/subscriber_rejoin", `{"channel_id":"g1","channel_type":2,"uid":"u1","membership_epoch":3,"joined_message_sequence":50}`, "secret"); rec.Code != http.StatusBadRequest || service.writes != 0 {
		t.Fatalf("missing removal boundary status=%d calls=%d", rec.Code, service.writes)
	}
	if rec := request("/channel/subscriber_rejoin", writeBody, "secret"); rec.Code != http.StatusOK || rec.Body.String() != `{"deleted_to_seq":50,"join_seq":51,"membership_epoch":3,"source_version":9}` || service.writes != 1 {
		t.Fatalf("write status=%d calls=%d body=%s", rec.Code, service.writes, rec.Body.String())
	}
	repairBody := strings.TrimSuffix(writeBody, "}") + `,"repair_same_epoch":true}`
	if rec := request("/channel/subscriber_rejoin", repairBody, "secret"); rec.Code != http.StatusOK || !service.lastWrite.RepairSameEpoch {
		t.Fatalf("repair write status=%d command=%+v", rec.Code, service.lastWrite)
	}
	checkBody := `{"channel_id":"g1","channel_type":2,"uid":"u1","membership_epoch":3,"join_seq":51,"deleted_to_seq":50}`
	if rec := request("/channel/subscriber_rejoin_check", checkBody, "secret"); rec.Code != http.StatusOK || rec.Body.String() != `{"actual_deleted_to_seq":50,"actual_epoch":3,"actual_join_seq":51,"ready":true,"source_version":9}` || service.checks != 1 {
		t.Fatalf("check status=%d calls=%d body=%s", rec.Code, service.checks, rec.Body.String())
	}
	if rec := request("/channel/subscriber_rejoin_check", strings.Replace(checkBody, `"join_seq":51`, `"join_seq":52`, 1), "secret"); rec.Code != http.StatusBadRequest || service.checks != 1 {
		t.Fatalf("mismatched floor status=%d calls=%d", rec.Code, service.checks)
	}
}
