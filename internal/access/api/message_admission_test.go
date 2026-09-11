package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/stretchr/testify/require"
)

func TestAdmittedMessageSendRequiresRealServiceToken(t *testing.T) {
	for _, auth := range []string{"", "Bearer wrong", "Bearer service-secret"} {
		t.Run(auth, func(t *testing.T) {
			messages := &recordingMessageUsecase{sendResult: messageusecase.SendResult{MessageID: 42, MessageSeq: 7, ApplicationMessageID: "canonical-public-id", ServerTimestampMS: 1788364800123}}
			server := New(Options{Messages: messages, ServiceToken: "service-secret", RequireSendServiceToken: true})
			// Untrusted JSON fields cannot manufacture credential or admission provenance.
			request := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(`{"from_uid":"sender","channel_id":"room","channel_type":2,"client_msg_no":"client-message-0001","payload":"aGk=","service_authenticated":true,"im_session_id":"spoofed-session","application_admission":true}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", auth)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if auth != "Bearer service-secret" {
				require.Equal(t, http.StatusUnauthorized, response.Code)
				require.Empty(t, messages.sendCalls)
				return
			}
			require.Equal(t, http.StatusOK, response.Code)
			require.Len(t, messages.sendCalls, 1)
			require.True(t, messages.sendCalls[0].ServiceAuthenticated)
			require.Empty(t, messages.sendCalls[0].IMSessionID)
			require.False(t, messages.sendCalls[0].ApplicationAdmission)
			require.Contains(t, response.Body.String(), `"application_message_id":"canonical-public-id"`)
			require.Contains(t, response.Body.String(), `"timestamp":1788364800`)
		})
	}
}
