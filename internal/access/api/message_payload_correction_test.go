package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPayloadCorrectionRoutesRequireServiceAuthority(t *testing.T) {
	for _, path := range []string{"/channel/message-payload-correction", "/channel/committed-message-claim"} {
		for _, token := range []string{"", "Bearer incorrect"} {
			srv := New(Options{ServiceToken: "service-secret"})
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			request.Header.Set("Authorization", token)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			srv.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Errorf("%s token=%q status=%d want401", path, token, response.Code)
			}
		}
	}
}
