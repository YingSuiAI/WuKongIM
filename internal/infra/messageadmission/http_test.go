package messageadmission

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	imadmission "github.com/YingSuiAI/centerim-contracts/generated/imadmission-go"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAdmissionSupportsEnvelopeBudgetBeyondSnapshotTextLimit(t *testing.T) {
	// Contracts allows a 256 KiB Agent snapshot text within a 320 KiB envelope.
	// Transport bounds apply to the entire envelope, not just that text field.
	const envelopeLimit = 320 * 1024
	const prefix = `{"id":"019c0000-0000-7000-8000-000000000002","payload":{"text":"`
	const suffix = `"}}`
	payload := func(size int) []byte {
		return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
	}
	for _, size := range []int{256*1024 + 1024, envelopeLimit, envelopeLimit + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			calls := 0
			responsePayload := payload(size)
			client, err := New(Options{URL: "http://platform.internal" + admissionPath, SigningSecret: "test-secret",
				Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					body, err := json.Marshal(imadmission.ImMessageAdmissionResponse{Payload: responsePayload})
					require.NoError(t, err)
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
				})}})
			require.NoError(t, err)
			command := message.SendCommand{ServiceAuthenticated: true, Payload: payload(size)}
			actual, reason, err := client.AdmitSend(t.Context(), command)
			if size <= envelopeLimit {
				require.NoError(t, err)
				require.Equal(t, message.ReasonSuccess, reason)
				require.Equal(t, responsePayload, actual)
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, message.ReasonInvalidRequest, reason)
				require.Zero(t, calls, "oversized source must fail before HTTP")
				command.Payload = []byte(`{}`)
				_, reason, err = client.AdmitSend(t.Context(), command)
				require.Equal(t, message.ReasonSystemError, reason)
				require.ErrorIs(t, err, ErrUnavailable)
				require.Equal(t, 1, calls, "oversized canonical response is never accepted")
			}
		})
	}
}

func TestHTTPAdmissionSignsTrustedOriginAndReturnsExactCanonicalBytes(t *testing.T) {
	canonical := []byte(`{"id":"019c0000-0000-7000-8000-000000000002","body":{"text":"canonical"}}`)
	for _, device := range []bool{false, true} {
		t.Run(map[bool]string{false: "service", true: "device"}[device], func(t *testing.T) {
			calls := 0
			client, err := New(Options{URL: "http://platform.internal" + admissionPath, SigningSecret: "test-secret",
				Client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls++
					body, err := io.ReadAll(request.Body)
					require.NoError(t, err)
					require.Equal(t, http.MethodPost, request.Method)
					require.Equal(t, admissionPath, request.URL.Path)
					requestHash := sha256.Sum256(body)
					mac := hmac.New(sha256.New, []byte("test-secret"))
					_, _ = mac.Write([]byte("im.message.admission.v1\n" + request.Header.Get("X-IM-Timestamp") + "\n" + request.Header.Get("X-IM-Request-ID") + "\n" + hex.EncodeToString(requestHash[:])))
					require.Equal(t, hex.EncodeToString(mac.Sum(nil)), request.Header.Get("X-IM-Signature"))
					var input imadmission.ImMessageAdmissionRequest
					require.NoError(t, json.Unmarshal(body, &input))
					require.Equal(t, "authenticated-sender", input.FromUid)
					require.Equal(t, map[bool]string{false: "service", true: "device"}[device], string(input.Origin))
					if device {
						require.NotNil(t, input.ImSessionId)
						require.Equal(t, "019c0000-0000-7000-8000-000000000001", input.ImSessionId.String())
					} else {
						require.Nil(t, input.ImSessionId)
					}
					require.Equal(t, `{"origin":"service","from_uid":"forged","im_session_id":"forged","id":"forged"}`, string(input.Payload))
					deadline, ok := request.Context().Deadline()
					require.True(t, ok)
					require.Positive(t, time.Until(deadline))
					require.LessOrEqual(t, time.Until(deadline), 2*time.Second)
					response, err := json.Marshal(imadmission.ImMessageAdmissionResponse{Payload: canonical})
					require.NoError(t, err)
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(response))), Header: make(http.Header)}, nil
				})},
			})
			require.NoError(t, err)
			command := message.SendCommand{FromUID: "authenticated-sender", ChannelID: "room", ChannelType: 2,
				ClientMsgNo: "client-message-0001", Payload: []byte(`{"origin":"service","from_uid":"forged","im_session_id":"forged","id":"forged"}`), ServiceAuthenticated: !device}
			if device {
				command.SenderSessionID = 9
				command.IMSessionID = "019c0000-0000-7000-8000-000000000001"
			}
			for attempt := 0; attempt < 2; attempt++ {
				payload, reason, err := client.AdmitSend(context.Background(), command)
				require.NoError(t, err)
				require.Equal(t, message.ReasonSuccess, reason)
				require.Equal(t, canonical, payload)
			}
			require.Equal(t, 2, calls)
		})
	}
}

func TestHTTPAdmissionRejectsUnverifiedOrMixedOriginBeforeNetwork(t *testing.T) {
	client, err := New(Options{URL: "https://platform.internal" + admissionPath, SigningSecret: "test-secret",
		Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("unexpected request")
			return nil, errors.New("unexpected network")
		})}})
	require.NoError(t, err)
	for _, command := range []message.SendCommand{
		{Payload: []byte("body")},
		{Payload: []byte("body"), SenderSessionID: 1},
		{Payload: []byte("body"), SenderSessionID: 1, IMSessionID: "forged"},
		{Payload: []byte("body"), SenderSessionID: 1, ServiceAuthenticated: true},
		{Payload: []byte("body"), ServiceAuthenticated: true, IMSessionID: "019c0000-0000-7000-8000-000000000001"},
	} {
		_, reason, err := client.AdmitSend(context.Background(), command)
		require.NoError(t, err)
		require.Equal(t, message.ReasonAuthFail, reason)
	}
}

func TestHTTPAdmissionBoundsFailuresAndDoesNotRetryOrLeakDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name           string
		status         int
		body           string
		transportError bool
		reason         message.Reason
		wantError      bool
	}{
		{"forbidden", 403, `{"code":"FORBIDDEN"}`, false, message.ReasonNotAllowSend, false},
		{"conflict", 409, `{"code":"IDEMPOTENCY_CONFLICT"}`, false, message.ReasonInvalidRequest, false},
		{"auth", 401, `{"code":"UNAUTHENTICATED"}`, false, message.ReasonAuthFail, false},
		{"unavailable", 503, `{"code":"ADMISSION_UNAVAILABLE"}`, false, message.ReasonSystemError, true},
		{"misclassified server failure", 503, `{"code":"FORBIDDEN"}`, false, message.ReasonSystemError, true},
		{"unsafe detail", 403, `{"code":"FORBIDDEN","detail":"private"}`, false, message.ReasonSystemError, true},
		{"redirect", 307, "", false, message.ReasonSystemError, true},
		{"malformed", 200, "private response body", false, message.ReasonSystemError, true},
		{"missing metadata", 200, `{"payload":"e30="}`, false, message.ReasonSystemError, true},
		{"oversized", 200, strings.Repeat("x", maximumBodyBytes+1), false, message.ReasonSystemError, true},
		{"transport", 0, "", true, message.ReasonSystemError, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client, err := New(Options{URL: "http://platform.internal" + admissionPath, SigningSecret: "test-secret", Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if test.transportError {
					return nil, errors.New("private transport secret")
				}
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			})}})
			require.NoError(t, err)
			_, reason, err := client.AdmitSend(context.Background(), message.SendCommand{ServiceAuthenticated: true, Payload: []byte("body")})
			require.Equal(t, test.reason, reason)
			require.Equal(t, 1, calls)
			if test.wantError {
				require.ErrorIs(t, err, ErrUnavailable)
				require.NotContains(t, err.Error(), "private")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHTTPAdmissionConfigurationPinsEndpointAndDisablesRedirects(t *testing.T) {
	for _, endpoint := range []string{"", "ftp://platform" + admissionPath, "https://user:pass@platform" + admissionPath, "https://platform/other", "https://platform" + admissionPath + "?x=1", "https://platform" + admissionPath + "#secret"} {
		_, err := New(Options{URL: endpoint, SigningSecret: "test-secret"})
		require.ErrorIs(t, err, ErrInvalidConfiguration)
	}
	client, err := New(Options{URL: "https://platform" + admissionPath, SigningSecret: "test-secret"})
	require.NoError(t, err)
	require.ErrorIs(t, client.http.CheckRedirect(nil, nil), http.ErrUseLastResponse)
}

func TestHTTPAdmissionPreservesCallerCancellation(t *testing.T) {
	client, err := New(Options{URL: "http://platform.internal" + admissionPath, SigningSecret: "test-secret", Client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, request.Context().Err()
	})}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = client.AdmitSend(ctx, message.SendCommand{ServiceAuthenticated: true, Payload: []byte("body")})
	require.ErrorIs(t, err, context.Canceled)
}
