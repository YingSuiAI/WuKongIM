package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// HTTPSenderOptions configures outbound HTTP webhook delivery.
type HTTPSenderOptions struct {
	// Addr is the base webhook URL. The event query parameter is added per request.
	Addr string
	// Timeout bounds one outbound HTTP request attempt.
	Timeout time.Duration
	// SigningSecret enables HMAC-SHA256 request signing when non-empty.
	SigningSecret string
	// Client optionally supplies a shared HTTP client for tests or custom transports.
	Client *http.Client
}

// HTTPSender posts JSON webhook requests to one configured endpoint.
type HTTPSender struct {
	addr          string
	timeout       time.Duration
	signingSecret string
	client        *http.Client
}

// NewHTTPSender creates an HTTP webhook sender.
func NewHTTPSender(opts HTTPSenderOptions) *HTTPSender {
	client := opts.Client
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPSender{addr: opts.Addr, timeout: opts.Timeout, signingSecret: opts.SigningSecret, client: client}
}

// Send posts the encoded webhook body as JSON. Only HTTP 200 is classified as success.
func (s *HTTPSender) Send(ctx context.Context, req SendRequest) error {
	if s == nil || s.addr == "" {
		return fmt.Errorf("webhook: http addr is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	target, err := url.Parse(s.addr)
	if err != nil {
		return err
	}
	query := target.Query()
	query.Set("event", req.Event)
	target.RawQuery = query.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(req.Body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if s.signingSecret != "" {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		requestID := req.RequestID
		if requestID == "" {
			requestID, err = newRequestID()
			if err != nil {
				return fmt.Errorf("webhook: generate request id: %w", err)
			}
		}
		signature := signRequest(s.signingSecret, timestamp, requestID, req.Body)
		httpReq.Header.Set("X-WuKongIM-Timestamp", timestamp)
		httpReq.Header.Set("X-WuKongIM-Request-ID", requestID)
		httpReq.Header.Set("X-WuKongIM-Signature", "sha256="+signature)
	}
	resp, err := s.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook: http status %s", strconv.Itoa(resp.StatusCode))
	}
	return nil
}

func newRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func signRequest(secret, timestamp, requestID string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write([]byte(requestID))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
