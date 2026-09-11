package messageadmission

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	imadmission "github.com/YingSuiAI/centerim-contracts/generated/imadmission-go"
	"github.com/google/uuid"
)

const (
	admissionPath    = "/internal/im/message-admissions"
	admissionDomain  = "im.message.admission.v1"
	maximumBodyBytes = 512 * 1024
	// Agent source envelopes and canonical message.committed envelopes include
	// metadata around the independently bounded 256 KiB snapshot text. Platform
	// applies each source type's own limit; this transport must carry that envelope.
	maximumSourcePayloadBytes   = 320 * 1024
	maximumAdmittedPayloadBytes = 320 * 1024
)

var (
	// ErrUnavailable exposes no URL, transport diagnostic, response body, or credential.
	ErrUnavailable = errors.New("message admission: authority unavailable")
	// ErrInvalidConfiguration reports a fixed endpoint or secret configuration error.
	ErrInvalidConfiguration = errors.New("message admission: invalid fixed endpoint, signing secret, or timeout")
)

// Options configure a deployment-owned endpoint. HTTP is for an already trusted private service
// network only; HTTPS uses normal certificate validation. Redirects are never followed.
type Options struct {
	// URL is the fixed internal admission endpoint; no per-message destination is accepted.
	URL string
	// SigningSecret authenticates requests in the admission-only HMAC domain and is never logged.
	SigningSecret string
	// Timeout bounds the request, unless its caller already has an earlier deadline.
	Timeout time.Duration
	// Client may supply a shared transport, including a test server's trusted TLS transport.
	Client *http.Client
}

// Client owns one bounded, domain-separated mandatory admission request per SEND attempt.
// It never retries HTTP internally: the sender's same-body SEND retry owns idempotent recovery.
type Client struct {
	url     string
	secret  []byte
	timeout time.Duration
	http    *http.Client
}

// New validates the fixed deployment authority and constructs a redirect-free client.
func New(opts Options) (*Client, error) {
	target, err := url.Parse(opts.URL)
	if err != nil || target == nil || strings.TrimSpace(opts.URL) != opts.URL || target.Host == "" ||
		(target.Scheme != "http" && target.Scheme != "https") || target.User != nil || target.Path != admissionPath ||
		target.RawQuery != "" || target.ForceQuery || target.Fragment != "" || target.RawPath != "" ||
		strings.TrimSpace(opts.SigningSecret) == "" || opts.Timeout < 0 {
		return nil, ErrInvalidConfiguration
	}
	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Second
	}
	client := http.Client{}
	if opts.Client != nil {
		client = *opts.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{url: target.String(), secret: []byte(opts.SigningSecret), timeout: opts.Timeout, http: &client}, nil
}

// AdmitSend derives origin from verified entry metadata and returns only server-canonical bytes.
func (c *Client) AdmitSend(ctx context.Context, cmd message.SendCommand) ([]byte, message.Reason, error) {
	if c == nil || c.http == nil {
		return nil, message.ReasonSystemError, ErrUnavailable
	}
	if cmd.NoPersist || cmd.SyncOnce || len(cmd.Payload) == 0 || len(cmd.Payload) > maximumSourcePayloadBytes {
		return nil, message.ReasonInvalidRequest, nil
	}
	request := imadmission.ImMessageAdmissionRequest{
		FromUid: cmd.FromUID, ChannelId: cmd.ChannelID, ChannelType: int(cmd.ChannelType),
		ClientMsgNo: cmd.ClientMsgNo, Payload: cmd.Payload,
	}
	switch {
	case cmd.SenderSessionID != 0 && !cmd.ServiceAuthenticated:
		id, err := uuid.Parse(cmd.IMSessionID)
		if err != nil || id == uuid.Nil {
			return nil, message.ReasonAuthFail, nil
		}
		request.Origin = imadmission.Device
		request.ImSessionId = &id
	case cmd.ServiceAuthenticated && cmd.SenderSessionID == 0 && cmd.IMSessionID == "":
		request.Origin = imadmission.Service
	default:
		return nil, message.ReasonAuthFail, nil
	}
	body, err := json.Marshal(request)
	if err != nil || len(body) > maximumBodyBytes {
		return nil, message.ReasonInvalidRequest, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, message.ReasonSystemError, ErrUnavailable
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, message.ReasonSystemError, ErrUnavailable
	}
	requestID := hex.EncodeToString(nonce[:])
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-IM-Request-ID", requestID)
	httpRequest.Header.Set("X-IM-Timestamp", timestamp)
	httpRequest.Header.Set("X-IM-Signature", sign(c.secret, timestamp, requestID, body))
	response, err := c.http.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return nil, message.ReasonSystemError, ctx.Err()
		}
		return nil, message.ReasonSystemError, ErrUnavailable
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maximumBodyBytes+1))
	if err != nil || len(responseBody) > maximumBodyBytes {
		if ctx.Err() != nil {
			return nil, message.ReasonSystemError, ctx.Err()
		}
		return nil, message.ReasonSystemError, fmt.Errorf("%w: bounded response status=%d", ErrUnavailable, response.StatusCode)
	}
	if response.StatusCode != http.StatusOK {
		var rejected imadmission.ImMessageAdmissionError
		if decodeStrict(responseBody, &rejected) != nil {
			return nil, message.ReasonSystemError, fmt.Errorf("%w: invalid response status=%d", ErrUnavailable, response.StatusCode)
		}
		switch {
		case response.StatusCode == http.StatusBadRequest && rejected.Code == imadmission.INVALIDREQUEST,
			response.StatusCode == http.StatusConflict && rejected.Code == imadmission.IDEMPOTENCYCONFLICT:
			return nil, message.ReasonInvalidRequest, nil
		case response.StatusCode == http.StatusForbidden && rejected.Code == imadmission.FORBIDDEN:
			return nil, message.ReasonNotAllowSend, nil
		case response.StatusCode == http.StatusUnauthorized && rejected.Code == imadmission.UNAUTHENTICATED:
			return nil, message.ReasonAuthFail, nil
		default:
			return nil, message.ReasonSystemError, fmt.Errorf("%w: status=%d", ErrUnavailable, response.StatusCode)
		}
	}
	var admitted imadmission.ImMessageAdmissionResponse
	if decodeStrict(responseBody, &admitted) != nil || len(admitted.Payload) == 0 || len(admitted.Payload) > maximumAdmittedPayloadBytes {
		return nil, message.ReasonSystemError, fmt.Errorf("%w: invalid success response", ErrUnavailable)
	}
	if _, err := (EnvelopeIdentityReader{}).ReadApplicationMessageID(admitted.Payload); err != nil {
		return nil, message.ReasonSystemError, fmt.Errorf("%w: invalid admitted identity", ErrUnavailable)
	}
	return admitted.Payload, message.ReasonSuccess, nil
}

func decodeStrict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return ErrUnavailable
	}
	return nil
}

func sign(secret []byte, timestamp, requestID string, body []byte) string {
	digest := sha256.Sum256(body)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(admissionDomain + "\n" + timestamp + "\n" + requestID + "\n" + hex.EncodeToString(digest[:])))
	return hex.EncodeToString(mac.Sum(nil))
}
