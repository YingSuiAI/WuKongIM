package webhook

import (
	"context"
	"time"
)

const (
	// EventMsgNotify reports committed durable messages.
	EventMsgNotify = "msg.notify"
	// EventMsgOffline reports offline recipients for one committed message chunk.
	EventMsgOffline = "msg.offline"
	// EventPresenceLease reports typed installation-level connection lease transitions.
	EventPresenceLease = "presence.lease.v1"
)

// Message is the event-neutral committed message shape accepted by the webhook runtime.
type Message struct {
	// MessageID is the globally unique durable message identifier.
	MessageID uint64
	// MessageSeq is the channel-local sequence assigned to the committed message.
	MessageSeq uint64
	// ChannelID identifies the channel that produced the committed message.
	ChannelID string
	// ChannelType is the WuKong channel type for ChannelID.
	ChannelType uint8
	// Setting carries legacy message setting bits when available from the append path.
	Setting uint8
	// Topic is the optional legacy message topic.
	Topic string
	// Expire is the legacy message expiration value.
	Expire uint32
	// SourceID is the node-local source identifier used by offline notifications.
	SourceID uint64
	// FromUID is the sender user ID.
	FromUID string
	// ClientMsgNo is the client idempotency key associated with the send request.
	ClientMsgNo string
	// ServerTimestampMS is the server timestamp in Unix milliseconds.
	ServerTimestampMS int64
	// Payload is the committed message payload.
	Payload []byte
	// RedDot carries the client red-dot flag.
	RedDot bool
	// SyncOnce carries the client sync-once flag.
	SyncOnce bool
}

// OfflineMessage carries one committed message plus one bounded recipient UID chunk.
type OfflineMessage struct {
	// Message is the committed message being reported offline.
	Message Message
	// ToUIDs is one bounded recipient UID batch for the offline message.
	ToUIDs []string
}

// PresenceLease is the typed installation-level webhook body.
type PresenceLease struct {
	PrincipalUID           string `json:"principal_uid"`
	TransportUID           string `json:"transport_uid"`
	AppInstanceID          string `json:"app_instance_id"`
	DeviceSessionID        string `json:"device_session_id"`
	IMSessionID            string `json:"im_session_id"`
	InstallationGeneration uint64 `json:"installation_generation"`
	DeviceID               string `json:"device_id"`
	DeviceClass            string `json:"device_class"`
	DeviceFlag             uint8  `json:"device_flag"`
	SessionGeneration      uint64 `json:"session_generation"`
	AuthorizationFence     uint64 `json:"authorization_fence"`
	OwnerNodeID            uint64 `json:"owner_node_id"`
	OwnerBootID            uint64 `json:"owner_boot_id"`
	OwnerSeq               uint64 `json:"owner_seq"`
	SessionID              uint64 `json:"session_id"`
	ConnectionID           string `json:"connection_id"`
	Kind                   string `json:"kind"`
	ObservedAt             int64  `json:"observed_at"`
	ExpiresAt              int64  `json:"expires_at"`
}

// SendRequest is one encoded webhook request.
type SendRequest struct {
	// Event is the webhook event name sent as the event query parameter.
	Event string
	// RequestID identifies one logical webhook batch across retry attempts.
	RequestID string
	// Body is the already encoded JSON request body.
	Body []byte
}

// Sender delivers one encoded webhook request to an external endpoint.
type Sender interface {
	Send(context.Context, SendRequest) error
}

// Observer receives structured webhook runtime observations.
type Observer interface {
	ObserveWebhook(Observation)
}

// Observation describes one webhook admission, send, retry, or drop event.
type Observation struct {
	// Queue identifies the runtime queue that produced the observation.
	Queue string
	// Event is the webhook event name.
	Event string
	// RequestID identifies one logical webhook request across its retry attempts.
	RequestID string
	// ChannelID identifies the channel when every item in the request belongs to one channel.
	ChannelID string
	// ChannelType is the type paired with ChannelID.
	ChannelType uint8
	// MessageID identifies the message when the request carries exactly one message.
	MessageID uint64
	// MessageSeq is the channel-local sequence paired with MessageID.
	MessageSeq uint64
	// Result is a bounded outcome label.
	Result string
	// Items reports the number of logical items in this observation.
	Items int
	// QueueDepth is the queue depth observed after the operation.
	QueueDepth int
	// QueueSize is the configured queue capacity.
	QueueSize int
	// Attempt is the one-based send attempt number when applicable.
	Attempt int
	// Duration is the measured send or admission duration when applicable.
	Duration time.Duration
	// Err carries the original error for observer-side classification. Observers must not serialize it directly.
	Err error
}
