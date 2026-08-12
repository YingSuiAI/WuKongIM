package message

import metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"

const (
	// EventTypeOpen starts an open event lane.
	EventTypeOpen = metadb.EventTypeOpen
	// EventTypeDelta appends a delta to an open event lane.
	EventTypeDelta = metadb.EventTypeDelta
	// EventTypeSnapshot replaces the compact event lane snapshot.
	EventTypeSnapshot = metadb.EventTypeSnapshot
	// EventTypeFinish closes the complete run.
	EventTypeFinish = metadb.EventTypeFinish

	// EventStatusOpen reports an active event lane.
	EventStatusOpen = metadb.EventStatusOpen
	// EventStatusClosed reports a completed event lane.
	EventStatusClosed = metadb.EventStatusClosed
	// EventStatusError reports an errored event lane.
	EventStatusError = metadb.EventStatusError
	// EventStatusCancelled reports a cancelled event lane.
	EventStatusCancelled = metadb.EventStatusCancelled

	// EventKeyDefault is the default event lane key.
	EventKeyDefault = metadb.EventKeyDefault
	// VisibilityPublic exposes event state to ordinary sync readers.
	VisibilityPublic = metadb.VisibilityPublic
	// VisibilityPrivate keeps event state scoped to the sender/owner.
	VisibilityPrivate = metadb.VisibilityPrivate
	// VisibilityRestricted keeps event state behind entry-specific policy.
	VisibilityRestricted = metadb.VisibilityRestricted
)

// MessageEventAnchor is the committed base message an event is allowed to project.
type MessageEventAnchor struct {
	ChannelID          string
	ChannelType        int64
	FromUID            string
	MessageID          uint64
	ClientMsgNo        string
	RunID              string
	AuthorizationFence string
}

// MessageEventAppend describes one message event projection update.
type MessageEventAppend struct {
	// ChannelID identifies the channel that owns the message.
	ChannelID string
	// ChannelType identifies the channel namespace.
	ChannelType int64
	// FromUID identifies the sender used for person/agent channel normalization.
	FromUID string
	// MessageID optionally carries the base message id for entry responses.
	MessageID uint64
	// ClientMsgNo identifies the message inside the channel.
	ClientMsgNo        string
	RunID              string
	AuthorizationFence string
	// AuthoritySequence is the Platform ledger's anchor/run-global sequence.
	AuthoritySequence uint64
	// EventID is the idempotency key for this event lane.
	EventID string
	// EventKey identifies the projected event lane for this message.
	EventKey string
	// EventType identifies the reducer transition to apply.
	EventType string
	// Visibility describes who can read the projected event state.
	Visibility string
	// OccurredAt records when the source event happened.
	OccurredAt int64
	// Payload stores the reducer payload for this event.
	Payload []byte
	// UpdatedAt records when the projection update was created.
	UpdatedAt int64
}

// MessageEventAppendResult reports the durable state after applying an event.
type MessageEventAppendResult struct {
	// Applied reports whether this call created a new projection transition.
	Applied bool
	// ChannelID identifies the channel that owns the message.
	ChannelID string
	// ChannelType identifies the channel namespace.
	ChannelType int64
	// FromUID identifies the sender carried by the append command.
	FromUID string
	// MessageID optionally carries the base message id from the append command.
	MessageID uint64
	// ClientMsgNo identifies the message inside the channel.
	ClientMsgNo string
	// RunID identifies the Agent run that owns this lane.
	RunID string
	// EventID is the applied or idempotently observed event id.
	EventID string
	// EventKey identifies the projected event lane for this message.
	EventKey string
	// MsgEventSeq is the per-message event sequence after the append.
	MsgEventSeq uint64
	// Status is the projected lane status after the append.
	Status string
	// State is the full projected lane state after the append.
	State MessageEventState
}

// MessageEventState stores one compact message event lane projection.
type MessageEventState struct {
	// ChannelID identifies the channel that owns the message.
	ChannelID string
	// ChannelType identifies the channel namespace.
	ChannelType int64
	// ClientMsgNo identifies the message inside the channel.
	ClientMsgNo string
	// RunID identifies the Agent run that owns this lane.
	RunID string
	// EventKey identifies the projected event lane for this message.
	EventKey string
	// Status records whether the event lane is open or terminal.
	Status string
	// LastMsgEventSeq is the latest per-message event sequence applied to this lane.
	LastMsgEventSeq uint64
	// LastAuthoritySequence is the Platform ledger watermark represented by this lane.
	LastAuthoritySequence uint64
	// LastEventID is the latest idempotency key applied to this lane.
	LastEventID string
	// LastEventType is the latest event type applied to this lane.
	LastEventType string
	// LastVisibility is the latest visibility associated with this lane.
	LastVisibility string
	// LastOccurredAt records when the latest source event happened.
	LastOccurredAt int64
	// SnapshotPayload stores the compact projected lane payload.
	SnapshotPayload []byte
	// EndReason stores the terminal close reason when provided.
	EndReason uint8
	// Error stores the terminal error message when provided.
	Error string
	// UpdatedAt records when this projection row was last updated.
	UpdatedAt int64
}

// MessageEventMessageKey identifies all event lanes for one message.
type MessageEventMessageKey struct {
	// ChannelID identifies the channel that owns the message.
	ChannelID string
	// ChannelType identifies the channel namespace.
	ChannelType int64
	// ClientMsgNo identifies the message inside the channel.
	ClientMsgNo string
}

// MessageEventMeta is the compact message event summary attached to a synced message.
type MessageEventMeta struct {
	// HasEvents reports whether the message has compact event lane states.
	HasEvents bool
	// Completed reports whether the reserved finish lane has been observed.
	Completed bool
	// EventVersion mirrors LastMsgEventSeq for compatible clients.
	EventVersion uint64
	// LastMsgEventSeq is the greatest message-level event sequence in returned lanes.
	LastMsgEventSeq uint64
	// EventCount is the number of non-finish lanes in Events.
	EventCount int
	// OpenEventCount is the number of non-finish lanes still open.
	OpenEventCount int
	// Events contains compact per-lane state in event-key order.
	Events []MessageEventKeyMeta
}

// MessageEventKeyMeta is one compact event lane summary.
type MessageEventKeyMeta struct {
	// EventKey identifies the lane.
	EventKey string
	// Status is the lane reducer status.
	Status string
	// LastMsgEventSeq is the latest message-level event sequence applied to this lane.
	LastMsgEventSeq uint64
	// EndReason stores the terminal close reason when provided.
	EndReason uint8
	// Error stores the terminal error message when provided.
	Error string
	// Snapshot optionally contains the decoded or raw snapshot in full summary mode.
	Snapshot any
}
