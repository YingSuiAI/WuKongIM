package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/engine"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/schema"
)

const (
	EventTypeOpen     = "open"
	EventTypeDelta    = "delta"
	EventTypeSnapshot = "snapshot"
	EventTypeFinish   = "finish"

	EventStatusOpen      = "open"
	EventStatusClosed    = "closed"
	EventStatusError     = "error"
	EventStatusCancelled = "cancelled"

	EventKeyDefault = "main"

	VisibilityPublic     = "public"
	VisibilityPrivate    = "private"
	VisibilityRestricted = "restricted"

	SnapshotKindText = "text"
)

const (
	messageEventStateColumnChannelID       uint16 = 1
	messageEventStateColumnChannelType     uint16 = 2
	messageEventStateColumnClientMsgNo     uint16 = 3
	messageEventStateColumnEventKey        uint16 = 4
	messageEventStateColumnStatus          uint16 = 5
	messageEventStateColumnLastSeq         uint16 = 6
	messageEventStateColumnLastEventID     uint16 = 7
	messageEventStateColumnLastEventType   uint16 = 8
	messageEventStateColumnLastVisibility  uint16 = 9
	messageEventStateColumnLastOccurredAt  uint16 = 10
	messageEventStateColumnSnapshotPayload uint16 = 11
	messageEventStateColumnEndReason       uint16 = 12
	messageEventStateColumnError           uint16 = 13
	messageEventStateColumnUpdatedAt       uint16 = 14
	messageEventStateColumnRunID           uint16 = 15

	messageEventCursorColumnChannelID   uint16 = 1
	messageEventCursorColumnChannelType uint16 = 2
	messageEventCursorColumnClientMsgNo uint16 = 3
	messageEventCursorColumnLastSeq     uint16 = 4
	messageEventCursorColumnUpdatedAt   uint16 = 5
	messageEventCursorColumnRunID       uint16 = 6
	messageEventCursorColumnTerminal    uint16 = 7

	messageEventAppliedColumnChannelID          uint16 = 1
	messageEventAppliedColumnChannelType        uint16 = 2
	messageEventAppliedColumnClientMsgNo        uint16 = 3
	messageEventAppliedColumnEventID            uint16 = 4
	messageEventAppliedColumnEventKey           uint16 = 5
	messageEventAppliedColumnMsgSeq             uint16 = 6
	messageEventAppliedColumnStatus             uint16 = 7
	messageEventAppliedColumnUpdatedAt          uint16 = 8
	messageEventAppliedColumnRunID              uint16 = 9
	messageEventAppliedColumnAuthorizationFence uint16 = 10
	messageEventAppliedColumnProjectionOnly     uint16 = 11
	messageEventAppliedColumnEventType          uint16 = 12
	messageEventAppliedColumnVisibility         uint16 = 13
	messageEventAppliedColumnOccurredAt         uint16 = 14
	messageEventAppliedColumnPayload            uint16 = 15
)

var messageEventStateTable = registerMetaTable(TableSpec[MessageEventState]{
	ID:   TableIDMessageEventState,
	Name: "message_event_state",
	Columns: []schema.Column{
		{ID: messageEventStateColumnChannelID, Name: "channel_id", Type: schema.TypeString, Required: true},
		{ID: messageEventStateColumnChannelType, Name: "channel_type", Type: schema.TypeInt64, Required: true},
		{ID: messageEventStateColumnClientMsgNo, Name: "client_msg_no", Type: schema.TypeString, Required: true},
		{ID: messageEventStateColumnEventKey, Name: "event_key", Type: schema.TypeString, Required: true},
		{ID: messageEventStateColumnRunID, Name: "run_id", Type: schema.TypeString, Required: true},
		{ID: messageEventStateColumnStatus, Name: "status", Type: schema.TypeString},
		{ID: messageEventStateColumnLastSeq, Name: "last_msg_event_seq", Type: schema.TypeUint64},
		{ID: messageEventStateColumnLastEventID, Name: "last_event_id", Type: schema.TypeString},
		{ID: messageEventStateColumnLastEventType, Name: "last_event_type", Type: schema.TypeString},
		{ID: messageEventStateColumnLastVisibility, Name: "last_visibility", Type: schema.TypeString},
		{ID: messageEventStateColumnLastOccurredAt, Name: "last_occurred_at", Type: schema.TypeInt64},
		{ID: messageEventStateColumnSnapshotPayload, Name: "snapshot_payload", Type: schema.TypeBytes},
		{ID: messageEventStateColumnEndReason, Name: "end_reason", Type: schema.TypeUint8},
		{ID: messageEventStateColumnError, Name: "error", Type: schema.TypeString},
		{ID: messageEventStateColumnUpdatedAt, Name: "updated_at", Type: schema.TypeInt64},
	},
	Families: []schema.Family{{ID: messageEventStatePrimaryFamilyID, Name: "primary", Columns: []uint16{
		messageEventStateColumnStatus,
		messageEventStateColumnLastSeq,
		messageEventStateColumnLastEventID,
		messageEventStateColumnLastEventType,
		messageEventStateColumnLastVisibility,
		messageEventStateColumnLastOccurredAt,
		messageEventStateColumnSnapshotPayload,
		messageEventStateColumnEndReason,
		messageEventStateColumnError,
		messageEventStateColumnUpdatedAt,
	}}},
	Primary: PrimarySpec[MessageEventState]{
		IndexID:  messageEventStatePrimaryIndexID,
		FamilyID: messageEventStatePrimaryFamilyID,
		Name:     "pk_message_event_state",
		Columns: []uint16{
			messageEventStateColumnChannelID,
			messageEventStateColumnChannelType,
			messageEventStateColumnClientMsgNo,
			messageEventStateColumnRunID,
			messageEventStateColumnEventKey,
		},
		Layout: KeyLayout{KeyString, KeyInt64Ordered, KeyString, KeyString, KeyString},
		Key: func(state MessageEventState) KeyParts {
			return messageEventStatePrimaryKey(state.ChannelID, state.ChannelType, state.ClientMsgNo, state.RunID, state.EventKey)
		},
	},
	Validate: validateMessageEventState,
	EncodeValue: func(state MessageEventState) ([]byte, error) {
		return encodeMessageEventStateValue(state), nil
	},
	DecodeValue: func(primary KeyParts, value []byte) (MessageEventState, error) {
		return decodeMessageEventStateValue(primary[0].S, primary[1].I64, primary[2].S, primary[3].S, primary[4].S, value)
	},
})

var messageEventCursorTable = registerMetaTable(TableSpec[MessageEventCursor]{
	ID:   TableIDMessageEventCursor,
	Name: "message_event_cursor",
	Columns: []schema.Column{
		{ID: messageEventCursorColumnChannelID, Name: "channel_id", Type: schema.TypeString, Required: true},
		{ID: messageEventCursorColumnChannelType, Name: "channel_type", Type: schema.TypeInt64, Required: true},
		{ID: messageEventCursorColumnClientMsgNo, Name: "client_msg_no", Type: schema.TypeString, Required: true},
		{ID: messageEventCursorColumnLastSeq, Name: "last_msg_event_seq", Type: schema.TypeUint64},
		{ID: messageEventCursorColumnUpdatedAt, Name: "updated_at", Type: schema.TypeInt64},
		{ID: messageEventCursorColumnRunID, Name: "run_id", Type: schema.TypeString, Required: true},
		{ID: messageEventCursorColumnTerminal, Name: "terminal", Type: schema.TypeUint8},
	},
	Families: []schema.Family{{ID: messageEventCursorPrimaryFamilyID, Name: "primary", Columns: []uint16{
		messageEventCursorColumnLastSeq,
		messageEventCursorColumnUpdatedAt,
		messageEventCursorColumnTerminal,
	}}},
	Primary: PrimarySpec[MessageEventCursor]{
		IndexID:  messageEventCursorPrimaryIndexID,
		FamilyID: messageEventCursorPrimaryFamilyID,
		Name:     "pk_message_event_cursor",
		Columns: []uint16{
			messageEventCursorColumnChannelID,
			messageEventCursorColumnChannelType,
			messageEventCursorColumnClientMsgNo,
			messageEventCursorColumnRunID,
		},
		Layout: KeyLayout{KeyString, KeyInt64Ordered, KeyString, KeyString},
		Key: func(cursor MessageEventCursor) KeyParts {
			return messageEventCursorPrimaryKey(cursor.ChannelID, cursor.ChannelType, cursor.ClientMsgNo, cursor.RunID)
		},
	},
	Validate: validateMessageEventCursor,
	EncodeValue: func(cursor MessageEventCursor) ([]byte, error) {
		return encodeMessageEventCursorValue(cursor), nil
	},
	DecodeValue: func(primary KeyParts, value []byte) (MessageEventCursor, error) {
		return decodeMessageEventCursorValue(primary[0].S, primary[1].I64, primary[2].S, primary[3].S, value)
	},
})

var messageEventAppliedTable = registerMetaTable(TableSpec[MessageEventApplied]{
	ID:   TableIDMessageEventApplied,
	Name: "message_event_applied",
	Columns: []schema.Column{
		{ID: messageEventAppliedColumnChannelID, Name: "channel_id", Type: schema.TypeString, Required: true},
		{ID: messageEventAppliedColumnChannelType, Name: "channel_type", Type: schema.TypeInt64, Required: true},
		{ID: messageEventAppliedColumnClientMsgNo, Name: "client_msg_no", Type: schema.TypeString, Required: true},
		{ID: messageEventAppliedColumnEventID, Name: "event_id", Type: schema.TypeString, Required: true},
		{ID: messageEventAppliedColumnEventKey, Name: "event_key", Type: schema.TypeString},
		{ID: messageEventAppliedColumnMsgSeq, Name: "msg_event_seq", Type: schema.TypeUint64},
		{ID: messageEventAppliedColumnStatus, Name: "status", Type: schema.TypeString},
		{ID: messageEventAppliedColumnUpdatedAt, Name: "updated_at", Type: schema.TypeInt64},
		{ID: messageEventAppliedColumnRunID, Name: "run_id", Type: schema.TypeString},
		{ID: messageEventAppliedColumnAuthorizationFence, Name: "authorization_fence", Type: schema.TypeString},
		{ID: messageEventAppliedColumnProjectionOnly, Name: "projection_only", Type: schema.TypeBool},
		{ID: messageEventAppliedColumnEventType, Name: "event_type", Type: schema.TypeString},
		{ID: messageEventAppliedColumnVisibility, Name: "visibility", Type: schema.TypeString},
		{ID: messageEventAppliedColumnOccurredAt, Name: "occurred_at", Type: schema.TypeInt64},
		{ID: messageEventAppliedColumnPayload, Name: "payload", Type: schema.TypeBytes},
	},
	Families: []schema.Family{{ID: messageEventAppliedPrimaryFamilyID, Name: "primary", Columns: []uint16{
		messageEventAppliedColumnEventKey,
		messageEventAppliedColumnMsgSeq,
		messageEventAppliedColumnStatus,
		messageEventAppliedColumnUpdatedAt,
		messageEventAppliedColumnRunID,
		messageEventAppliedColumnAuthorizationFence,
		messageEventAppliedColumnProjectionOnly,
		messageEventAppliedColumnEventType,
		messageEventAppliedColumnVisibility,
		messageEventAppliedColumnOccurredAt,
		messageEventAppliedColumnPayload,
	}}},
	Primary: PrimarySpec[MessageEventApplied]{
		IndexID:  messageEventAppliedPrimaryIndexID,
		FamilyID: messageEventAppliedPrimaryFamilyID,
		Name:     "pk_message_event_applied",
		Columns: []uint16{
			messageEventAppliedColumnChannelID,
			messageEventAppliedColumnChannelType,
			messageEventAppliedColumnClientMsgNo,
			messageEventAppliedColumnEventID,
		},
		Layout: KeyLayout{KeyString, KeyInt64Ordered, KeyString, KeyString},
		Key: func(applied MessageEventApplied) KeyParts {
			return messageEventAppliedPrimaryKey(applied.ChannelID, applied.ChannelType, applied.ClientMsgNo, applied.EventID)
		},
	},
	Validate: validateMessageEventApplied,
	EncodeValue: func(applied MessageEventApplied) ([]byte, error) {
		return encodeMessageEventAppliedValue(applied), nil
	},
	DecodeValue: func(primary KeyParts, value []byte) (MessageEventApplied, error) {
		return decodeMessageEventAppliedValue(primary[0].S, primary[1].I64, primary[2].S, primary[3].S, value)
	},
})

// MessageEventStateTable describes the message event state table schema.
var MessageEventStateTable = messageEventStateTable.Schema()

// MessageEventCursorTable describes the message event cursor table schema.
var MessageEventCursorTable = messageEventCursorTable.Schema()

// MessageEventAppliedTable describes the message event idempotency table schema.
var MessageEventAppliedTable = messageEventAppliedTable.Schema()

// GetMessageEventState returns one projected message event lane.
func (s *Shard) GetMessageEventState(ctx context.Context, channelID string, channelType int64, clientMsgNo, runID, eventKey string) (MessageEventState, bool, error) {
	if err := s.check(ctx); err != nil {
		return MessageEventState{}, false, err
	}
	channelID, clientMsgNo, runID, eventKey, err := normalizeMessageEventStateKey(channelID, channelType, clientMsgNo, runID, eventKey)
	if err != nil {
		return MessageEventState{}, false, err
	}
	state, ok, err := messageEventStateTable.Get(ctx, s, messageEventStatePrimaryKey(channelID, channelType, clientMsgNo, runID, eventKey))
	if err != nil || !ok {
		return MessageEventState{}, ok, err
	}
	return cloneMessageEventState(state), true, nil
}

// ListMessageEventStates returns projected lanes for one message in event-key order.
func (s *Shard) ListMessageEventStates(ctx context.Context, channelID string, channelType int64, clientMsgNo string, limit int) ([]MessageEventState, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	channelID, clientMsgNo, err := normalizeMessageEventMessageKey(channelID, channelType, clientMsgNo)
	if err != nil {
		return nil, err
	}
	rows, _, _, err := messageEventStateTable.ScanPrimaryPrefix(ctx, s, KeyParts{String(channelID), Int64Ordered(channelType), String(clientMsgNo)}, nil, limit)
	if err != nil {
		return nil, err
	}
	out := make([]MessageEventState, 0, len(rows))
	for _, row := range rows {
		out = append(out, cloneMessageEventState(row))
	}
	return out, nil
}

// GetMessageEventCursor returns the run-global authority cursor for one anchor.
func (s *Shard) GetMessageEventCursor(ctx context.Context, channelID string, channelType int64, clientMsgNo, runID string) (MessageEventCursor, bool, error) {
	if err := s.check(ctx); err != nil {
		return MessageEventCursor{}, false, err
	}
	channelID, clientMsgNo, err := normalizeMessageEventMessageKey(channelID, channelType, clientMsgNo)
	if err != nil {
		return MessageEventCursor{}, false, err
	}
	runID = strings.TrimSpace(runID)
	if err := validateKeyString(runID); err != nil {
		return MessageEventCursor{}, false, err
	}
	return messageEventCursorTable.getByPrimaryKey(s.db, s.hashSlot, messageEventCursorPrimaryKey(channelID, channelType, clientMsgNo, runID))
}

// GetMessageEventApplied reads one durable event-id idempotency record.
func (s *Shard) GetMessageEventApplied(ctx context.Context, channelID string, channelType int64, clientMsgNo, eventID string) (MessageEventApplied, bool, error) {
	if err := s.check(ctx); err != nil {
		return MessageEventApplied{}, false, err
	}
	channelID = strings.TrimSpace(channelID)
	clientMsgNo = strings.TrimSpace(clientMsgNo)
	eventID = strings.TrimSpace(eventID)
	if channelType <= 0 || channelID == "" || clientMsgNo == "" || eventID == "" {
		return MessageEventApplied{}, false, dberrors.ErrInvalidArgument
	}
	return messageEventAppliedTable.getByPrimaryKey(s.db, s.hashSlot, messageEventAppliedPrimaryKey(channelID, channelType, clientMsgNo, eventID))
}

// AppendMessageEvent applies one message event projection update.
func (s *Shard) AppendMessageEvent(ctx context.Context, event MessageEventAppend) (MessageEventAppendResult, error) {
	if err := s.check(ctx); err != nil {
		return MessageEventAppendResult{}, err
	}
	event, err := normalizeMessageEventAppend(event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if event.ProjectionOnly {
		return MessageEventAppendResult{}, dberrors.ErrInvalidArgument
	}
	unlock := s.lock()
	defer unlock()

	appliedEvent, appliedExists, err := messageEventAppliedTable.getByPrimaryKey(s.db, s.hashSlot, messageEventAppliedPrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.EventID))
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if appliedExists {
		if !messageEventAppliedMatches(appliedEvent, event) {
			return MessageEventAppendResult{}, dberrors.ErrConflict
		}
		return s.messageEventAppendResultFromApplied(event, appliedEvent)
	}
	state, stateExists, err := messageEventStateTable.getByPrimaryKey(s.db, s.hashSlot, messageEventStatePrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, event.EventKey))
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	cursor, cursorExists, err := messageEventCursorTable.getByPrimaryKey(s.db, s.hashSlot, messageEventCursorPrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID))
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if cursorExists && cursor.Terminal {
		return MessageEventAppendResult{}, dberrors.ErrConflict
	}
	if !validMessageEventAuthoritySequence(cursor, cursorExists, event) {
		return MessageEventAppendResult{}, dberrors.ErrConflict
	}
	nextState, nextCursor, didApply, result := reduceMessageEventAppend(state, stateExists, cursor, cursorExists, event)
	if !didApply {
		return result, nil
	}

	stateKey, err := messageEventStateRowKey(s.hashSlot, nextState.ChannelID, nextState.ChannelType, nextState.ClientMsgNo, nextState.RunID, nextState.EventKey)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	cursorKey, err := messageEventCursorRowKey(s.hashSlot, nextCursor.ChannelID, nextCursor.ChannelType, nextCursor.ClientMsgNo, nextCursor.RunID)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	nextApplied := messageEventAppliedFromResult(event, result)
	appliedKey, err := messageEventAppliedRowKey(s.hashSlot, nextApplied.ChannelID, nextApplied.ChannelType, nextApplied.ClientMsgNo, nextApplied.EventID)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	batch := s.db.engine.NewBatch()
	defer batch.Close()
	if err := batch.Set(stateKey, encodeMessageEventStateValue(nextState)); err != nil {
		return MessageEventAppendResult{}, err
	}
	if err := batch.Set(cursorKey, encodeMessageEventCursorValue(nextCursor)); err != nil {
		return MessageEventAppendResult{}, err
	}
	if err := batch.Set(appliedKey, encodeMessageEventAppliedValue(nextApplied)); err != nil {
		return MessageEventAppendResult{}, err
	}
	if err := batch.Commit(true); err != nil {
		return MessageEventAppendResult{}, err
	}
	return result, nil
}

// AppendMessageEvent stages one message event projection update.
func (b *Batch) AppendMessageEvent(hashSlot HashSlot, event MessageEventAppend) (MessageEventAppendResult, error) {
	if err := b.ensureOpen(); err != nil {
		return MessageEventAppendResult{}, err
	}
	event, err := normalizeMessageEventAppend(event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	stateKey, err := messageEventStateRowKey(hashSlot, event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, event.EventKey)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	cursorKey, err := messageEventCursorRowKey(hashSlot, event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	appliedKey, err := messageEventAppliedRowKey(hashSlot, event.ChannelID, event.ChannelType, event.ClientMsgNo, event.EventID)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	appliedEvent, appliedExists, err := b.loadMessageEventAppliedForAppend(hashSlot, appliedKey, event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if appliedExists {
		if !messageEventAppliedMatches(appliedEvent, event) {
			return MessageEventAppendResult{}, dberrors.ErrConflict
		}
		return b.messageEventAppendResultFromApplied(hashSlot, event, appliedEvent)
	}
	if event.ProjectionOnly {
		return b.appendMessageEventProjection(hashSlot, stateKey, event)
	}
	state, stateExists, err := b.loadMessageEventStateForAppend(hashSlot, stateKey, event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	cursor, cursorExists, err := b.loadMessageEventCursorForAppend(hashSlot, cursorKey, event)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	if !validMessageEventAuthoritySequence(cursor, cursorExists, event) {
		return MessageEventAppendResult{}, dberrors.ErrConflict
	}
	if cursorExists && cursor.Terminal {
		return MessageEventAppendResult{}, dberrors.ErrConflict
	}
	baseState := cloneMessageEventState(state)
	baseCursor := cursor
	baseApplied := appliedEvent
	baseAppliedExists := appliedExists
	nextState, nextCursor, didApply, result := reduceMessageEventAppend(state, stateExists, cursor, cursorExists, event)
	if !didApply {
		return result, nil
	}
	if b.messageEventStates == nil {
		b.messageEventStates = make(map[string]MessageEventState)
	}
	if b.messageEventCursors == nil {
		b.messageEventCursors = make(map[string]MessageEventCursor)
	}
	if b.messageEventApplied == nil {
		b.messageEventApplied = make(map[string]MessageEventApplied)
	}
	b.messageEventStates[string(stateKey)] = cloneMessageEventState(nextState)
	b.messageEventCursors[string(cursorKey)] = nextCursor
	nextApplied := messageEventAppliedFromResult(event, result)
	b.messageEventApplied[string(appliedKey)] = nextApplied

	stateValue := encodeMessageEventStateValue(nextState)
	cursorValue := encodeMessageEventCursorValue(nextCursor)
	appliedValue := encodeMessageEventAppliedValue(nextApplied)
	b.addOp(hashSlot, func(ctx context.Context, state *batchCommitState, batch *engine.Batch) error {
		currentApplied, currentAppliedExists, err := messageEventAppliedTable.loadBatchRow(state, hashSlot, messageEventAppliedPrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.EventID), appliedKey)
		if err != nil {
			return err
		}
		if !messageEventAppliedEqual(currentApplied, currentAppliedExists, baseApplied, baseAppliedExists) {
			return dberrors.ErrConflict
		}
		currentState, currentStateExists, err := messageEventStateTable.loadBatchRow(state, hashSlot, messageEventStatePrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, event.EventKey), stateKey)
		if err != nil {
			return err
		}
		if !messageEventStateEqual(currentState, currentStateExists, baseState, stateExists) {
			return dberrors.ErrConflict
		}
		currentCursor, currentCursorExists, err := messageEventCursorTable.loadBatchRow(state, hashSlot, messageEventCursorPrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID), cursorKey)
		if err != nil {
			return err
		}
		if !messageEventCursorEqual(currentCursor, currentCursorExists, baseCursor, cursorExists) {
			return dberrors.ErrConflict
		}
		if err := batch.Set(stateKey, stateValue); err != nil {
			return err
		}
		if err := batch.Set(cursorKey, cursorValue); err != nil {
			return err
		}
		if err := batch.Set(appliedKey, appliedValue); err != nil {
			return err
		}
		state.tableRows[string(stateKey)] = tableRowOverlay{value: append([]byte(nil), stateValue...), exists: true}
		state.tableRows[string(cursorKey)] = tableRowOverlay{value: append([]byte(nil), cursorValue...), exists: true}
		state.tableRows[string(appliedKey)] = tableRowOverlay{value: append([]byte(nil), appliedValue...), exists: true}
		return nil
	})
	return result, nil
}

func (b *Batch) appendMessageEventProjection(hashSlot HashSlot, stateKey []byte, event MessageEventAppend) (MessageEventAppendResult, error) {
	nextState := MessageEventState{
		ChannelID: event.ChannelID, ChannelType: event.ChannelType, ClientMsgNo: event.ClientMsgNo,
		RunID: event.RunID, EventKey: event.EventKey, Status: EventStatusClosed, LastMsgEventSeq: event.MsgEventSeq,
		LastAuthoritySequence: event.AuthoritySequence,
		LastEventID:           event.EventID, LastEventType: EventTypeFinish, LastVisibility: event.Visibility,
		LastOccurredAt: event.OccurredAt, SnapshotPayload: cloneBytes(event.Payload), UpdatedAt: event.UpdatedAt,
	}
	if b.messageEventStates == nil {
		b.messageEventStates = make(map[string]MessageEventState)
	}
	b.messageEventStates[string(stateKey)] = cloneMessageEventState(nextState)
	stateValue := encodeMessageEventStateValue(nextState)
	b.addOp(hashSlot, func(ctx context.Context, commitState *batchCommitState, batch *engine.Batch) error {
		current, exists, err := messageEventStateTable.loadBatchRow(commitState, hashSlot, messageEventStatePrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, event.EventKey), stateKey)
		if err != nil {
			return err
		}
		if exists && messageEventStateEqual(current, true, nextState, true) {
			return nil
		}
		if exists && current.LastAuthoritySequence > event.AuthoritySequence {
			return dberrors.ErrConflict
		}
		if err := batch.Set(stateKey, stateValue); err != nil {
			return err
		}
		commitState.tableRows[string(stateKey)] = tableRowOverlay{value: append([]byte(nil), stateValue...), exists: true}
		return nil
	})
	return messageEventAppendResult(event, nextState), nil
}

func (s *Shard) messageEventAppendResultFromApplied(event MessageEventAppend, applied MessageEventApplied) (MessageEventAppendResult, error) {
	state, stateExists, err := messageEventStateTable.getByPrimaryKey(s.db, s.hashSlot, messageEventStatePrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, applied.EventKey))
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	return messageEventAppendResultFromApplied(event, applied, state, stateExists), nil
}

func (b *Batch) messageEventAppendResultFromApplied(hashSlot HashSlot, event MessageEventAppend, applied MessageEventApplied) (MessageEventAppendResult, error) {
	stateKey, err := messageEventStateRowKey(hashSlot, event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, applied.EventKey)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	stateEvent := event
	stateEvent.EventKey = applied.EventKey
	state, stateExists, err := b.loadMessageEventStateForAppend(hashSlot, stateKey, stateEvent)
	if err != nil {
		return MessageEventAppendResult{}, err
	}
	return messageEventAppendResultFromApplied(event, applied, state, stateExists), nil
}

func (b *Batch) loadMessageEventStateForAppend(hashSlot HashSlot, stateKey []byte, event MessageEventAppend) (MessageEventState, bool, error) {
	if b.messageEventStates != nil {
		if state, ok := b.messageEventStates[string(stateKey)]; ok {
			return cloneMessageEventState(state), true, nil
		}
	}
	return messageEventStateTable.getByPrimaryKey(b.db, hashSlot, messageEventStatePrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID, event.EventKey))
}

func (b *Batch) loadMessageEventCursorForAppend(hashSlot HashSlot, cursorKey []byte, event MessageEventAppend) (MessageEventCursor, bool, error) {
	if b.messageEventCursors != nil {
		if cursor, ok := b.messageEventCursors[string(cursorKey)]; ok {
			return cursor, true, nil
		}
	}
	return messageEventCursorTable.getByPrimaryKey(b.db, hashSlot, messageEventCursorPrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID))
}

func (b *Batch) loadMessageEventAppliedForAppend(hashSlot HashSlot, appliedKey []byte, event MessageEventAppend) (MessageEventApplied, bool, error) {
	if b.messageEventApplied != nil {
		if applied, ok := b.messageEventApplied[string(appliedKey)]; ok {
			return applied, true, nil
		}
	}
	return messageEventAppliedTable.getByPrimaryKey(b.db, hashSlot, messageEventAppliedPrimaryKey(event.ChannelID, event.ChannelType, event.ClientMsgNo, event.EventID))
}

func reduceMessageEventAppend(state MessageEventState, stateExists bool, cursor MessageEventCursor, cursorExists bool, event MessageEventAppend) (MessageEventState, MessageEventCursor, bool, MessageEventAppendResult) {
	if stateExists {
		state = cloneMessageEventState(state)
		if state.LastEventID == event.EventID || isMessageEventTerminal(state.Status) {
			return state, cursor, false, messageEventAppendResult(event, state)
		}
	} else {
		state = MessageEventState{
			ChannelID:   event.ChannelID,
			ChannelType: event.ChannelType,
			ClientMsgNo: event.ClientMsgNo,
			RunID:       event.RunID,
			EventKey:    event.EventKey,
			Status:      EventStatusOpen,
		}
	}
	if !cursorExists {
		cursor = MessageEventCursor{ChannelID: event.ChannelID, ChannelType: event.ChannelType, ClientMsgNo: event.ClientMsgNo, RunID: event.RunID}
	}

	nextSeq := event.MsgEventSeq
	if nextSeq == 0 {
		nextSeq = cursor.LastMsgEventSeq + 1
	}
	switch event.EventType {
	case EventTypeOpen:
		state.Status = EventStatusOpen
		state.SnapshotPayload = messageEventSnapshotFromPayload(event.Payload)
	case EventTypeDelta:
		state.Status = EventStatusOpen
		state.SnapshotPayload = reduceMessageEventDelta(state.SnapshotPayload, event.Payload)
	case EventTypeSnapshot:
		state.Status = EventStatusOpen
		state.SnapshotPayload = messageEventSnapshotFromPayload(event.Payload)
	case EventTypeFinish:
		state.Status = EventStatusClosed
		payload := decodeMessageEventTerminalPayload(event.Payload)
		if payload.snapshot != nil {
			state.SnapshotPayload = payload.snapshot
		}
		state.EndReason = payload.endReason
		state.Error = payload.errorText
	}
	state.LastMsgEventSeq = nextSeq
	state.LastAuthoritySequence = event.AuthoritySequence
	state.LastEventID = event.EventID
	state.LastEventType = event.EventType
	state.LastVisibility = event.Visibility
	state.LastOccurredAt = event.OccurredAt
	state.UpdatedAt = event.UpdatedAt

	cursor.LastMsgEventSeq = nextSeq
	cursor.LastAuthoritySequence = event.AuthoritySequence
	cursor.Terminal = event.EventType == EventTypeFinish
	cursor.UpdatedAt = event.UpdatedAt
	return state, cursor, true, messageEventAppendResult(event, state)
}

func validMessageEventAuthoritySequence(cursor MessageEventCursor, exists bool, event MessageEventAppend) bool {
	if event.ProjectionOnly {
		return event.MsgEventSeq > 0
	}
	if !exists || cursor.LastAuthoritySequence == 0 {
		return event.AuthoritySequence > 0
	}
	return event.AuthoritySequence == cursor.LastAuthoritySequence+1
}

func messageEventAppliedFromResult(event MessageEventAppend, result MessageEventAppendResult) MessageEventApplied {
	return MessageEventApplied{
		ChannelID:          event.ChannelID,
		ChannelType:        event.ChannelType,
		ClientMsgNo:        event.ClientMsgNo,
		EventID:            event.EventID,
		EventKey:           result.EventKey,
		MsgEventSeq:        result.MsgEventSeq,
		AuthoritySequence:  event.AuthoritySequence,
		Status:             result.Status,
		RunID:              event.RunID,
		AuthorizationFence: event.AuthorizationFence,
		ProjectionOnly:     event.ProjectionOnly,
		EventType:          event.EventType,
		Visibility:         event.Visibility,
		OccurredAt:         event.OccurredAt,
		Payload:            cloneBytes(event.Payload),
		UpdatedAt:          event.UpdatedAt,
	}
}

func messageEventAppendResultFromApplied(event MessageEventAppend, applied MessageEventApplied, state MessageEventState, stateExists bool) MessageEventAppendResult {
	appliedState := MessageEventState{
		ChannelID:             event.ChannelID,
		ChannelType:           event.ChannelType,
		ClientMsgNo:           event.ClientMsgNo,
		RunID:                 event.RunID,
		EventKey:              applied.EventKey,
		Status:                applied.Status,
		LastMsgEventSeq:       applied.MsgEventSeq,
		LastAuthoritySequence: applied.AuthoritySequence,
		LastEventID:           event.EventID,
		UpdatedAt:             applied.UpdatedAt,
	}
	if stateExists && state.LastEventID == event.EventID && state.LastMsgEventSeq == applied.MsgEventSeq {
		appliedState = cloneMessageEventState(state)
	}
	return MessageEventAppendResult{
		Applied:     false,
		ChannelID:   event.ChannelID,
		ChannelType: event.ChannelType,
		ClientMsgNo: event.ClientMsgNo,
		RunID:       event.RunID,
		EventID:     event.EventID,
		EventKey:    applied.EventKey,
		MsgEventSeq: applied.MsgEventSeq,
		Status:      applied.Status,
		State:       appliedState,
	}
}

func messageEventAppendResult(event MessageEventAppend, state MessageEventState) MessageEventAppendResult {
	state = cloneMessageEventState(state)
	return MessageEventAppendResult{
		Applied:     true,
		ChannelID:   event.ChannelID,
		ChannelType: event.ChannelType,
		ClientMsgNo: event.ClientMsgNo,
		RunID:       event.RunID,
		EventID:     event.EventID,
		EventKey:    state.EventKey,
		MsgEventSeq: state.LastMsgEventSeq,
		Status:      state.Status,
		State:       state,
	}
}

func normalizeMessageEventAppend(event MessageEventAppend) (MessageEventAppend, error) {
	event.ChannelID = strings.TrimSpace(event.ChannelID)
	event.ClientMsgNo = strings.TrimSpace(event.ClientMsgNo)
	event.RunID = strings.TrimSpace(event.RunID)
	event.AuthorizationFence = strings.TrimSpace(event.AuthorizationFence)
	event.EventID = strings.TrimSpace(event.EventID)
	event.EventKey = strings.TrimSpace(event.EventKey)
	event.EventType = strings.ToLower(strings.TrimSpace(event.EventType))
	event.Visibility = strings.TrimSpace(event.Visibility)
	event.Payload = cloneBytes(event.Payload)
	if event.ChannelID == "" || event.ChannelType <= 0 || event.ClientMsgNo == "" || event.RunID == "" || event.AuthorizationFence == "" || event.EventID == "" || event.EventType == "" || event.AuthoritySequence == 0 {
		return MessageEventAppend{}, dberrors.ErrInvalidArgument
	}
	if event.EventKey == "" {
		event.EventKey = EventKeyDefault
	}
	if event.Visibility == "" {
		event.Visibility = VisibilityPublic
	}
	switch event.EventType {
	case EventTypeOpen, EventTypeDelta, EventTypeSnapshot, EventTypeFinish:
	default:
		return MessageEventAppend{}, dberrors.ErrInvalidArgument
	}
	if event.ProjectionOnly && event.EventType != EventTypeSnapshot {
		return MessageEventAppend{}, dberrors.ErrInvalidArgument
	}
	return event, nil
}

func normalizeMessageEventStateKey(channelID string, channelType int64, clientMsgNo, runID, eventKey string) (string, string, string, string, error) {
	channelID, clientMsgNo, err := normalizeMessageEventMessageKey(channelID, channelType, clientMsgNo)
	if err != nil {
		return "", "", "", "", err
	}
	runID = strings.TrimSpace(runID)
	eventKey = strings.TrimSpace(eventKey)
	if eventKey == "" {
		eventKey = EventKeyDefault
	}
	if err := validateKeyString(runID); err != nil {
		return "", "", "", "", err
	}
	if err := validateKeyString(eventKey); err != nil {
		return "", "", "", "", err
	}
	return channelID, clientMsgNo, runID, eventKey, nil
}

func normalizeMessageEventMessageKey(channelID string, channelType int64, clientMsgNo string) (string, string, error) {
	channelID = strings.TrimSpace(channelID)
	clientMsgNo = strings.TrimSpace(clientMsgNo)
	if channelType <= 0 {
		return "", "", dberrors.ErrInvalidArgument
	}
	if err := validateKeyString(channelID); err != nil {
		return "", "", err
	}
	if err := validateKeyString(clientMsgNo); err != nil {
		return "", "", err
	}
	return channelID, clientMsgNo, nil
}

func validateMessageEventState(state MessageEventState) error {
	_, _, _, _, err := normalizeMessageEventStateKey(state.ChannelID, state.ChannelType, state.ClientMsgNo, state.RunID, state.EventKey)
	return err
}

func validateMessageEventCursor(cursor MessageEventCursor) error {
	channelID, clientMsgNo, err := normalizeMessageEventMessageKey(cursor.ChannelID, cursor.ChannelType, cursor.ClientMsgNo)
	if err != nil {
		return err
	}
	if channelID != cursor.ChannelID || clientMsgNo != cursor.ClientMsgNo {
		return dberrors.ErrInvalidArgument
	}
	return validateKeyString(cursor.RunID)
}

func validateMessageEventApplied(applied MessageEventApplied) error {
	channelID, clientMsgNo, err := normalizeMessageEventMessageKey(applied.ChannelID, applied.ChannelType, applied.ClientMsgNo)
	if err != nil {
		return err
	}
	if channelID != applied.ChannelID || clientMsgNo != applied.ClientMsgNo {
		return dberrors.ErrInvalidArgument
	}
	if err := validateKeyString(applied.EventID); err != nil {
		return err
	}
	if err := validateKeyString(applied.EventKey); err != nil {
		return err
	}
	return nil
}

func isMessageEventTerminal(status string) bool {
	return status == EventStatusClosed || status == EventStatusError || status == EventStatusCancelled
}

func reduceMessageEventDelta(existing []byte, payload []byte) []byte {
	var delta struct {
		TextDelta         string `json:"text_delta"`
		AuthoritySequence uint64 `json:"authority_sequence"`
		ProjectionDigest  string `json:"projection_digest_sha256"`
	}
	if err := json.Unmarshal(payload, &delta); err != nil || delta.TextDelta == "" {
		return cloneBytes(payload)
	}
	text := ""
	var current map[string]any
	if json.Unmarshal(existing, &current) != nil {
		current = map[string]any{"state": "running", "complete": false}
	}
	if currentText, ok := current["text"].(string); ok {
		text = currentText
	}
	current["text"] = text + delta.TextDelta
	current["authority_sequence"] = delta.AuthoritySequence
	current["projection_digest_sha256"] = delta.ProjectionDigest
	out, err := json.Marshal(current)
	if err != nil {
		return cloneBytes(payload)
	}
	return out
}

func messageEventSnapshotFromPayload(payload []byte) []byte {
	var body struct {
		Snapshot json.RawMessage `json:"snapshot"`
	}
	if json.Unmarshal(payload, &body) != nil || len(body.Snapshot) == 0 || string(body.Snapshot) == "null" {
		return nil
	}
	return cloneBytes(body.Snapshot)
}

type messageEventTerminalPayload struct {
	snapshot  []byte
	endReason uint8
	errorText string
}

func decodeMessageEventTerminalPayload(payload []byte) messageEventTerminalPayload {
	var raw struct {
		Snapshot json.RawMessage `json:"snapshot"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return messageEventTerminalPayload{}
	}
	out := messageEventTerminalPayload{}
	if len(raw.Snapshot) > 0 && string(raw.Snapshot) != "null" {
		out.snapshot = cloneBytes(raw.Snapshot)
		var snapshot struct {
			PublicErrorCode string `json:"public_error_code"`
		}
		if json.Unmarshal(raw.Snapshot, &snapshot) == nil {
			out.errorText = snapshot.PublicErrorCode
		}
	}
	return out
}

func messageEventStatePrimaryKey(channelID string, channelType int64, clientMsgNo, runID, eventKey string) KeyParts {
	return KeyParts{String(channelID), Int64Ordered(channelType), String(clientMsgNo), String(runID), String(eventKey)}
}

func messageEventCursorPrimaryKey(channelID string, channelType int64, clientMsgNo, runID string) KeyParts {
	return KeyParts{String(channelID), Int64Ordered(channelType), String(clientMsgNo), String(runID)}
}

func messageEventAppliedPrimaryKey(channelID string, channelType int64, clientMsgNo string, eventID string) KeyParts {
	return KeyParts{String(channelID), Int64Ordered(channelType), String(clientMsgNo), String(eventID)}
}

func messageEventStateRowKey(hashSlot HashSlot, channelID string, channelType int64, clientMsgNo, runID, eventKey string) ([]byte, error) {
	return messageEventStateTable.primaryRowKey(hashSlot, messageEventStatePrimaryKey(channelID, channelType, clientMsgNo, runID, eventKey))
}

func messageEventCursorRowKey(hashSlot HashSlot, channelID string, channelType int64, clientMsgNo, runID string) ([]byte, error) {
	return messageEventCursorTable.primaryRowKey(hashSlot, messageEventCursorPrimaryKey(channelID, channelType, clientMsgNo, runID))
}

func messageEventAppliedRowKey(hashSlot HashSlot, channelID string, channelType int64, clientMsgNo string, eventID string) ([]byte, error) {
	return messageEventAppliedTable.primaryRowKey(hashSlot, messageEventAppliedPrimaryKey(channelID, channelType, clientMsgNo, eventID))
}

func encodeMessageEventStateValue(state MessageEventState) []byte {
	value := appendValueString(nil, state.Status)
	value = appendValueUint64(value, state.LastMsgEventSeq)
	value = appendValueUint64(value, state.LastAuthoritySequence)
	value = appendValueString(value, state.LastEventID)
	value = appendValueString(value, state.LastEventType)
	value = appendValueString(value, state.LastVisibility)
	value = appendValueInt64(value, state.LastOccurredAt)
	value = appendValueBytes(value, state.SnapshotPayload)
	value = append(value, state.EndReason)
	value = appendValueString(value, state.Error)
	return appendValueInt64(value, state.UpdatedAt)
}

func decodeMessageEventStateValue(channelID string, channelType int64, clientMsgNo, runID, eventKey string, value []byte) (MessageEventState, error) {
	status, rest, err := readValueString(value)
	if err != nil {
		return MessageEventState{}, err
	}
	lastSeq, rest, err := readValueUint64(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	lastAuthoritySequence, rest, err := readValueUint64(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	lastEventID, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	lastEventType, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	lastVisibility, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	lastOccurredAt, rest, err := readValueInt64(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	snapshot, rest, err := readValueBytes(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	if len(rest) < 1 {
		return MessageEventState{}, dberrors.ErrCorruptValue
	}
	endReason := rest[0]
	rest = rest[1:]
	errorText, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	updatedAt, rest, err := readValueInt64(rest)
	if err != nil {
		return MessageEventState{}, err
	}
	if len(rest) != 0 {
		return MessageEventState{}, dberrors.ErrCorruptValue
	}
	return MessageEventState{
		ChannelID:             channelID,
		ChannelType:           channelType,
		ClientMsgNo:           clientMsgNo,
		RunID:                 runID,
		EventKey:              eventKey,
		Status:                status,
		LastMsgEventSeq:       lastSeq,
		LastAuthoritySequence: lastAuthoritySequence,
		LastEventID:           lastEventID,
		LastEventType:         lastEventType,
		LastVisibility:        lastVisibility,
		LastOccurredAt:        lastOccurredAt,
		SnapshotPayload:       snapshot,
		EndReason:             endReason,
		Error:                 errorText,
		UpdatedAt:             updatedAt,
	}, nil
}

func encodeMessageEventAppliedValue(applied MessageEventApplied) []byte {
	value := appendValueString(nil, applied.EventKey)
	value = appendValueUint64(value, applied.MsgEventSeq)
	value = appendValueUint64(value, applied.AuthoritySequence)
	value = appendValueString(value, applied.Status)
	value = appendValueInt64(value, applied.UpdatedAt)
	value = appendValueString(value, applied.RunID)
	value = appendValueString(value, applied.AuthorizationFence)
	if applied.ProjectionOnly {
		value = append(value, 1)
	} else {
		value = append(value, 0)
	}
	value = appendValueString(value, applied.EventType)
	value = appendValueString(value, applied.Visibility)
	value = appendValueInt64(value, applied.OccurredAt)
	return appendValueBytes(value, applied.Payload)
}

func decodeMessageEventAppliedValue(channelID string, channelType int64, clientMsgNo string, eventID string, value []byte) (MessageEventApplied, error) {
	eventKey, rest, err := readValueString(value)
	if err != nil {
		return MessageEventApplied{}, err
	}
	msgEventSeq, rest, err := readValueUint64(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	authoritySequence, rest, err := readValueUint64(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	status, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	updatedAt, rest, err := readValueInt64(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	runID, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	authorizationFence, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	if len(rest) == 0 || rest[0] > 1 {
		return MessageEventApplied{}, dberrors.ErrCorruptValue
	}
	projectionOnly := rest[0] == 1
	rest = rest[1:]
	eventType, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	visibility, rest, err := readValueString(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	occurredAt, rest, err := readValueInt64(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	payload, rest, err := readValueBytes(rest)
	if err != nil {
		return MessageEventApplied{}, err
	}
	if len(rest) != 0 {
		return MessageEventApplied{}, dberrors.ErrCorruptValue
	}
	return MessageEventApplied{
		ChannelID:          channelID,
		ChannelType:        channelType,
		ClientMsgNo:        clientMsgNo,
		EventID:            eventID,
		EventKey:           eventKey,
		MsgEventSeq:        msgEventSeq,
		AuthoritySequence:  authoritySequence,
		Status:             status,
		RunID:              runID,
		AuthorizationFence: authorizationFence,
		ProjectionOnly:     projectionOnly,
		EventType:          eventType,
		Visibility:         visibility,
		OccurredAt:         occurredAt,
		Payload:            payload,
		UpdatedAt:          updatedAt,
	}, nil
}

func encodeMessageEventCursorValue(cursor MessageEventCursor) []byte {
	value := appendValueUint64(nil, cursor.LastMsgEventSeq)
	value = appendValueUint64(value, cursor.LastAuthoritySequence)
	value = appendValueInt64(value, cursor.UpdatedAt)
	if cursor.Terminal {
		return append(value, 1)
	}
	return append(value, 0)
}

func decodeMessageEventCursorValue(channelID string, channelType int64, clientMsgNo, runID string, value []byte) (MessageEventCursor, error) {
	lastSeq, rest, err := readValueUint64(value)
	if err != nil {
		return MessageEventCursor{}, err
	}
	lastAuthoritySequence, rest, err := readValueUint64(rest)
	if err != nil {
		return MessageEventCursor{}, err
	}
	updatedAt, rest, err := readValueInt64(rest)
	if err != nil {
		return MessageEventCursor{}, err
	}
	if len(rest) != 1 || rest[0] > 1 {
		return MessageEventCursor{}, dberrors.ErrCorruptValue
	}
	return MessageEventCursor{
		ChannelID:             channelID,
		ChannelType:           channelType,
		ClientMsgNo:           clientMsgNo,
		RunID:                 runID,
		LastMsgEventSeq:       lastSeq,
		LastAuthoritySequence: lastAuthoritySequence,
		Terminal:              rest[0] == 1,
		UpdatedAt:             updatedAt,
	}, nil
}

func cloneMessageEventState(state MessageEventState) MessageEventState {
	state.SnapshotPayload = cloneBytes(state.SnapshotPayload)
	return state
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func messageEventStateEqual(left MessageEventState, leftExists bool, right MessageEventState, rightExists bool) bool {
	if leftExists != rightExists {
		return false
	}
	if !leftExists {
		return true
	}
	return left.ChannelID == right.ChannelID &&
		left.ChannelType == right.ChannelType &&
		left.ClientMsgNo == right.ClientMsgNo &&
		left.RunID == right.RunID &&
		left.EventKey == right.EventKey &&
		left.Status == right.Status &&
		left.LastMsgEventSeq == right.LastMsgEventSeq &&
		left.LastAuthoritySequence == right.LastAuthoritySequence &&
		left.LastEventID == right.LastEventID &&
		left.LastEventType == right.LastEventType &&
		left.LastVisibility == right.LastVisibility &&
		left.LastOccurredAt == right.LastOccurredAt &&
		bytes.Equal(left.SnapshotPayload, right.SnapshotPayload) &&
		left.EndReason == right.EndReason &&
		left.Error == right.Error &&
		left.UpdatedAt == right.UpdatedAt
}

func messageEventCursorEqual(left MessageEventCursor, leftExists bool, right MessageEventCursor, rightExists bool) bool {
	if leftExists != rightExists {
		return false
	}
	if !leftExists {
		return true
	}
	return left == right
}

func messageEventAppliedEqual(left MessageEventApplied, leftExists bool, right MessageEventApplied, rightExists bool) bool {
	if leftExists != rightExists {
		return false
	}
	if !leftExists {
		return true
	}
	return left.ChannelID == right.ChannelID &&
		left.ChannelType == right.ChannelType &&
		left.ClientMsgNo == right.ClientMsgNo &&
		left.EventID == right.EventID &&
		left.EventKey == right.EventKey &&
		left.MsgEventSeq == right.MsgEventSeq &&
		left.AuthoritySequence == right.AuthoritySequence &&
		left.Status == right.Status &&
		left.RunID == right.RunID &&
		left.AuthorizationFence == right.AuthorizationFence &&
		left.ProjectionOnly == right.ProjectionOnly &&
		left.EventType == right.EventType &&
		left.Visibility == right.Visibility &&
		left.OccurredAt == right.OccurredAt &&
		bytes.Equal(left.Payload, right.Payload) &&
		left.UpdatedAt == right.UpdatedAt
}

func messageEventAppliedMatches(applied MessageEventApplied, event MessageEventAppend) bool {
	return applied.ChannelID == event.ChannelID &&
		applied.ChannelType == event.ChannelType &&
		applied.ClientMsgNo == event.ClientMsgNo &&
		applied.EventID == event.EventID &&
		applied.RunID == event.RunID &&
		applied.AuthorizationFence == event.AuthorizationFence &&
		applied.AuthoritySequence == event.AuthoritySequence &&
		applied.ProjectionOnly == event.ProjectionOnly &&
		applied.EventKey == event.EventKey &&
		applied.EventType == event.EventType &&
		applied.Visibility == event.Visibility &&
		applied.OccurredAt == event.OccurredAt &&
		bytes.Equal(applied.Payload, event.Payload)
}

// MessageEventAppliedMatches reports whether an idempotency row represents the exact normalized event.
func MessageEventAppliedMatches(applied MessageEventApplied, event MessageEventAppend) bool {
	return messageEventAppliedMatches(applied, event)
}

// MessageEventSameInput compares the normalized producer-owned fields of two appends.
func MessageEventSameInput(left, right MessageEventAppend) bool {
	return left.ChannelID == right.ChannelID &&
		left.ChannelType == right.ChannelType &&
		left.ClientMsgNo == right.ClientMsgNo &&
		left.RunID == right.RunID &&
		left.AuthorizationFence == right.AuthorizationFence &&
		left.AuthoritySequence == right.AuthoritySequence &&
		left.ProjectionOnly == right.ProjectionOnly &&
		left.EventID == right.EventID &&
		left.EventKey == right.EventKey &&
		left.EventType == right.EventType &&
		left.Visibility == right.Visibility &&
		left.OccurredAt == right.OccurredAt &&
		bytes.Equal(left.Payload, right.Payload)
}
