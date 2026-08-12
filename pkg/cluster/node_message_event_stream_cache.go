package cluster

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/propose"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
)

const (
	defaultMessageEventStreamCacheMaxSessions = 50000
	defaultMessageEventStreamCacheMaxBytes    = 512 << 20
	defaultMessageEventFinishCoalesceWindow   = time.Millisecond
	maxMessageEventLanesPerRun                = 8
	maxMessageEventEventsPerRun               = 4096
	maxMessageEventProjectionBytesPerRun      = 4 << 20
	maxMessageEventSnapshotBytes              = 256 << 10
	maxMessageEventRunDuration                = 30 * time.Minute
)

type messageEventStreamCacheKey struct {
	channelID   string
	channelType int64
	clientMsgNo string
}

type messageEventStreamCacheSession struct {
	states             map[messageEventLaneKey]metadb.MessageEventState
	applied            map[string]metadb.MessageEventAppendResult
	appliedDigest      map[string]string
	terminalRuns       map[string]metadb.MessageEventAppendResult
	runSequences       map[string]uint64
	runMsgEventSeqs    map[string]uint64
	runEventCounts     map[string]int
	runProjectionBytes map[string]int64
	runStarted         map[string]time.Time
	finishingRuns      map[string]messageEventFinishingRun
	updated            time.Time
}

type messageEventFinishingRun struct {
	eventID string
	digest  string
	seq     uint64
}

type messageEventLaneKey struct {
	runID    string
	eventKey string
}

type messageEventFinishCoalesceKey struct {
	channelID   string
	channelType int64
	clientMsgNo string
	runID       string
}

type messageEventFinishCoalesceRequest struct {
	ctx    context.Context
	event  metadb.MessageEventAppend
	events []metadb.MessageEventAppend
	done   chan messageEventFinishCoalesceResult
}

type messageEventFinishCoalesceResult struct {
	result metadb.MessageEventAppendResult
	path   string
	err    error
}

type messageEventFinishCoalesceGroup struct {
	requests []*messageEventFinishCoalesceRequest
}

// messageEventFinishCoalescer batches concurrent finish flushes for one channel.
type messageEventFinishCoalescer struct {
	mu     sync.Mutex
	window time.Duration
	groups map[messageEventFinishCoalesceKey]*messageEventFinishCoalesceGroup
}

func newMessageEventFinishCoalescer(window time.Duration) *messageEventFinishCoalescer {
	if window <= 0 {
		return nil
	}
	return &messageEventFinishCoalescer{
		window: window,
		groups: make(map[messageEventFinishCoalesceKey]*messageEventFinishCoalesceGroup),
	}
}

// messageEventStreamCache keeps in-flight stream projections on the Slot leader.
type messageEventStreamCache struct {
	mu              sync.Mutex
	restorePaused   bool
	maxSessions     int
	maxPayloadBytes int64
	sessions        map[messageEventStreamCacheKey]*messageEventStreamCacheSession
	openLanes       int
	payloadBytes    int64
}

func newMessageEventStreamCache(maxSessions int) *messageEventStreamCache {
	if maxSessions <= 0 {
		maxSessions = defaultMessageEventStreamCacheMaxSessions
	}
	return &messageEventStreamCache{
		maxSessions:     maxSessions,
		maxPayloadBytes: defaultMessageEventStreamCacheMaxBytes,
		sessions:        make(map[messageEventStreamCacheKey]*messageEventStreamCacheSession),
	}
}

func (c *messageEventStreamCache) resetAfterRestore() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions = make(map[messageEventStreamCacheKey]*messageEventStreamCacheSession)
	c.openLanes = 0
	c.payloadBytes = 0
}

func (c *messageEventStreamCache) pauseForRestore() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.restorePaused = true
	c.sessions = make(map[messageEventStreamCacheKey]*messageEventStreamCacheSession)
	c.openLanes = 0
	c.payloadBytes = 0
}

func (c *messageEventStreamCache) resumeAfterRestore() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions = make(map[messageEventStreamCacheKey]*messageEventStreamCacheSession)
	c.openLanes = 0
	c.payloadBytes = 0
	c.restorePaused = false
}

func (c *messageEventStreamCache) appendCached(event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, error) {
	result, _, err := c.appendCachedObserved(event)
	return result, err
}

func (c *messageEventStreamCache) appendCachedObserved(event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, MessageEventStreamCacheObservation, error) {
	if c == nil {
		return cachedMessageEventResult(event, cachedMessageEventState(event)), MessageEventStreamCacheObservation{}, nil
	}
	key := messageEventCacheKey(event)
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.restorePaused {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrMaintenance
	}

	session, err := c.sessionLocked(key, now)
	if err != nil {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), err
	}
	if result, ok := session.applied[event.EventID]; ok {
		if session.appliedDigest[event.EventID] != metadb.MessageEventDigest(event) {
			return metadb.MessageEventAppendResult{}, c.observationLocked(), metadb.ErrStaleMeta
		}
		result = cloneMessageEventAppendResult(result)
		result.Applied = false
		return result, c.observationLocked(), nil
	}
	if _, terminal := session.terminalRuns[event.RunID]; terminal {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrMessageEventRunTerminal
	}
	if _, finishing := session.finishingRuns[event.RunID]; finishing {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrMessageEventRunTerminal
	}
	lastSequence, sequenceKnown := session.runSequences[event.RunID]
	if sequenceKnown && event.AuthoritySequence != lastSequence+1 {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), metadb.ErrStaleMeta
	}
	if !sequenceKnown && event.EventType == metadb.EventTypeDelta && event.AuthoritySequence > 1 {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrMessageEventStreamCacheMiss
	}
	event.MsgEventSeq = session.runMsgEventSeqs[event.RunID] + 1
	if !sequenceKnown && event.EventType == metadb.EventTypeSnapshot && event.AuthoritySequence > 1 {
		// After leader failover the cache has no transport watermark. Using the
		// authoritative snapshot watermark creates an intentional transport gap,
		// so connected clients recover the self-contained snapshot instead of
		// treating it as an old seq=1 replay.
		event.MsgEventSeq = event.AuthoritySequence
	}
	laneKey := messageEventLaneCacheKey(event)
	if _, exists := session.states[laneKey]; !exists && c.runLaneCountLocked(session, event.RunID) >= maxMessageEventLanesPerRun {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrMessageEventLaneLimit
	}
	started := session.runStarted[event.RunID]
	if !started.IsZero() && now.Sub(started) > maxMessageEventRunDuration {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrBackpressured
	}
	if session.runEventCounts[event.RunID] >= maxMessageEventEventsPerRun || session.runProjectionBytes[event.RunID]+int64(len(event.Payload)) > maxMessageEventProjectionBytesPerRun {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrBackpressured
	}

	state := session.states[laneKey]
	oldState, hadState := session.states[laneKey]
	if state.EventKey == "" {
		state = cachedMessageEventState(event)
	}
	if isMessageEventTerminalStatus(state.Status) {
		result := cachedMessageEventResult(event, state)
		session.applied[event.EventID] = cloneMessageEventAppendResult(result)
		return result, c.observationLocked(), nil
	}

	state.Status = metadb.EventStatusOpen
	nextPayload := cloneBytes(state.SnapshotPayload)
	switch event.EventType {
	case metadb.EventTypeOpen:
		nextPayload = cachedMessageEventSnapshotFromPayload(event.Payload)
	case metadb.EventTypeDelta:
		nextPayload = reduceCachedMessageEventDelta(state.SnapshotPayload, event.Payload)
	case metadb.EventTypeSnapshot:
		nextPayload = cachedMessageEventSnapshotFromPayload(event.Payload)
	}
	if len(nextPayload) > maxMessageEventSnapshotBytes {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrBackpressured
	}
	projectedPayloadBytes := c.payloadBytes - int64(len(state.SnapshotPayload)) + int64(len(nextPayload))
	if projectedPayloadBytes > c.maxPayloadBytes {
		return metadb.MessageEventAppendResult{}, c.observationLocked(), ErrBackpressured
	}
	state.SnapshotPayload = nextPayload
	state.LastEventID = event.EventID
	state.LastEventType = event.EventType
	state.LastVisibility = event.Visibility
	state.LastOccurredAt = event.OccurredAt
	state.UpdatedAt = event.UpdatedAt
	state.LastMsgEventSeq = event.MsgEventSeq
	state.LastAuthoritySequence = event.AuthoritySequence
	session.states[laneKey] = cloneMessageEventState(state)
	c.accountStateReplaceLocked(oldState, hadState, state)
	session.updated = now

	result := cachedMessageEventResult(event, state)
	session.applied[event.EventID] = lightweightMessageEventAppendResult(result)
	session.appliedDigest[event.EventID] = metadb.MessageEventDigest(event)
	session.runSequences[event.RunID] = event.AuthoritySequence
	session.runMsgEventSeqs[event.RunID] = event.MsgEventSeq
	session.runEventCounts[event.RunID]++
	session.runProjectionBytes[event.RunID] += int64(len(event.Payload))
	if started.IsZero() {
		session.runStarted[event.RunID] = now
	}
	return result, c.observationLocked(), nil
}

func (c *messageEventStreamCache) mergeTerminalPayload(event metadb.MessageEventAppend) metadb.MessageEventAppend {
	if c == nil || !isMessageEventTerminalEvent(event.EventType) {
		return event
	}
	key := messageEventCacheKey(event)
	c.mu.Lock()
	defer c.mu.Unlock()

	session := c.sessions[key]
	if session == nil {
		return event
	}
	state := session.states[messageEventLaneCacheKey(event)]
	if state.EventKey == "" || len(state.SnapshotPayload) == 0 {
		return event
	}
	event.Payload = mergeMessageEventTerminalPayload(event.Payload, state.SnapshotPayload)
	return event
}

func (c *messageEventStreamCache) prepareFinish(event metadb.MessageEventAppend) (metadb.MessageEventAppend, []metadb.MessageEventState, error) {
	if c == nil || event.EventType != metadb.EventTypeFinish {
		return event, nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	session := c.sessions[messageEventCacheKey(event)]
	if session == nil {
		return event, nil, nil
	}
	digest := metadb.MessageEventDigest(event)
	if finishing, ok := session.finishingRuns[event.RunID]; ok {
		if finishing.eventID != event.EventID {
			return event, nil, ErrMessageEventRunTerminal
		}
		if finishing.digest != digest {
			return event, nil, metadb.ErrStaleMeta
		}
		event.MsgEventSeq = finishing.seq
	} else {
		if last := session.runSequences[event.RunID]; last > 0 && event.AuthoritySequence != last+1 {
			return event, nil, metadb.ErrStaleMeta
		}
		if event.MsgEventSeq == 0 {
			event.MsgEventSeq = session.runMsgEventSeqs[event.RunID] + 1
		}
		session.finishingRuns[event.RunID] = messageEventFinishingRun{eventID: event.EventID, digest: digest, seq: event.MsgEventSeq}
	}
	out := make([]metadb.MessageEventState, 0, len(session.states))
	for _, state := range session.states {
		if state.EventKey == "" || state.RunID != event.RunID || isMessageEventTerminalStatus(state.Status) {
			continue
		}
		out = append(out, cloneMessageEventState(state))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventKey < out[j].EventKey })
	return event, out, nil
}

func (c *messageEventStreamCache) abortFinish(event metadb.MessageEventAppend) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	session := c.sessions[messageEventCacheKey(event)]
	if session == nil {
		return
	}
	finishing, ok := session.finishingRuns[event.RunID]
	if ok && finishing.eventID == event.EventID && finishing.digest == metadb.MessageEventDigest(event) {
		delete(session.finishingRuns, event.RunID)
	}
}

func (c *messageEventStreamCache) states(key metadb.MessageEventMessageKey) []metadb.MessageEventState {
	if c == nil {
		return nil
	}
	cacheKey := messageEventStreamCacheKey{
		channelID:   strings.TrimSpace(key.ChannelID),
		channelType: key.ChannelType,
		clientMsgNo: strings.TrimSpace(key.ClientMsgNo),
	}
	if cacheKey.channelID == "" || cacheKey.channelType <= 0 || cacheKey.clientMsgNo == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	session := c.sessions[cacheKey]
	if session == nil || len(session.states) == 0 {
		return nil
	}
	out := make([]metadb.MessageEventState, 0, len(session.states))
	for _, state := range session.states {
		out = append(out, cloneMessageEventState(state))
	}
	return out
}

func (c *messageEventStreamCache) remove(event metadb.MessageEventAppend) {
	if c == nil {
		return
	}
	key := messageEventCacheKey(event)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteSessionLocked(key)
}

func (c *messageEventStreamCache) removeObserved(event metadb.MessageEventAppend) MessageEventStreamCacheObservation {
	if c == nil {
		return MessageEventStreamCacheObservation{}
	}
	key := messageEventCacheKey(event)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteSessionLocked(key)
	return c.observationLocked()
}

func (c *messageEventStreamCache) terminalResult(event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, bool, error) {
	if c == nil {
		return metadb.MessageEventAppendResult{}, false, nil
	}
	key := messageEventCacheKey(event)
	c.mu.Lock()
	defer c.mu.Unlock()
	session := c.sessions[key]
	if session == nil {
		return metadb.MessageEventAppendResult{}, false, nil
	}
	if result, ok := session.applied[event.EventID]; ok {
		if session.appliedDigest[event.EventID] != metadb.MessageEventDigest(event) {
			return metadb.MessageEventAppendResult{}, false, metadb.ErrStaleMeta
		}
		return cloneMessageEventAppendResult(result), true, nil
	}
	if _, terminal := session.terminalRuns[event.RunID]; terminal {
		return metadb.MessageEventAppendResult{}, false, ErrMessageEventRunTerminal
	}
	return metadb.MessageEventAppendResult{}, false, nil
}

func (c *messageEventStreamCache) completeRunObserved(event metadb.MessageEventAppend, result metadb.MessageEventAppendResult) MessageEventStreamCacheObservation {
	if c == nil {
		return MessageEventStreamCacheObservation{}
	}
	key := messageEventCacheKey(event)
	c.mu.Lock()
	defer c.mu.Unlock()
	session := c.sessions[key]
	if session == nil {
		session = &messageEventStreamCacheSession{
			states:             make(map[messageEventLaneKey]metadb.MessageEventState),
			applied:            make(map[string]metadb.MessageEventAppendResult),
			appliedDigest:      make(map[string]string),
			terminalRuns:       make(map[string]metadb.MessageEventAppendResult),
			runSequences:       make(map[string]uint64),
			runMsgEventSeqs:    make(map[string]uint64),
			runEventCounts:     make(map[string]int),
			runProjectionBytes: make(map[string]int64),
			runStarted:         make(map[string]time.Time),
			finishingRuns:      make(map[string]messageEventFinishingRun),
		}
		c.sessions[key] = session
	}
	for laneKey, state := range session.states {
		if laneKey.runID != event.RunID {
			continue
		}
		c.accountStateRemoveLocked(state)
		delete(session.states, laneKey)
	}
	session.applied[event.EventID] = lightweightMessageEventAppendResult(result)
	session.appliedDigest[event.EventID] = metadb.MessageEventDigest(event)
	session.terminalRuns[event.RunID] = lightweightMessageEventAppendResult(result)
	session.runSequences[event.RunID] = event.AuthoritySequence
	session.runMsgEventSeqs[event.RunID] = result.MsgEventSeq
	delete(session.runEventCounts, event.RunID)
	delete(session.runProjectionBytes, event.RunID)
	delete(session.runStarted, event.RunID)
	delete(session.finishingRuns, event.RunID)
	session.updated = time.Now()
	return c.observationLocked()
}

func (c *messageEventStreamCache) removeHashSlotsObserved(hashSlots map[uint16]struct{}, hashSlotCount uint16) MessageEventStreamCacheObservation {
	if c == nil {
		return MessageEventStreamCacheObservation{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(hashSlots) == 0 || hashSlotCount == 0 {
		return c.observationLocked()
	}
	for key := range c.sessions {
		hashSlot := routing.HashSlotForKey(key.channelID, hashSlotCount)
		if _, ok := hashSlots[hashSlot]; ok {
			c.deleteSessionLocked(key)
		}
	}
	return c.observationLocked()
}

func (c *messageEventStreamCache) observation() MessageEventStreamCacheObservation {
	if c == nil {
		return MessageEventStreamCacheObservation{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.observationLocked()
}

func (c *messageEventStreamCache) observationLocked() MessageEventStreamCacheObservation {
	return MessageEventStreamCacheObservation{
		Sessions:     len(c.sessions),
		OpenLanes:    c.openLanes,
		PayloadBytes: c.payloadBytes,
		MaxSessions:  c.maxSessions,
	}
}

func (c *messageEventStreamCache) deleteSessionLocked(key messageEventStreamCacheKey) {
	session := c.sessions[key]
	if session != nil {
		for _, state := range session.states {
			c.accountStateRemoveLocked(state)
		}
	}
	delete(c.sessions, key)
}

func (c *messageEventStreamCache) sessionLocked(key messageEventStreamCacheKey, now time.Time) (*messageEventStreamCacheSession, error) {
	session := c.sessions[key]
	if session != nil {
		session.updated = now
		return session, nil
	}
	if len(c.sessions) >= c.maxSessions {
		if !c.evictOldestTerminalLocked() {
			return nil, ErrBackpressured
		}
	}
	session = &messageEventStreamCacheSession{
		states:             make(map[messageEventLaneKey]metadb.MessageEventState),
		applied:            make(map[string]metadb.MessageEventAppendResult),
		appliedDigest:      make(map[string]string),
		terminalRuns:       make(map[string]metadb.MessageEventAppendResult),
		runSequences:       make(map[string]uint64),
		runMsgEventSeqs:    make(map[string]uint64),
		runEventCounts:     make(map[string]int),
		runProjectionBytes: make(map[string]int64),
		runStarted:         make(map[string]time.Time),
		finishingRuns:      make(map[string]messageEventFinishingRun),
		updated:            now,
	}
	c.sessions[key] = session
	return session, nil
}

func (c *messageEventStreamCache) evictOldestTerminalLocked() bool {
	var (
		oldestKey messageEventStreamCacheKey
		oldestSet bool
		oldestAt  time.Time
	)
	for key, session := range c.sessions {
		if session == nil {
			c.deleteSessionLocked(key)
			return true
		}
		if !isMessageEventTerminalCacheSession(session) {
			continue
		}
		if !oldestSet || session.updated.Before(oldestAt) {
			oldestKey = key
			oldestAt = session.updated
			oldestSet = true
		}
	}
	if oldestSet {
		c.deleteSessionLocked(oldestKey)
		return true
	}
	return false
}

func (c *messageEventStreamCache) accountStateReplaceLocked(oldState metadb.MessageEventState, hadOld bool, newState metadb.MessageEventState) {
	if hadOld {
		c.accountStateRemoveLocked(oldState)
	}
	c.accountStateAddLocked(newState)
}

func (c *messageEventStreamCache) accountStateAddLocked(state metadb.MessageEventState) {
	if state.EventKey == "" {
		return
	}
	if !isMessageEventTerminalStatus(state.Status) {
		c.openLanes++
	}
	c.payloadBytes += int64(len(state.SnapshotPayload))
}

func (c *messageEventStreamCache) accountStateRemoveLocked(state metadb.MessageEventState) {
	if state.EventKey == "" {
		return
	}
	if !isMessageEventTerminalStatus(state.Status) && c.openLanes > 0 {
		c.openLanes--
	}
	c.payloadBytes -= int64(len(state.SnapshotPayload))
	if c.payloadBytes < 0 {
		c.payloadBytes = 0
	}
}

func isMessageEventTerminalCacheSession(session *messageEventStreamCacheSession) bool {
	if session == nil || len(session.states) == 0 {
		return true
	}
	for _, state := range session.states {
		if !isMessageEventTerminalStatus(state.Status) {
			return false
		}
	}
	return true
}

func (c *messageEventStreamCache) runLaneCountLocked(session *messageEventStreamCacheSession, runID string) int {
	count := 0
	for laneKey := range session.states {
		if laneKey.runID == runID {
			count++
		}
	}
	return count
}

func messageEventLaneCacheKey(event metadb.MessageEventAppend) messageEventLaneKey {
	return messageEventLaneKey{runID: event.RunID, eventKey: event.EventKey}
}

type messageEventAppendRPCRequest struct {
	Op          string                          `json:"op,omitempty"`
	Event       metadb.MessageEventAppend       `json:"event,omitempty"`
	Keys        []metadb.MessageEventMessageKey `json:"keys,omitempty"`
	Limit       int                             `json:"limit,omitempty"`
	ChannelID   string                          `json:"channel_id,omitempty"`
	ChannelType int64                           `json:"channel_type,omitempty"`
	MessageID   uint64                          `json:"message_id,omitempty"`
}

type messageEventAppendRPCResponse struct {
	Result metadb.MessageEventAppendResult `json:"result,omitempty"`
	States []messageEventStatesRPCEntry    `json:"states,omitempty"`
	Anchor MessageEventAnchor              `json:"anchor,omitempty"`
	Found  bool                            `json:"found,omitempty"`
}

type messageEventStatesRPCEntry struct {
	Key    metadb.MessageEventMessageKey `json:"key"`
	States []metadb.MessageEventState    `json:"states"`
}

type messageEventAppendRPCHandler struct {
	node *Node
}

func (h messageEventAppendRPCHandler) HandleRPC(ctx context.Context, payload []byte) ([]byte, error) {
	var req messageEventAppendRPCRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	switch req.Op {
	case "", "append":
		result, err := h.node.appendMessageEventLocal(ctx, req.Event)
		if err != nil {
			return nil, err
		}
		return json.Marshal(messageEventAppendRPCResponse{Result: result})
	case "states_batch":
		rows, err := h.node.getMessageEventStatesBatchLocal(ctx, req.Keys, req.Limit)
		if err != nil {
			return nil, err
		}
		return json.Marshal(messageEventAppendRPCResponse{States: messageEventStateEntriesFromMap(rows)})
	case "anchor_lookup":
		anchor, found, err := h.node.lookupMessageEventAnchorLocal(ctx, req.ChannelID, req.ChannelType, req.MessageID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(messageEventAppendRPCResponse{Anchor: anchor, Found: found})
	default:
		return nil, metadb.ErrInvalidArgument
	}
}

func (n *Node) forwardMessageEventAnchorLookup(ctx context.Context, nodeID uint64, channelID string, channelType int64, messageID uint64) (MessageEventAnchor, bool, error) {
	body, err := json.Marshal(messageEventAppendRPCRequest{Op: "anchor_lookup", ChannelID: channelID, ChannelType: channelType, MessageID: messageID})
	if err != nil {
		return MessageEventAnchor{}, false, err
	}
	respBody, err := n.CallRPC(ctx, nodeID, clusternet.RPCMessageEventAppend, body)
	if err != nil {
		return MessageEventAnchor{}, false, err
	}
	var resp messageEventAppendRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return MessageEventAnchor{}, false, err
	}
	return resp.Anchor, resp.Found, nil
}

func (n *Node) forwardMessageEventAppend(ctx context.Context, nodeID uint64, event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, error) {
	body, err := json.Marshal(messageEventAppendRPCRequest{Event: event})
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	respBody, err := n.CallRPC(ctx, nodeID, clusternet.RPCMessageEventAppend, body)
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	var resp messageEventAppendRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	resp.Result.State.SnapshotPayload = cloneBytes(resp.Result.State.SnapshotPayload)
	return resp.Result, nil
}

func (n *Node) forwardMessageEventStatesBatch(ctx context.Context, nodeID uint64, keys []metadb.MessageEventMessageKey, limit int) (map[metadb.MessageEventMessageKey][]metadb.MessageEventState, error) {
	body, err := json.Marshal(messageEventAppendRPCRequest{Op: "states_batch", Keys: keys, Limit: limit})
	if err != nil {
		return nil, err
	}
	respBody, err := n.CallRPC(ctx, nodeID, clusternet.RPCMessageEventAppend, body)
	if err != nil {
		return nil, err
	}
	var resp messageEventAppendRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}
	return messageEventStateMapFromEntries(resp.States), nil
}

func messageEventStateEntriesFromMap(rows map[metadb.MessageEventMessageKey][]metadb.MessageEventState) []messageEventStatesRPCEntry {
	entries := make([]messageEventStatesRPCEntry, 0, len(rows))
	for key, states := range rows {
		entries = append(entries, messageEventStatesRPCEntry{Key: key, States: cloneMessageEventStates(states)})
	}
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i].Key, entries[j].Key
		if left.ChannelID != right.ChannelID {
			return left.ChannelID < right.ChannelID
		}
		if left.ChannelType != right.ChannelType {
			return left.ChannelType < right.ChannelType
		}
		return left.ClientMsgNo < right.ClientMsgNo
	})
	return entries
}

func messageEventStateMapFromEntries(entries []messageEventStatesRPCEntry) map[metadb.MessageEventMessageKey][]metadb.MessageEventState {
	out := make(map[metadb.MessageEventMessageKey][]metadb.MessageEventState, len(entries))
	for _, entry := range entries {
		if len(entry.States) == 0 {
			continue
		}
		out[entry.Key] = cloneMessageEventStates(entry.States)
	}
	return out
}

func normalizeClusterMessageEventAppend(event metadb.MessageEventAppend) (metadb.MessageEventAppend, error) {
	event.ChannelID = strings.TrimSpace(event.ChannelID)
	event.ClientMsgNo = strings.TrimSpace(event.ClientMsgNo)
	event.RunID = strings.TrimSpace(event.RunID)
	event.AuthorizationFence = strings.TrimSpace(event.AuthorizationFence)
	event.EventID = strings.TrimSpace(event.EventID)
	event.EventKey = strings.TrimSpace(event.EventKey)
	event.EventType = strings.ToLower(strings.TrimSpace(event.EventType))
	event.Visibility = strings.TrimSpace(event.Visibility)
	event.Payload = cloneBytes(event.Payload)
	if event.ChannelID == "" || event.ChannelType <= 0 || event.ClientMsgNo == "" || event.RunID == "" || event.AuthorizationFence == "" || event.AuthoritySequence == 0 || event.EventID == "" || event.EventType == "" {
		return metadb.MessageEventAppend{}, metadb.ErrInvalidArgument
	}
	if event.EventKey == "" {
		event.EventKey = metadb.EventKeyDefault
	}
	if event.Visibility == "" {
		event.Visibility = metadb.VisibilityPublic
	}
	switch event.EventType {
	case metadb.EventTypeOpen,
		metadb.EventTypeDelta,
		metadb.EventTypeSnapshot,
		metadb.EventTypeFinish:
		return event, nil
	default:
		return metadb.MessageEventAppend{}, metadb.ErrInvalidArgument
	}
}

func (n *Node) appendMessageEventLocal(ctx context.Context, event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, error) {
	releaseAdmission, err := n.acquireWriteAdmission()
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	defer releaseAdmission()
	event, err = normalizeClusterMessageEventAppend(event)
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	route, err := n.RouteKey(event.ChannelID)
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	if route.Leader != n.cfg.NodeID {
		return metadb.MessageEventAppendResult{}, ErrNotLeader
	}
	if n.defaultSlotMetaDB != nil {
		applied, found, err := n.defaultSlotMetaDB.ForHashSlot(route.HashSlot).GetMessageEventApplied(ctx, event.ChannelID, event.ChannelType, event.ClientMsgNo, event.EventID)
		if err != nil {
			return metadb.MessageEventAppendResult{}, err
		}
		if found {
			if applied.EventDigest != metadb.MessageEventDigest(event) {
				return metadb.MessageEventAppendResult{}, metadb.ErrStaleMeta
			}
			return metadb.MessageEventAppendResult{Applied: false, ChannelID: event.ChannelID, ChannelType: event.ChannelType, ClientMsgNo: event.ClientMsgNo, RunID: event.RunID, EventID: event.EventID, EventKey: applied.EventKey, MsgEventSeq: applied.MsgEventSeq, Status: applied.Status}, nil
		}
	}
	runTerminal, err := n.messageEventRunTerminalState(ctx, route.HashSlot, event)
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	if runTerminal {
		return metadb.MessageEventAppendResult{}, ErrMessageEventRunTerminal
	}
	if isMessageEventCacheOnlyEvent(event.EventType) {
		start := time.Now()
		result, observation, err := n.messageEventStreamCache.appendCachedObserved(event)
		n.observeMessageEventAppend(messageEventPathCache, event, messageEventResultForError(err), time.Since(start))
		n.setMessageEventStreamCache(observation)
		return result, err
	}
	if event.EventType == metadb.EventTypeFinish {
		if result, applied, err := n.messageEventStreamCache.terminalResult(event); err != nil || applied {
			if applied {
				result.Applied = false
			}
			return result, err
		}
		return n.appendMessageEventFinishLocal(ctx, event)
	}
	start := time.Now()
	result, err := n.appendMessageEventDurable(ctx, event)
	n.observeMessageEventAppend(messageEventPathDurable, event, messageEventResultForError(err), time.Since(start))
	return result, err
}

func (n *Node) appendMessageEventFinishLocal(ctx context.Context, event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, error) {
	start := time.Now()
	stageStart := time.Now()
	var err error
	event, openStates, err := n.messageEventStreamCache.prepareFinish(event)
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	cacheOpenDur := time.Since(stageStart)
	hasDurableRun, err := n.hasDurableMessageEventRun(ctx, event)
	if err != nil {
		n.messageEventStreamCache.abortFinish(event)
		return metadb.MessageEventAppendResult{}, err
	}
	if len(openStates) == 0 && !hasDurableRun && !messageEventPayloadHasSnapshot(event.Payload) {
		n.messageEventStreamCache.abortFinish(event)
		n.observeMessageEventAppendStage(messageEventPathFinishBatch, messageEventResultCacheMiss, messageEventAppendStageFinishCacheOpen, cacheOpenDur)
		n.observeMessageEventAppend(messageEventPathFinishBatch, event, messageEventResultCacheMiss, time.Since(start))
		n.setMessageEventStreamCache(n.messageEventStreamCache.observation())
		return metadb.MessageEventAppendResult{}, ErrMessageEventStreamCacheMiss
	}
	stageStart = time.Now()
	events := make([]metadb.MessageEventAppend, 0, len(openStates)+1)
	for _, state := range openStates {
		if state.EventKey == event.EventKey {
			event.Payload = mergeMessageEventTerminalPayload(event.Payload, state.SnapshotPayload)
			continue
		}
		projection := finishFlushMessageEvent(event, state)
		projection.ProjectionOnly = true
		projection.Payload = cloneBytes(state.SnapshotPayload)
		events = append(events, projection)
	}
	events = append(events, event)
	batchBuildDur := time.Since(stageStart)
	var (
		result metadb.MessageEventAppendResult
		path   string
	)
	result, path, err = n.appendMessageEventFinishPrepared(ctx, event, events)
	appendResult := messageEventResultForError(err)
	n.observeMessageEventAppendStage(path, appendResult, messageEventAppendStageFinishCacheOpen, cacheOpenDur)
	n.observeMessageEventAppendStage(path, appendResult, messageEventAppendStageFinishBatchBuild, batchBuildDur)
	if err != nil {
		n.messageEventStreamCache.abortFinish(event)
		n.observeMessageEventAppend(path, event, appendResult, time.Since(start))
		return metadb.MessageEventAppendResult{}, err
	}
	stageStart = time.Now()
	observation := n.messageEventStreamCache.completeRunObserved(event, result)
	n.observeMessageEventAppendStage(path, messageEventResultOK, messageEventAppendStageFinishCacheRemove, time.Since(stageStart))
	n.setMessageEventStreamCache(observation)
	n.observeMessageEventAppend(path, event, messageEventResultOK, time.Since(start))
	return result, nil
}

func (n *Node) messageEventRunTerminalState(ctx context.Context, hashSlot uint16, event metadb.MessageEventAppend) (bool, error) {
	if n == nil || n.defaultSlotMetaDB == nil {
		return false, nil
	}
	cursor, found, err := n.defaultSlotMetaDB.ForHashSlot(hashSlot).GetMessageEventCursor(ctx, event.ChannelID, event.ChannelType, event.ClientMsgNo, event.RunID)
	if err != nil {
		return false, err
	}
	return found && cursor.Terminal, nil
}

func (n *Node) hasDurableMessageEventRun(ctx context.Context, event metadb.MessageEventAppend) (bool, error) {
	if n == nil || n.defaultSlotMetaDB == nil {
		return false, nil
	}
	route, err := n.RouteKey(event.ChannelID)
	if err != nil {
		return false, err
	}
	states, err := n.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ListMessageEventStates(ctx, event.ChannelID, event.ChannelType, event.ClientMsgNo, maxMessageEventLanesPerRun+1)
	if err != nil {
		return false, err
	}
	for _, state := range states {
		if state.RunID == event.RunID && state.LastEventType != metadb.EventTypeFinish {
			return true, nil
		}
	}
	return false, nil
}

func (n *Node) appendMessageEventFinishPrepared(ctx context.Context, event metadb.MessageEventAppend, events []metadb.MessageEventAppend) (metadb.MessageEventAppendResult, string, error) {
	if n == nil || n.messageEventFinishCoalescer == nil {
		return n.appendMessageEventFinishPreparedDirect(ctx, events)
	}
	return n.messageEventFinishCoalescer.append(ctx, n, event, events)
}

func (n *Node) appendMessageEventFinishPreparedDirect(ctx context.Context, events []metadb.MessageEventAppend) (metadb.MessageEventAppendResult, string, error) {
	if len(events) == 1 {
		result, err := n.appendMessageEventDurable(ctx, events[0])
		return result, messageEventPathDurable, err
	}
	results, err := n.appendMessageEventsDurableResults(ctx, events, messageEventPathFinishBatch)
	if err != nil {
		return metadb.MessageEventAppendResult{}, messageEventPathFinishBatch, err
	}
	return results[len(results)-1], messageEventPathFinishBatch, nil
}

func (c *messageEventFinishCoalescer) append(ctx context.Context, n *Node, event metadb.MessageEventAppend, events []metadb.MessageEventAppend) (metadb.MessageEventAppendResult, string, error) {
	if c == nil {
		return n.appendMessageEventFinishPreparedDirect(ctx, events)
	}
	req := &messageEventFinishCoalesceRequest{
		ctx:    ctx,
		event:  event,
		events: cloneMessageEventAppends(events),
		done:   make(chan messageEventFinishCoalesceResult, 1),
	}
	key := messageEventFinishCoalesceKey{channelID: event.ChannelID, channelType: event.ChannelType, clientMsgNo: event.ClientMsgNo, runID: event.RunID}
	c.mu.Lock()
	group := c.groups[key]
	if group == nil {
		group = &messageEventFinishCoalesceGroup{}
		c.groups[key] = group
		time.AfterFunc(c.window, func() { c.flush(n, key) })
	}
	group.requests = append(group.requests, req)
	c.mu.Unlock()

	select {
	case result := <-req.done:
		return result.result, result.path, result.err
	case <-ctx.Done():
		if c.remove(key, req) {
			return metadb.MessageEventAppendResult{}, messageEventPathFinishBatch, ctx.Err()
		}
		result := <-req.done
		return result.result, result.path, result.err
	}
}

func (c *messageEventFinishCoalescer) remove(key messageEventFinishCoalesceKey, req *messageEventFinishCoalesceRequest) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	group := c.groups[key]
	if group == nil {
		return false
	}
	for i, candidate := range group.requests {
		if candidate != req {
			continue
		}
		group.requests = append(group.requests[:i], group.requests[i+1:]...)
		if len(group.requests) == 0 {
			delete(c.groups, key)
		}
		return true
	}
	return false
}

func (c *messageEventFinishCoalescer) flush(n *Node, key messageEventFinishCoalesceKey) {
	c.mu.Lock()
	group := c.groups[key]
	if group == nil {
		c.mu.Unlock()
		return
	}
	delete(c.groups, key)
	requests := append([]*messageEventFinishCoalesceRequest(nil), group.requests...)
	c.mu.Unlock()

	if len(requests) == 0 {
		return
	}
	live := requests[:0]
	for _, req := range requests {
		if req.ctx != nil && req.ctx.Err() != nil {
			req.done <- messageEventFinishCoalesceResult{path: messageEventPathFinishBatch, err: req.ctx.Err()}
			continue
		}
		live = append(live, req)
	}
	if len(live) == 0 {
		return
	}
	primary := live[0]
	path := messageEventPathFinishBatch
	results, err := n.appendMessageEventsCoalesced(messageEventFinishCoalesceContext(live), primary.events)
	if len(primary.events) == 1 {
		path = messageEventPathDurable
	}
	if err != nil {
		for _, req := range live {
			req.done <- messageEventFinishCoalesceResult{path: path, err: err}
		}
		return
	}
	byEvent := make(map[messageEventResultKey]metadb.MessageEventAppendResult, len(results))
	for _, result := range results {
		byEvent[messageEventResultKey{clientMsgNo: result.ClientMsgNo, eventID: result.EventID}] = result
	}
	for index, req := range live {
		if index > 0 && req.event.EventID != primary.event.EventID {
			req.done <- messageEventFinishCoalesceResult{path: path, err: ErrMessageEventRunTerminal}
			continue
		}
		result, ok := byEvent[messageEventResultKey{clientMsgNo: req.event.ClientMsgNo, eventID: req.event.EventID}]
		if !ok {
			req.done <- messageEventFinishCoalesceResult{path: path, err: metadb.ErrCorruptValue}
			continue
		}
		if index > 0 {
			result.Applied = false
		}
		req.done <- messageEventFinishCoalesceResult{result: result, path: path}
	}
}

func (n *Node) appendMessageEventsCoalesced(ctx context.Context, events []metadb.MessageEventAppend) ([]metadb.MessageEventAppendResult, error) {
	if len(events) == 1 {
		result, err := n.appendMessageEventDurable(ctx, events[0])
		if err != nil {
			return nil, err
		}
		return []metadb.MessageEventAppendResult{result}, nil
	}
	return n.appendMessageEventsDurableResults(ctx, events, messageEventPathFinishBatch)
}

func messageEventFinishCoalesceContext(requests []*messageEventFinishCoalesceRequest) context.Context {
	for _, req := range requests {
		if req.ctx != nil && req.ctx.Err() == nil {
			return req.ctx
		}
	}
	return context.Background()
}

type messageEventResultKey struct {
	clientMsgNo string
	eventID     string
}

func (n *Node) clearMessageEventStreamCacheForLostLocalAuthority(before, after *routing.Table) {
	lost := n.messageEventLostLocalAuthorityHashSlots(before, after)
	if len(lost) == 0 {
		return
	}
	n.setMessageEventStreamCache(n.messageEventStreamCache.removeHashSlotsObserved(lost, before.HashSlotCount))
}

func (n *Node) messageEventLostLocalAuthorityHashSlots(before, after *routing.Table) map[uint16]struct{} {
	if n == nil || n.messageEventStreamCache == nil || before == nil || after == nil || before.HashSlotCount == 0 {
		return nil
	}
	lost := make(map[uint16]struct{})
	for hashSlot, slotID := range before.HashToSlot {
		if slotID == 0 || before.SlotLeaders[slotID] != n.cfg.NodeID {
			continue
		}
		current, ok := routeAuthorityFromTable(after, uint16(hashSlot))
		if !ok || current.leaderNodeID != n.cfg.NodeID {
			lost[uint16(hashSlot)] = struct{}{}
		}
	}
	return lost
}

func finishFlushMessageEvent(finish metadb.MessageEventAppend, state metadb.MessageEventState) metadb.MessageEventAppend {
	event := finish
	event.EventID = finishFlushMessageEventID(finish.EventID, state.EventKey)
	event.EventKey = state.EventKey
	event.EventType = metadb.EventTypeSnapshot
	event.Payload = cloneBytes(state.SnapshotPayload)
	event.AuthoritySequence = state.LastAuthoritySequence
	event.MsgEventSeq = state.LastMsgEventSeq
	event.OccurredAt = state.LastOccurredAt
	event.UpdatedAt = state.UpdatedAt
	return event
}

func finishFlushMessageEventID(finishEventID string, eventKey string) string {
	return finishEventID + "/flush/" + eventKey
}

func (n *Node) appendMessageEventDurable(ctx context.Context, event metadb.MessageEventAppend) (metadb.MessageEventAppendResult, error) {
	encodeStart := time.Now()
	command, err := metafsm.EncodeAppendMessageEventCommandChecked(event)
	encodeDur := time.Since(encodeStart)
	if err != nil {
		n.observeMessageEventProposeStage(messageEventPathDurable, messageEventResultForError(err), messageEventProposeStageEncode, encodeDur)
		return metadb.MessageEventAppendResult{}, err
	}
	start := time.Now()
	proposeStart := time.Now()
	resultBytes, err := n.ProposeResult(n.messageEventProposeStageContext(ctx, messageEventPathDurable), ProposeRequest{Key: event.ChannelID, Command: command})
	proposeDur := time.Since(proposeStart)
	if err != nil {
		resultClass := messageEventResultForError(err)
		n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageEncode, encodeDur)
		n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageSlotProposeWait, proposeDur)
		n.observeMessageEventPropose(messageEventPathDurable, resultClass, 1, time.Since(start))
		return metadb.MessageEventAppendResult{}, err
	}
	decodeStart := time.Now()
	result, err := metafsm.DecodeAppendMessageEventResult(resultBytes)
	decodeDur := time.Since(decodeStart)
	if err != nil {
		resultClass := messageEventResultForError(err)
		if string(resultBytes) == metafsm.ApplyResultStaleMeta {
			resultClass = messageEventResultInvalid
			n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageEncode, encodeDur)
			n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageSlotProposeWait, proposeDur)
			n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageDecode, decodeDur)
			n.observeMessageEventPropose(messageEventPathDurable, resultClass, 1, time.Since(start))
			return metadb.MessageEventAppendResult{}, metadb.ErrStaleMeta
		}
		n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageEncode, encodeDur)
		n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageSlotProposeWait, proposeDur)
		n.observeMessageEventProposeStage(messageEventPathDurable, resultClass, messageEventProposeStageDecode, decodeDur)
		n.observeMessageEventPropose(messageEventPathDurable, resultClass, 1, time.Since(start))
		return metadb.MessageEventAppendResult{}, err
	}
	n.observeMessageEventProposeStage(messageEventPathDurable, messageEventResultOK, messageEventProposeStageEncode, encodeDur)
	n.observeMessageEventProposeStage(messageEventPathDurable, messageEventResultOK, messageEventProposeStageSlotProposeWait, proposeDur)
	n.observeMessageEventProposeStage(messageEventPathDurable, messageEventResultOK, messageEventProposeStageDecode, decodeDur)
	n.observeMessageEventPropose(messageEventPathDurable, messageEventResultOK, 1, time.Since(start))
	return result, nil
}

func (n *Node) appendMessageEventsDurable(ctx context.Context, events []metadb.MessageEventAppend) (metadb.MessageEventAppendResult, error) {
	results, err := n.appendMessageEventsDurableResults(ctx, events, messageEventPathFinishBatch)
	if err != nil {
		return metadb.MessageEventAppendResult{}, err
	}
	return results[len(results)-1], nil
}

func (n *Node) appendMessageEventsDurableResults(ctx context.Context, events []metadb.MessageEventAppend, path string) ([]metadb.MessageEventAppendResult, error) {
	encodeStart := time.Now()
	command, err := metafsm.EncodeAppendMessageEventsCommandChecked(events)
	encodeDur := time.Since(encodeStart)
	if err != nil {
		n.observeMessageEventProposeStage(path, messageEventResultForError(err), messageEventProposeStageEncode, encodeDur)
		return nil, err
	}
	start := time.Now()
	proposeStart := time.Now()
	resultBytes, err := n.ProposeResult(n.messageEventProposeStageContext(ctx, path), ProposeRequest{Key: events[len(events)-1].ChannelID, Command: command})
	proposeDur := time.Since(proposeStart)
	if err != nil {
		resultClass := messageEventResultForError(err)
		n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageEncode, encodeDur)
		n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageSlotProposeWait, proposeDur)
		n.observeMessageEventPropose(path, resultClass, len(events), time.Since(start))
		return nil, err
	}
	decodeStart := time.Now()
	results, err := metafsm.DecodeAppendMessageEventResults(resultBytes)
	decodeDur := time.Since(decodeStart)
	if err != nil {
		resultClass := messageEventResultForError(err)
		if string(resultBytes) == metafsm.ApplyResultStaleMeta {
			resultClass = messageEventResultInvalid
			n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageEncode, encodeDur)
			n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageSlotProposeWait, proposeDur)
			n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageDecode, decodeDur)
			n.observeMessageEventPropose(path, resultClass, len(events), time.Since(start))
			return nil, metadb.ErrStaleMeta
		}
		n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageEncode, encodeDur)
		n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageSlotProposeWait, proposeDur)
		n.observeMessageEventProposeStage(path, resultClass, messageEventProposeStageDecode, decodeDur)
		n.observeMessageEventPropose(path, resultClass, len(events), time.Since(start))
		return nil, err
	}
	n.observeMessageEventProposeStage(path, messageEventResultOK, messageEventProposeStageEncode, encodeDur)
	n.observeMessageEventProposeStage(path, messageEventResultOK, messageEventProposeStageSlotProposeWait, proposeDur)
	n.observeMessageEventProposeStage(path, messageEventResultOK, messageEventProposeStageDecode, decodeDur)
	n.observeMessageEventPropose(path, messageEventResultOK, len(events), time.Since(start))
	return results, nil
}

func (n *Node) messageEventProposeStageContext(ctx context.Context, path string) context.Context {
	next := propose.StageObserverFromContext(ctx)
	if n == nil || n.cfg.MessageEvent.Observer == nil {
		return ctx
	}
	if _, ok := n.cfg.MessageEvent.Observer.(MessageEventProposeStageObserver); !ok && next == nil {
		return ctx
	}
	return propose.WithStageObserver(ctx, messageEventProposeStageAdapter{
		node: n,
		path: path,
		next: next,
	})
}

func messageEventCacheKey(event metadb.MessageEventAppend) messageEventStreamCacheKey {
	return messageEventStreamCacheKey{
		channelID:   event.ChannelID,
		channelType: event.ChannelType,
		clientMsgNo: event.ClientMsgNo,
	}
}

func cachedMessageEventState(event metadb.MessageEventAppend) metadb.MessageEventState {
	return metadb.MessageEventState{
		ChannelID:             event.ChannelID,
		ChannelType:           event.ChannelType,
		ClientMsgNo:           event.ClientMsgNo,
		RunID:                 event.RunID,
		EventKey:              event.EventKey,
		Status:                metadb.EventStatusOpen,
		LastEventID:           event.EventID,
		LastEventType:         event.EventType,
		LastVisibility:        event.Visibility,
		LastOccurredAt:        event.OccurredAt,
		UpdatedAt:             event.UpdatedAt,
		LastMsgEventSeq:       event.MsgEventSeq,
		LastAuthoritySequence: event.AuthoritySequence,
		EndReason:             0,
		Error:                 "",
	}
}

func cachedMessageEventResult(event metadb.MessageEventAppend, state metadb.MessageEventState) metadb.MessageEventAppendResult {
	state = cloneMessageEventState(state)
	return metadb.MessageEventAppendResult{
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

func reduceCachedMessageEventDelta(existing []byte, payload []byte) []byte {
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

func cachedMessageEventSnapshotFromPayload(payload []byte) []byte {
	var body struct {
		Snapshot json.RawMessage `json:"snapshot"`
	}
	if json.Unmarshal(payload, &body) != nil || len(body.Snapshot) == 0 || string(body.Snapshot) == "null" {
		return nil
	}
	return cloneBytes(body.Snapshot)
}

func mergeMessageEventTerminalPayload(payload []byte, snapshot []byte) []byte {
	if len(snapshot) == 0 {
		return cloneBytes(payload)
	}
	body := map[string]json.RawMessage{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &body); err != nil {
			raw, marshalErr := json.Marshal(string(payload))
			if marshalErr != nil {
				return cloneBytes(payload)
			}
			body["raw_payload"] = raw
		}
	}
	if raw, exists := body["snapshot"]; !exists || isEmptyMessageEventSnapshotRaw(raw) {
		body["snapshot"] = cloneJSONRawMessage(snapshot)
	}
	out, err := json.Marshal(body)
	if err != nil {
		return cloneBytes(payload)
	}
	return out
}

func messageEventPayloadHasSnapshot(payload []byte) bool {
	body := map[string]json.RawMessage{}
	if len(payload) == 0 || json.Unmarshal(payload, &body) != nil {
		return false
	}
	raw, exists := body["snapshot"]
	return exists && !isEmptyMessageEventSnapshotRaw(raw)
}

func isEmptyMessageEventSnapshotRaw(raw json.RawMessage) bool {
	switch strings.TrimSpace(string(raw)) {
	case "", "null":
		return true
	default:
		return false
	}
}

func cloneJSONRawMessage(in []byte) json.RawMessage {
	if json.Valid(in) {
		return cloneBytes(in)
	}
	out, err := json.Marshal(string(in))
	if err != nil {
		return nil
	}
	return out
}

func isMessageEventCacheOnlyEvent(eventType string) bool {
	switch eventType {
	case metadb.EventTypeOpen, metadb.EventTypeDelta, metadb.EventTypeSnapshot:
		return true
	default:
		return false
	}
}

func isMessageEventTerminalEvent(eventType string) bool {
	return eventType == metadb.EventTypeFinish
}

func isMessageEventTerminalStatus(status string) bool {
	switch status {
	case metadb.EventStatusClosed, metadb.EventStatusError, metadb.EventStatusCancelled:
		return true
	default:
		return false
	}
}

func cloneMessageEventState(state metadb.MessageEventState) metadb.MessageEventState {
	state.SnapshotPayload = cloneBytes(state.SnapshotPayload)
	return state
}

func cloneMessageEventStates(states []metadb.MessageEventState) []metadb.MessageEventState {
	out := make([]metadb.MessageEventState, len(states))
	for i, state := range states {
		out[i] = cloneMessageEventState(state)
	}
	return out
}

func cloneMessageEventAppends(events []metadb.MessageEventAppend) []metadb.MessageEventAppend {
	out := make([]metadb.MessageEventAppend, len(events))
	for i, event := range events {
		out[i] = event
		out[i].Payload = cloneBytes(event.Payload)
	}
	return out
}

func cloneMessageEventAppendResult(result metadb.MessageEventAppendResult) metadb.MessageEventAppendResult {
	result.State = cloneMessageEventState(result.State)
	return result
}

func lightweightMessageEventAppendResult(result metadb.MessageEventAppendResult) metadb.MessageEventAppendResult {
	result.State.SnapshotPayload = nil
	return result
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
