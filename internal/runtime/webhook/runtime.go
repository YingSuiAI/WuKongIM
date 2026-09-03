package webhook

import (
	"context"
	"errors"
	"sync"
	"time"

	goruntimeregistry "github.com/WuKongIM/WuKongIM/pkg/goroutine"
	"github.com/WuKongIM/WuKongIM/pkg/workqueue"
)

const (
	webhookNotifyQueue  = "webhook_notify"
	webhookOfflineQueue = "webhook_offline"
	webhookOnlineQueue  = "webhook_presence_lease"

	resultAccepted       = "accepted"
	resultOK             = "ok"
	resultFull           = "full"
	resultClosed         = "closed"
	resultCanceled       = "canceled"
	resultTimeout        = "timeout"
	resultError          = "error"
	resultRetry          = "retry"
	resultRetryExhausted = "retry_exhausted"
	resultEncodeError    = "encode_error"

	defaultOfflineShards = 256
)

type runtimeState uint8

const (
	runtimeStateNew runtimeState = iota
	runtimeStateStarted
	runtimeStateStopping
	runtimeStateStopped
)

// RuntimeOptions configures the bounded webhook runtime.
type RuntimeOptions struct {
	// Goroutines receives lifecycle and pool ownership observations.
	Goroutines *goruntimeregistry.Registry
	// Sender delivers encoded webhook requests.
	Sender Sender
	// Observer receives structured runtime observations.
	Observer Observer
	// QueueSize bounds accepted webhook events waiting in memory per event queue.
	QueueSize int
	// Workers bounds concurrent sender calls per event queue.
	Workers int
	// NotifyBatchMaxItems limits msg.notify messages sent in one request.
	NotifyBatchMaxItems int
	// NotifyBatchMaxWait bounds how long msg.notify waits for adjacent messages.
	NotifyBatchMaxWait time.Duration
	// OnlineBatchMaxItems limits user.onlinestatus records sent in one request.
	OnlineBatchMaxItems int
	// OnlineBatchMaxWait bounds how long user.onlinestatus waits for adjacent records.
	OnlineBatchMaxWait time.Duration
	// OfflineUIDBatchSize is the compression threshold used for msg.offline UID chunks.
	OfflineUIDBatchSize int
	// RequestTimeout bounds one outbound sender attempt.
	RequestTimeout time.Duration
	// RetryMaxAttempts bounds attempts for one admitted webhook batch before drop.
	RetryMaxAttempts int
	// FocusEvents limits delivered event names. Empty means all events are delivered.
	FocusEvents []string
}

// Runtime owns bounded webhook event admission, batching, retry, and delivery.
type Runtime struct {
	opts     RuntimeOptions
	sender   Sender
	observer Observer
	focus    map[string]struct{}

	mu      sync.RWMutex
	state   runtimeState
	stopCh  chan struct{}
	notify  *workqueue.BoundedBatchPool[Message]
	online  *workqueue.BoundedBatchPool[PresenceLease]
	offline *workqueue.ShardedMailbox[OfflineMessage]
}

// New creates a webhook runtime. Start opens the queues.
func New(opts RuntimeOptions) (*Runtime, error) {
	if opts.Sender == nil || opts.QueueSize <= 0 || opts.Workers <= 0 {
		return nil, workqueue.ErrInvalidConfig
	}
	if opts.NotifyBatchMaxWait < 0 || opts.OnlineBatchMaxWait < 0 || opts.RequestTimeout <= 0 {
		return nil, workqueue.ErrInvalidConfig
	}
	if opts.NotifyBatchMaxItems < 0 || opts.OnlineBatchMaxItems < 0 || opts.OfflineUIDBatchSize < 0 {
		return nil, workqueue.ErrInvalidConfig
	}
	if opts.NotifyBatchMaxItems == 0 {
		opts.NotifyBatchMaxItems = 1
	}
	if opts.OnlineBatchMaxItems == 0 {
		opts.OnlineBatchMaxItems = 1
	}
	rt := &Runtime{
		opts:     opts,
		sender:   opts.Sender,
		observer: opts.Observer,
		focus:    make(map[string]struct{}, len(opts.FocusEvents)),
		stopCh:   make(chan struct{}),
	}
	for _, event := range opts.FocusEvents {
		if event != "" {
			rt.focus[event] = struct{}{}
		}
	}
	return rt, nil
}

// Start opens bounded queue admission.
func (r *Runtime) Start(ctx context.Context) error {
	if r == nil {
		return workqueue.ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case runtimeStateStarted:
		return nil
	case runtimeStateStopping:
		return workqueue.ErrClosed
	case runtimeStateStopped:
		r.stopCh = make(chan struct{})
	}
	r.ensureStopChLocked()

	notify, err := workqueue.NewBoundedBatchPool[Message](workqueue.BoundedBatchPoolConfig[Message]{
		Name:       webhookNotifyQueue,
		Goroutines: r.opts.Goroutines,
		Task:       goruntimeregistry.TaskWebhookNotify,
		Workers:    r.opts.Workers,
		QueueSize:  r.opts.QueueSize,
		Policy: func(Message) workqueue.BatchOptions {
			return workqueue.BatchOptions{MaxItems: r.opts.NotifyBatchMaxItems, MaxWait: r.opts.NotifyBatchMaxWait}
		},
	}, r.handleNotifyBatch)
	if err != nil {
		return err
	}

	online, err := workqueue.NewBoundedBatchPool[PresenceLease](workqueue.BoundedBatchPoolConfig[PresenceLease]{
		Name:       webhookOnlineQueue,
		Goroutines: r.opts.Goroutines,
		Task:       goruntimeregistry.TaskWebhookOnline,
		Workers:    r.opts.Workers,
		QueueSize:  r.opts.QueueSize,
		Policy: func(PresenceLease) workqueue.BatchOptions {
			return workqueue.BatchOptions{MaxItems: r.opts.OnlineBatchMaxItems, MaxWait: r.opts.OnlineBatchMaxWait}
		},
	}, r.handleOnlineBatch)
	if err != nil {
		_ = notify.Close(context.Background())
		return err
	}

	shards, queueSizePerShard := offlineMailboxSizing(r.opts.QueueSize)
	offline, err := workqueue.NewShardedMailbox[OfflineMessage](workqueue.ShardedMailboxConfig{
		Name:              webhookOfflineQueue,
		Goroutines:        r.opts.Goroutines,
		Task:              goruntimeregistry.TaskWebhookOffline,
		Shards:            shards,
		Workers:           r.opts.Workers,
		QueueSizePerShard: queueSizePerShard,
		BatchMaxItems:     1,
	}, r.handleOfflineBatch)
	if err != nil {
		_ = online.Close(context.Background())
		_ = notify.Close(context.Background())
		return err
	}

	r.notify = notify
	r.online = online
	r.offline = offline
	r.state = runtimeStateStarted
	return nil
}

// Stop closes admission and drains accepted webhook work until ctx expires.
func (r *Runtime) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	switch r.state {
	case runtimeStateNew:
		r.state = runtimeStateStopped
		close(r.ensureStopChLocked())
		r.mu.Unlock()
		return nil
	case runtimeStateStopped:
		r.mu.Unlock()
		return nil
	case runtimeStateStopping:
		stopCh := r.ensureStopChLocked()
		r.mu.Unlock()
		return waitForStop(ctx, stopCh)
	}
	r.state = runtimeStateStopping
	notify := r.notify
	online := r.online
	offline := r.offline
	stopCh := r.ensureStopChLocked()
	r.mu.Unlock()

	err := errors.Join(
		notify.Close(ctx),
		online.Close(ctx),
		offline.Close(ctx),
	)

	r.mu.Lock()
	r.notify = nil
	r.online = nil
	r.offline = nil
	r.state = runtimeStateStopped
	close(stopCh)
	r.mu.Unlock()
	return err
}

// Notify admits one committed message for msg.notify delivery.
func (r *Runtime) Notify(ctx context.Context, msg Message) {
	if r == nil || !r.enabled(EventMsgNotify) {
		return
	}
	msg = cloneMessage(msg)

	r.mu.RLock()
	notify := r.notify
	if r.state != runtimeStateStarted || notify == nil {
		r.mu.RUnlock()
		r.observeAdmission(EventMsgNotify, webhookNotifyQueue, resultClosed, 1, 0, r.opts.QueueSize, nil)
		return
	}
	err := notify.Submit(ctx, msg)
	depth := notify.QueueDepth()
	capacity := notify.QueueCapacity()
	r.mu.RUnlock()
	r.observeAdmission(EventMsgNotify, webhookNotifyQueue, admissionResult(err), 1, depth, capacity, err)
}

// Offline admits one bounded recipient chunk for msg.offline delivery.
func (r *Runtime) Offline(ctx context.Context, msg OfflineMessage) {
	if r == nil || !r.enabled(EventMsgOffline) {
		return
	}
	msg = cloneOfflineMessage(msg)
	items := len(msg.ToUIDs)

	r.mu.RLock()
	offline := r.offline
	if r.state != runtimeStateStarted || offline == nil {
		r.mu.RUnlock()
		r.observeAdmission(EventMsgOffline, webhookOfflineQueue, resultClosed, items, 0, r.opts.QueueSize, nil)
		return
	}
	err := offline.Submit(ctx, msg.Message.ChannelID, msg)
	depth := offline.QueueDepth()
	r.mu.RUnlock()
	r.observeAdmission(EventMsgOffline, webhookOfflineQueue, admissionResult(err), items, depth, r.opts.QueueSize, err)
}

// PresenceLease admits one typed installation lease transition.
func (r *Runtime) PresenceLease(ctx context.Context, status PresenceLease) {
	if r == nil || !r.enabled(EventPresenceLease) || status.ConnectionID == "" {
		return
	}

	r.mu.RLock()
	online := r.online
	if r.state != runtimeStateStarted || online == nil {
		r.mu.RUnlock()
		r.observeAdmission(EventPresenceLease, webhookOnlineQueue, resultClosed, 1, 0, r.opts.QueueSize, nil)
		return
	}
	err := online.Submit(ctx, status)
	depth := online.QueueDepth()
	capacity := online.QueueCapacity()
	r.mu.RUnlock()
	r.observeAdmission(EventPresenceLease, webhookOnlineQueue, admissionResult(err), 1, depth, capacity, err)
}

func (r *Runtime) handleNotifyBatch(ctx context.Context, batch []Message) error {
	body, err := buildNotifyBody(batch)
	if err != nil {
		r.observeSend(EventMsgNotify, resultEncodeError, len(batch), 0, 0, "", observationIdentityForMessages(batch), err)
		return nil
	}
	r.sendWithRetryIdentity(ctx, EventMsgNotify, body, len(batch), observationIdentityForMessages(batch))
	return nil
}

func (r *Runtime) handleOnlineBatch(ctx context.Context, batch []PresenceLease) error {
	body, err := buildPresenceLeaseBody(batch)
	if err != nil {
		r.observeSend(EventPresenceLease, resultEncodeError, len(batch), 0, 0, "", observationIdentity{}, err)
		return nil
	}
	r.sendWithRetry(ctx, EventPresenceLease, body, len(batch))
	return nil
}

func (r *Runtime) handleOfflineBatch(ctx context.Context, batch workqueue.MailboxBatch[OfflineMessage]) error {
	for _, item := range batch.Items {
		body, err := buildOfflineBody(item, r.opts.OfflineUIDBatchSize)
		items := len(item.ToUIDs)
		if err != nil {
			r.observeSend(EventMsgOffline, resultEncodeError, items, 0, 0, "", observationIdentityForMessage(item.Message), err)
			continue
		}
		r.sendWithRetryIdentity(ctx, EventMsgOffline, body, items, observationIdentityForMessage(item.Message))
	}
	return nil
}

func (r *Runtime) sendWithRetry(ctx context.Context, event string, body []byte, items int) {
	r.sendWithRetryIdentity(ctx, event, body, items, observationIdentity{})
}

func (r *Runtime) sendWithRetryIdentity(ctx context.Context, event string, body []byte, items int, identity observationIdentity) {
	if ctx == nil {
		ctx = context.Background()
	}
	attempts := r.opts.RetryMaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	requestID, err := newRequestID()
	if err != nil {
		r.observeSend(event, resultError, items, 0, 0, "", identity, err)
		return
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			r.observeSend(event, contextResult(err), items, attempt, 0, requestID, identity, err)
			return
		}
		attemptCtx := ctx
		cancel := func() {}
		if r.opts.RequestTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, r.opts.RequestTimeout)
		}
		started := time.Now()
		err := r.sender.Send(attemptCtx, SendRequest{Event: event, RequestID: requestID, Body: body})
		cancel()
		duration := time.Since(started)
		if err == nil {
			r.observeSend(event, resultOK, items, attempt, duration, requestID, identity, nil)
			return
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			r.observeSend(event, contextResult(ctxErr), items, attempt, duration, requestID, identity, ctxErr)
			return
		}
		if attempt == attempts {
			r.observeSend(event, resultRetryExhausted, items, attempt, duration, requestID, identity, err)
			return
		}
		r.observeSend(event, resultRetry, items, attempt, duration, requestID, identity, err)
	}
}

type observationIdentity struct {
	ChannelID   string
	ChannelType uint8
	MessageID   uint64
	MessageSeq  uint64
}

func observationIdentityForMessages(messages []Message) observationIdentity {
	if len(messages) == 0 {
		return observationIdentity{}
	}
	identity := observationIdentityForMessage(messages[0])
	if len(messages) == 1 {
		return identity
	}
	identity.MessageID = 0
	identity.MessageSeq = 0
	for _, message := range messages[1:] {
		if message.ChannelID != identity.ChannelID || message.ChannelType != identity.ChannelType {
			identity.ChannelID = ""
			identity.ChannelType = 0
			break
		}
	}
	return identity
}

func observationIdentityForMessage(message Message) observationIdentity {
	return observationIdentity{
		ChannelID:   message.ChannelID,
		ChannelType: message.ChannelType,
		MessageID:   message.MessageID,
		MessageSeq:  message.MessageSeq,
	}
}

func (r *Runtime) enabled(event string) bool {
	if r == nil {
		return false
	}
	if len(r.focus) == 0 {
		return true
	}
	_, ok := r.focus[event]
	return ok
}

func (r *Runtime) ensureStopChLocked() chan struct{} {
	if r.stopCh == nil {
		r.stopCh = make(chan struct{})
	}
	return r.stopCh
}

func (r *Runtime) observeAdmission(event string, queue string, result string, items int, depth int, size int, err error) {
	if r == nil || r.observer == nil {
		return
	}
	r.observer.ObserveWebhook(Observation{
		Queue:      queue,
		Event:      event,
		Result:     result,
		Items:      items,
		QueueDepth: depth,
		QueueSize:  size,
		Err:        err,
	})
}

func (r *Runtime) observeSend(event string, result string, items int, attempt int, duration time.Duration, requestID string, identity observationIdentity, err error) {
	if r == nil || r.observer == nil {
		return
	}
	r.observer.ObserveWebhook(Observation{
		Queue:       queueForEvent(event),
		Event:       event,
		RequestID:   requestID,
		ChannelID:   identity.ChannelID,
		ChannelType: identity.ChannelType,
		MessageID:   identity.MessageID,
		MessageSeq:  identity.MessageSeq,
		Result:      result,
		Items:       items,
		Attempt:     attempt,
		Duration:    duration,
		Err:         err,
	})
}

func queueForEvent(event string) string {
	switch event {
	case EventMsgNotify:
		return webhookNotifyQueue
	case EventMsgOffline:
		return webhookOfflineQueue
	case EventPresenceLease:
		return webhookOnlineQueue
	default:
		return ""
	}
}

func admissionResult(err error) string {
	switch {
	case err == nil:
		return resultAccepted
	case errors.Is(err, workqueue.ErrFull):
		return resultFull
	case errors.Is(err, workqueue.ErrClosed):
		return resultClosed
	case errors.Is(err, context.Canceled):
		return resultCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return resultTimeout
	default:
		return resultError
	}
}

func contextResult(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return resultCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return resultTimeout
	default:
		return resultError
	}
}

func cloneMessage(msg Message) Message {
	msg.Payload = append([]byte(nil), msg.Payload...)
	return msg
}

func cloneOfflineMessage(msg OfflineMessage) OfflineMessage {
	msg.Message = cloneMessage(msg.Message)
	msg.ToUIDs = append([]string(nil), msg.ToUIDs...)
	return msg
}

func offlineMailboxSizing(queueSize int) (int, int) {
	if queueSize <= 1 {
		return 1, 1
	}
	maxShards := defaultOfflineShards
	if queueSize < maxShards {
		maxShards = queueSize
	}
	for shards := maxShards; shards > 1; shards-- {
		if queueSize%shards == 0 {
			return shards, queueSize / shards
		}
	}
	return 1, queueSize
}

func waitForStop(ctx context.Context, stopCh <-chan struct{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-stopCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
