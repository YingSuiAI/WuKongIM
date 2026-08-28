package cluster

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterpkg "github.com/WuKongIM/WuKongIM/pkg/cluster"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/propose"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	goruntimeregistry "github.com/WuKongIM/WuKongIM/pkg/goroutine"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

var errPersonDirectoryBatcherStopped = errors.New("cluster: person-directory batcher stopped")

const (
	personDirectoryBatchMaxItems = 128
	personDirectoryQueueMaxItems = 4096
	// Thirty-two items lets the 50ms window collect roughly one cloud-rehearsal
	// ingress-node wave and halves Slot proposal amplification. Lower rates
	// still seal on the collection deadline instead of waiting for this target.
	personDirectoryBatchTargetItems = 32
	personDirectoryBatchMaxActive   = 8
	personDirectoryBatchCollectWait = 50 * time.Millisecond
	// Admission gets one full Slot election window without consuming the
	// recovery budget needed for an authoritative read and idempotent retry.
	personDirectoryAdmissionAttemptTimeout = 5 * time.Second
	personDirectoryBatchTimeout            = 10 * time.Second
	personDirectoryRetryBackoff            = 25 * time.Millisecond
)

type personDirectoryBatchNode interface {
	AdmitPersonDirectoryTaskWaves(context.Context, []metadb.PersonDirectoryTask, func(int, error))
}

type personDirectoryAdmissionStateReader interface {
	GetChannelMetadataAuthoritative(context.Context, string, int64) (metadb.Channel, error)
}

type personDirectoryMutation struct {
	task metadb.PersonDirectoryTask
}

type personDirectoryBatchAdmission struct {
	ctx      context.Context
	mutation personDirectoryMutation
}

type personDirectoryBatchOutcome struct {
	index int
	err   error
}

func (m personDirectoryMutation) key() metadb.ChannelKey {
	return metadb.ChannelKey{ChannelID: m.task.ChannelID, ChannelType: m.task.ChannelType}
}

type personDirectoryBatcher struct {
	node       personDirectoryBatchNode
	goroutines *goruntimeregistry.Registry
	ctx        context.Context
	cancel     context.CancelFunc

	mu      sync.Mutex
	current *personDirectoryBatch
	// inflight owns every admitted Channel until its sealed durable batch
	// reaches a terminal result. It prevents later callers from starting a
	// duplicate cross-Slot transaction after current advances to a new batch.
	inflight    map[metadb.ChannelKey]*personDirectoryEntry
	queuedItems int
	// maxQueued bounds owned directory mutations independently from active batches.
	maxQueued int
	// capacity is closed and replaced whenever queued ownership is released.
	capacity    chan struct{}
	collectWait time.Duration
	targetItems int
	// timeout bounds the complete admit/read-recover lifecycle. attemptTimeout
	// bounds each potentially ambiguous proposal attempt inside that lifecycle.
	timeout        time.Duration
	attemptTimeout time.Duration
	active         chan struct{}
	stopped        bool
	owners         int
	stoppedDone    chan struct{}
	stopOnce       sync.Once
}

type personDirectoryBatch struct {
	entries map[metadb.ChannelKey]*personDirectoryEntry
	order   []*personDirectoryEntry
	trigger chan struct{}
	once    sync.Once
	sealed  bool
}

type personDirectoryEntry struct {
	mutation personDirectoryMutation
	waiters  map[*personDirectoryWaiter]struct{}
	canceled bool
}

type personDirectoryWaiter struct {
	entry  *personDirectoryEntry
	result chan error
}

func newPersonDirectoryBatcher(node personDirectoryBatchNode, goroutines *goruntimeregistry.Registry) *personDirectoryBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	if goroutines == nil {
		goroutines = goruntimeregistry.Default()
	}
	return &personDirectoryBatcher{
		node: node, goroutines: goroutines, ctx: ctx, cancel: cancel,
		collectWait: personDirectoryBatchCollectWait,
		targetItems: personDirectoryBatchTargetItems, timeout: personDirectoryBatchTimeout,
		attemptTimeout: personDirectoryAdmissionAttemptTimeout,
		active:         make(chan struct{}, personDirectoryBatchMaxActive), capacity: make(chan struct{}),
		maxQueued: personDirectoryQueueMaxItems, inflight: make(map[metadb.ChannelKey]*personDirectoryEntry), stoppedDone: make(chan struct{}),
	}
}

// Stop seals admission, cancels active durable calls, and joins every owned
// batch goroutine before returning. A timed-out caller may call Stop again to
// continue joining the same terminal lifecycle.
func (b *personDirectoryBatcher) Stop(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	if !b.stopped {
		b.stopped = true
		b.cancel()
		if b.current != nil {
			b.current.sealed = true
			b.current.once.Do(func() { close(b.current.trigger) })
		}
		b.signalCapacityLocked()
		if b.owners == 0 {
			b.stopOnce.Do(func() { close(b.stoppedDone) })
		}
	}
	done := b.stoppedDone
	b.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *personDirectoryBatcher) ensure(ctx context.Context, mutation personDirectoryMutation) error {
	return b.ensureBatch([]personDirectoryBatchAdmission{{ctx: ctx, mutation: mutation}})[0]
}

func (b *personDirectoryBatcher) ensureBatch(admissions []personDirectoryBatchAdmission) []error {
	results := make([]error, len(admissions))
	b.ensureBatchWaves(admissions, func(wave []personDirectoryBatchOutcome) {
		for _, outcome := range wave {
			results[outcome.index] = outcome.err
		}
	})
	return results
}

func (b *personDirectoryBatcher) ensureBatchWaves(admissions []personDirectoryBatchAdmission, emit func([]personDirectoryBatchOutcome)) {
	waiters := make([]*personDirectoryWaiter, len(admissions))
	contexts := make([]context.Context, len(admissions))
	immediate := make([]personDirectoryBatchOutcome, 0, len(admissions))
	for i, admission := range admissions {
		ctx := admission.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		contexts[i] = ctx
		if err := ctx.Err(); err != nil {
			immediate = append(immediate, personDirectoryBatchOutcome{index: i, err: err})
			continue
		}
		if b == nil || b.node == nil || admission.mutation.task.ChannelID == "" || admission.mutation.task.ChannelType != 1 {
			immediate = append(immediate, personDirectoryBatchOutcome{index: i, err: metadb.ErrInvalidArgument})
			continue
		}
		waiter, err := b.admit(ctx, admission.mutation)
		if err != nil {
			immediate = append(immediate, personDirectoryBatchOutcome{index: i, err: err})
			continue
		}
		waiters[i] = waiter
	}
	if len(immediate) > 0 && emit != nil {
		emit(immediate)
	}

	cases := make([]reflect.SelectCase, len(waiters)*2)
	for i := range cases {
		cases[i].Dir = reflect.SelectRecv
	}
	remaining := 0
	for i, waiter := range waiters {
		if waiter == nil {
			continue
		}
		remaining++
		cases[i*2] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(waiter.result)}
		if done := contexts[i].Done(); done != nil {
			cases[i*2+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)}
		}
	}
	for remaining > 0 {
		wave := make([]personDirectoryBatchOutcome, 0, remaining)
		chosen, value, ok := reflect.Select(cases)
		wave = append(wave, b.completeSelectedDirectoryAdmission(chosen, value, ok, waiters, contexts, cases))
		remaining--
		for remaining > 0 {
			withDefault := append(cases, reflect.SelectCase{Dir: reflect.SelectDefault})
			chosen, value, ok = reflect.Select(withDefault)
			if chosen == len(cases) {
				break
			}
			wave = append(wave, b.completeSelectedDirectoryAdmission(chosen, value, ok, waiters, contexts, cases))
			remaining--
		}
		sort.Slice(wave, func(i, j int) bool { return wave[i].index < wave[j].index })
		if emit != nil {
			emit(wave)
		}
	}
}

func (b *personDirectoryBatcher) completeSelectedDirectoryAdmission(
	chosen int,
	value reflect.Value,
	ok bool,
	waiters []*personDirectoryWaiter,
	contexts []context.Context,
	cases []reflect.SelectCase,
) personDirectoryBatchOutcome {
	index := chosen / 2
	waiter := waiters[index]
	cases[index*2].Chan = reflect.Value{}
	cases[index*2+1].Chan = reflect.Value{}
	if chosen%2 == 1 {
		b.detach(waiter)
		return personDirectoryBatchOutcome{index: index, err: contexts[index].Err()}
	}
	if !ok {
		return personDirectoryBatchOutcome{index: index, err: metadb.ErrInvalidArgument}
	}
	err, _ := value.Interface().(error)
	return personDirectoryBatchOutcome{index: index, err: err}
}

func (b *personDirectoryBatcher) admit(ctx context.Context, mutation personDirectoryMutation) (*personDirectoryWaiter, error) {
	for {
		waiter := &personDirectoryWaiter{result: make(chan error, 1)}
		b.mu.Lock()
		if b.stopped {
			b.mu.Unlock()
			return nil, errPersonDirectoryBatcherStopped
		}
		key := mutation.key()
		if entry := b.inflight[key]; entry != nil && !entry.canceled {
			waiter.entry = entry
			entry.waiters[waiter] = struct{}{}
			b.mu.Unlock()
			return waiter, nil
		}
		batch := b.current
		if b.queuedItems < b.maxQueued {
			if batch == nil || batch.sealed || len(batch.order) >= personDirectoryBatchMaxItems {
				batch = &personDirectoryBatch{entries: make(map[metadb.ChannelKey]*personDirectoryEntry), trigger: make(chan struct{})}
				b.current = batch
				b.owners++
				goruntimeregistry.SafeGo(b.goroutines, goruntimeregistry.TaskMessageDirectoryBatch, func() {
					defer b.ownerDone()
					b.run(batch)
				})
			}
			entry := &personDirectoryEntry{mutation: mutation, waiters: map[*personDirectoryWaiter]struct{}{waiter: {}}}
			waiter.entry = entry
			batch.entries[key] = entry
			batch.order = append(batch.order, entry)
			b.inflight[key] = entry
			b.queuedItems++
			if len(batch.order) >= b.targetItems {
				// Seal while admission still owns b.mu. Merely waking run lets a
				// vector caller append the rest of its wave before the owner is
				// scheduled, silently turning the latency target into maxItems.
				batch.sealed = true
				if b.current == batch {
					b.current = nil
				}
				batch.once.Do(func() { close(batch.trigger) })
			}
			b.mu.Unlock()
			return waiter, nil
		}
		capacity := b.capacity
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.ctx.Done():
			return nil, errPersonDirectoryBatcherStopped
		case <-capacity:
		}
	}
}

func (b *personDirectoryBatcher) detach(waiter *personDirectoryWaiter) {
	if waiter == nil || waiter.entry == nil {
		return
	}
	b.mu.Lock()
	entry := waiter.entry
	delete(entry.waiters, waiter)
	if len(entry.waiters) == 0 && !entry.canceled {
		batch := b.current
		key := entry.mutation.key()
		if batch != nil && !batch.sealed && batch.entries[key] == entry {
			entry.canceled = true
			delete(batch.entries, key)
			if b.inflight[key] == entry {
				delete(b.inflight, key)
			}
			b.queuedItems--
			b.signalCapacityLocked()
		}
	}
	b.mu.Unlock()
}

func (b *personDirectoryBatcher) run(batch *personDirectoryBatch) {
	timer := time.NewTimer(b.collectWait)
	select {
	case <-batch.trigger:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	case <-timer.C:
	case <-b.ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	b.mu.Lock()
	batch.sealed = true
	if b.current == batch {
		b.current = nil
	}
	entries := make([]*personDirectoryEntry, 0, len(batch.order))
	for _, entry := range batch.order {
		if !entry.canceled {
			entries = append(entries, entry)
		}
	}
	b.mu.Unlock()
	if len(entries) == 0 {
		return
	}

	completed := make([]bool, len(entries))
	complete := func(index int, err error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if index < 0 || index >= len(entries) || completed[index] {
			return
		}
		completed[index] = true
		entry := entries[index]
		released := false
		if !entry.canceled {
			b.queuedItems--
			released = true
		}
		key := entry.mutation.key()
		if b.inflight[key] == entry {
			delete(b.inflight, key)
		}
		for waiter := range entry.waiters {
			waiter.result <- err
		}
		entry.waiters = nil
		if released {
			b.signalCapacityLocked()
		}
	}
	select {
	case b.active <- struct{}{}:
		ctx, cancel := context.WithTimeout(b.ctx, b.timeout)
		b.submit(ctx, entries, complete)
		cancel()
		<-b.active
	case <-b.ctx.Done():
		for i := range entries {
			complete(i, b.ctx.Err())
		}
	}
	for i := range entries {
		if !completed[i] {
			complete(i, metadb.ErrInvalidArgument)
		}
	}
}

func (b *personDirectoryBatcher) ownerDone() {
	b.mu.Lock()
	b.owners--
	if b.stopped && b.owners == 0 {
		b.stopOnce.Do(func() { close(b.stoppedDone) })
	}
	b.mu.Unlock()
}

func (b *personDirectoryBatcher) signalCapacityLocked() {
	close(b.capacity)
	b.capacity = make(chan struct{})
}

func (b *personDirectoryBatcher) submit(ctx context.Context, entries []*personDirectoryEntry, complete func(int, error)) {
	tasks := make([]metadb.PersonDirectoryTask, 0, len(entries))
	for _, entry := range entries {
		tasks = append(tasks, entry.mutation.task)
	}
	retry := make([]int, 0, len(entries))
	retryErrs := make([]error, len(entries))
	seen := make([]bool, len(entries))
	b.admitPersonDirectoryAttempt(ctx, tasks, func(index int, err error) {
		if index < 0 || index >= len(entries) || seen[index] {
			return
		}
		seen[index] = true
		if err == nil || ctx.Err() != nil || !retryablePersonDirectoryLeaderChange(err) {
			complete(index, err)
			return
		}
		retry = append(retry, index)
		retryErrs[index] = err
	})
	for i := range seen {
		if !seen[i] {
			complete(i, metadb.ErrInvalidArgument)
		}
	}
	for _, index := range retry {
		complete(index, b.retryPersonDirectoryAdmission(ctx, entries[index].mutation.task, retryErrs[index]))
	}
}

// retryPersonDirectoryAdmission is deliberately scoped to the idempotent
// create-only directory boundary. It never retries arbitrary Slot proposals:
// an authoritative read must first prove that the exact Channel key is still
// unadmitted, and every resubmission reuses the original task value unchanged.
func (b *personDirectoryBatcher) retryPersonDirectoryAdmission(ctx context.Context, task metadb.PersonDirectoryTask, lastErr error) error {
	reader, ok := b.node.(personDirectoryAdmissionStateReader)
	if !ok {
		return lastErr
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		channel, readErr := reader.GetChannelMetadataAuthoritative(ctx, task.ChannelID, task.ChannelType)
		if readErr == nil && (channel.ChannelID != task.ChannelID || channel.ChannelType != task.ChannelType) {
			return errors.Join(lastErr, metadb.ErrCorruptValue)
		}
		if readErr == nil && task.Generation != 0 && channel.DirectoryProjectionState != metadb.DirectoryProjectionNone && channel.DirectoryProjectionGeneration != task.Generation {
			return errors.Join(lastErr, metadb.ErrStaleMeta)
		}
		switch {
		case readErr == nil && channel.DirectoryProjectionState != metadb.DirectoryProjectionNone:
			return nil
		case readErr == nil, errors.Is(readErr, metadb.ErrNotFound):
			var (
				admissionErr error
				emitted      bool
			)
			b.admitPersonDirectoryAttempt(ctx, []metadb.PersonDirectoryTask{task}, func(index int, err error) {
				if index == 0 && !emitted {
					emitted = true
					admissionErr = err
				}
			})
			if !emitted {
				return metadb.ErrInvalidArgument
			}
			if admissionErr == nil {
				return nil
			}
			if !retryablePersonDirectoryLeaderChange(admissionErr) {
				return admissionErr
			}
			lastErr = admissionErr
		case retryablePersonDirectoryLeaderChange(readErr):
			lastErr = errors.Join(lastErr, readErr)
		default:
			return errors.Join(lastErr, readErr)
		}
		if err := waitPersonDirectoryRetry(ctx); err != nil {
			return err
		}
	}
}

func (b *personDirectoryBatcher) admitPersonDirectoryAttempt(ctx context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	attemptTimeout := b.attemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = personDirectoryAdmissionAttemptTimeout
	}
	attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	b.node.AdmitPersonDirectoryTaskWaves(attemptCtx, tasks, emit)
	cancel()
}

func retryablePersonDirectoryLeaderChange(err error) bool {
	return errors.Is(err, clusterpkg.ErrRouteNotReady) ||
		errors.Is(err, clusterpkg.ErrNoSlotLeader) ||
		errors.Is(err, clusterpkg.ErrNotLeader) ||
		errors.Is(err, clusterpkg.ErrNotStarted) ||
		errors.Is(err, clusterpkg.ErrStopping) ||
		errors.Is(err, metadb.ErrStaleMeta) ||
		channelruntime.ErrorMatches(err, channelruntime.ErrStaleMeta) ||
		channelruntime.ErrorMatches(err, channelruntime.ErrNotLeader) ||
		errors.Is(err, propose.ErrNotLeader) ||
		errors.Is(err, routing.ErrRouteNotReady) ||
		errors.Is(err, routing.ErrNoSlotLeader) ||
		errors.Is(err, clusternet.ErrNodeNotFound) ||
		errors.Is(err, clusternet.ErrServiceNotFound) ||
		errors.Is(err, transport.ErrStopped) ||
		errors.Is(err, transport.ErrTimeout) ||
		errors.Is(err, transport.ErrNodeNotFound) ||
		errors.Is(err, transport.ErrQueueFull) ||
		errors.Is(err, transport.ErrDialFailed) ||
		errors.Is(err, transport.ErrBusy) ||
		errors.Is(err, context.DeadlineExceeded)
}

func waitPersonDirectoryRetry(ctx context.Context) error {
	timer := time.NewTimer(personDirectoryRetryBackoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
