package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	goruntimeregistry "github.com/WuKongIM/WuKongIM/pkg/goroutine"
)

func TestPersonDirectoryBatcherUsesBoundedCollectionWindowAndBatch(t *testing.T) {
	batcher := newPersonDirectoryBatcher(&recordingPersonDirectoryBatchNode{}, nil)
	if batcher.collectWait != 50*time.Millisecond {
		t.Fatalf("collect wait = %v, want 50ms", batcher.collectWait)
	}
	if batcher.targetItems != 32 || personDirectoryBatchMaxItems != 128 {
		t.Fatalf("target/max batch items = %d/%d, want 32/128", batcher.targetItems, personDirectoryBatchMaxItems)
	}
	if cap(batcher.active) != 8 {
		t.Fatalf("active batch capacity = %d, want 8", cap(batcher.active))
	}
	if batcher.attemptTimeout != 5*time.Second || batcher.timeout != 10*time.Second {
		t.Fatalf("attempt/total timeout = %v/%v, want 5s/10s", batcher.attemptTimeout, batcher.timeout)
	}
}

func TestPersonDirectoryBatcherStopCancelsAndJoinsOwnedBatch(t *testing.T) {
	node := &blockingPersonDirectoryBatchNode{started: make(chan struct{}, 1)}
	registry := goruntimeregistry.New()
	batcher := newPersonDirectoryBatcher(node, registry)
	batcher.collectWait = time.Hour
	batcher.targetItems = 1
	result := make(chan error, 1)
	go func() {
		result <- batcher.ensure(context.Background(), testPersonDirectoryMutation(0))
	}()

	select {
	case <-node.started:
	case <-time.After(time.Second):
		t.Fatal("person-directory batch did not start")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := batcher.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("ensure() error = %v, want canceled owned admission", err)
	}
	if snapshot := registry.Snapshot(); snapshot.ManagedTotal != 0 {
		t.Fatalf("managed goroutines after Stop = %d, want 0", snapshot.ManagedTotal)
	}
	if err := batcher.ensure(context.Background(), testPersonDirectoryMutation(1)); !errors.Is(err, errPersonDirectoryBatcherStopped) {
		t.Fatalf("ensure() after Stop error = %v, want stopped", err)
	}
}

func TestPersonDirectoryBatcherCoalescesConcurrentChannelsIntoOneDurableAdmission(t *testing.T) {
	node := &recordingPersonDirectoryBatchNode{}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 4

	errCh := make(chan error, 4)
	for i := 0; i < 4; i++ {
		index := i
		go func() {
			errCh <- batcher.ensure(context.Background(), testPersonDirectoryMutation(index))
		}()
	}
	for range 4 {
		if err := <-errCh; err != nil {
			t.Fatalf("ensure() error = %v", err)
		}
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.admissionCalls != 1 || len(node.tasks) != 4 {
		t.Fatalf("admission calls/tasks = %d/%d, want 1/4", node.admissionCalls, len(node.tasks))
	}
}

func TestPersonDirectoryBatcherSealsVectorAdmissionAtTargetSize(t *testing.T) {
	const (
		targetItems = 12
		totalItems  = 24
	)
	node := &recordingPersonDirectoryBatchNode{}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = targetItems
	admissions := make([]personDirectoryBatchAdmission, totalItems)
	for i := range admissions {
		admissions[i] = personDirectoryBatchAdmission{
			ctx:      context.Background(),
			mutation: testPersonDirectoryMutation(i),
		}
	}

	results := batcher.ensureBatch(admissions)
	for i, err := range results {
		if err != nil {
			t.Fatalf("result %d = %v", i, err)
		}
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if got, want := node.batchSizes, []int{targetItems, targetItems}; !equalInts(got, want) {
		t.Fatalf("durable batch sizes = %v, want %v", got, want)
	}
}

func TestPersonDirectoryBatcherEmitsCompletedWaveBeforeSlowSiblingBatch(t *testing.T) {
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSlow) }) })
	node := &stagedPersonDirectoryBatchNode{releaseSlow: releaseSlow}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 2
	admissions := make([]personDirectoryBatchAdmission, 4)
	for i := range admissions {
		admissions[i] = personDirectoryBatchAdmission{ctx: context.Background(), mutation: testPersonDirectoryMutation(i)}
	}
	waves := make(chan []personDirectoryBatchOutcome, 2)
	done := make(chan struct{})
	go func() {
		batcher.ensureBatchWaves(admissions, func(wave []personDirectoryBatchOutcome) {
			waves <- append([]personDirectoryBatchOutcome(nil), wave...)
		})
		close(done)
	}()

	select {
	case wave := <-waves:
		if got := directoryOutcomeIndexes(wave); !equalInts(got, []int{0, 1}) {
			t.Fatalf("first completed wave indexes = %v, want [0 1]", got)
		}
	case <-time.After(time.Second):
		t.Fatal("fast durable batch was held behind slow sibling")
	}
	releaseOnce.Do(func() { close(releaseSlow) })
	select {
	case wave := <-waves:
		if got := directoryOutcomeIndexes(wave); !equalInts(got, []int{2, 3}) {
			t.Fatalf("second completed wave indexes = %v, want [2 3]", got)
		}
	case <-time.After(time.Second):
		t.Fatal("slow durable batch did not complete after release")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wave admission did not join all owned work")
	}
}

func TestPersonDirectoryBatcherPublishesFastSourceSlotWithinOneDurableBatch(t *testing.T) {
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSlow) }) })
	node := &stagedIntraBatchPersonDirectoryBatchNode{releaseSlow: releaseSlow}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 2
	admissions := []personDirectoryBatchAdmission{
		{ctx: context.Background(), mutation: testPersonDirectoryMutation(0)},
		{ctx: context.Background(), mutation: testPersonDirectoryMutation(1)},
	}
	waves := make(chan []personDirectoryBatchOutcome, 2)
	done := make(chan struct{})
	go func() {
		batcher.ensureBatchWaves(admissions, func(wave []personDirectoryBatchOutcome) {
			waves <- append([]personDirectoryBatchOutcome(nil), wave...)
		})
		close(done)
	}()

	select {
	case wave := <-waves:
		if got := directoryOutcomeIndexes(wave); !equalInts(got, []int{0}) {
			releaseOnce.Do(func() { close(releaseSlow) })
			t.Fatalf("first completed wave indexes = %v, want fast source index 0", got)
		}
	case <-time.After(100 * time.Millisecond):
		releaseOnce.Do(func() { close(releaseSlow) })
		t.Fatal("fast source result was held until every Slot in the durable batch completed")
	}
	releaseOnce.Do(func() { close(releaseSlow) })
	select {
	case wave := <-waves:
		if got := directoryOutcomeIndexes(wave); !equalInts(got, []int{1}) {
			t.Fatalf("second completed wave indexes = %v, want slow source index 1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("slow source result did not complete after release")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batched source admission did not join")
	}
}

func TestPersonDirectoryBatcherSingleflightsSameChannelWhileSealedBatchIsActive(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	node := &blockingPersonDirectoryBatchNode{
		started: make(chan struct{}, 2),
		release: release,
	}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 1
	mutation := testPersonDirectoryMutation(0)
	results := make(chan error, 2)
	go func() { results <- batcher.ensure(context.Background(), mutation) }()
	select {
	case <-node.started:
	case <-time.After(time.Second):
		t.Fatal("first person-directory batch did not start")
	}

	go func() { results <- batcher.ensure(context.Background(), mutation) }()
	select {
	case <-node.started:
		releaseAll()
		for range 2 {
			<-results
		}
		t.Fatal("same channel started a second durable batch while the first batch was active")
	case <-time.After(50 * time.Millisecond):
	}

	releaseAll()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("ensure() error = %v", err)
		}
	}
}

func TestPersonDirectoryBatcherReturnsAdmissionFailure(t *testing.T) {
	admissionErr := errors.New("admission failed")
	node := &recordingPersonDirectoryBatchNode{admissionErr: admissionErr}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Millisecond
	batcher.targetItems = 8

	err := batcher.ensure(context.Background(), testPersonDirectoryMutation(0))
	if !errors.Is(err, admissionErr) {
		t.Fatalf("ensure() error = %v, want admission failure", err)
	}
}

func TestPersonDirectoryBatcherRereadsAppliedTaskAfterAmbiguousLeaderChange(t *testing.T) {
	node := &ambiguousAppliedPersonDirectoryBatchNode{}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Millisecond
	batcher.targetItems = 1
	mutation := testPersonDirectoryMutation(0)

	err := batcher.ensure(context.Background(), mutation)

	if err != nil {
		t.Fatalf("ensure() error = %v", err)
	}
	if node.admissionCalls != 1 || node.readCalls != 1 {
		t.Fatalf("admission/read calls = %d/%d, want one ambiguous submit then one authoritative recovery read", node.admissionCalls, node.readCalls)
	}
	if node.task != mutation.task {
		t.Fatalf("admitted task = %#v, want unchanged identity %#v", node.task, mutation.task)
	}
}

func TestPersonDirectoryBatcherRetainsRecoveryBudgetAfterAttemptTimeout(t *testing.T) {
	node := &attemptTimeoutAppliedPersonDirectoryBatchNode{}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Millisecond
	batcher.targetItems = 1
	batcher.attemptTimeout = 20 * time.Millisecond
	batcher.timeout = 200 * time.Millisecond

	err := batcher.ensure(context.Background(), testPersonDirectoryMutation(0))

	if err != nil {
		t.Fatalf("ensure() error = %v", err)
	}
	if node.admissionCalls != 1 || node.readCalls != 1 {
		t.Fatalf("admission/read calls = %d/%d, want one expired attempt then one recovery read", node.admissionCalls, node.readCalls)
	}
}

func TestPersonDirectoryBatcherDoesNotRetryNonLeaderFailure(t *testing.T) {
	admissionErr := errors.New("admission payload rejected")
	node := &nonRetryablePersonDirectoryBatchNode{admissionErr: admissionErr}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Millisecond
	batcher.targetItems = 1

	err := batcher.ensure(context.Background(), testPersonDirectoryMutation(0))

	if !errors.Is(err, admissionErr) {
		t.Fatalf("ensure() error = %v, want non-retryable admission error", err)
	}
	if node.admissionCalls != 1 || node.readCalls != 0 {
		t.Fatalf("admission/read calls = %d/%d, want no recovery for non-leader failure", node.admissionCalls, node.readCalls)
	}
}

func TestPersonDirectoryBatcherRejectsRecoveredDifferentGeneration(t *testing.T) {
	node := &generationMismatchPersonDirectoryBatchNode{}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Millisecond
	batcher.targetItems = 1
	mutation := testPersonDirectoryMutation(0)
	mutation.task.Generation = 7

	err := batcher.ensure(context.Background(), mutation)

	if !errors.Is(err, metadb.ErrStaleMeta) {
		t.Fatalf("ensure() error = %v, want typed generation conflict", err)
	}
	if node.admissionCalls != 1 || node.readCalls != 1 {
		t.Fatalf("admission/read calls = %d/%d, want conflict without resubmission", node.admissionCalls, node.readCalls)
	}
}

func TestPersonDirectoryBatcherRetriesMissingTaskUntilLeaderElection(t *testing.T) {
	node := &missingUntilElectionPersonDirectoryBatchNode{failuresBeforeElection: 3}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Millisecond
	batcher.targetItems = 1
	mutation := testPersonDirectoryMutation(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := batcher.ensure(ctx, mutation)

	if err != nil {
		t.Fatalf("ensure() error = %v", err)
	}
	if got := len(node.tasks); got != node.failuresBeforeElection+1 {
		t.Fatalf("admission calls = %d, want %d attempts spanning election", got, node.failuresBeforeElection+1)
	}
	if node.readCalls != node.failuresBeforeElection {
		t.Fatalf("authoritative reads = %d, want one before every resubmit", node.readCalls)
	}
	for i, task := range node.tasks {
		if task != mutation.task {
			t.Fatalf("admission task %d = %#v, want unchanged identity %#v", i, task, mutation.task)
		}
	}
}

func TestPersonDirectoryBatcherPreservesAlignedPartialAdmissionResults(t *testing.T) {
	admissionErr := errors.New("second source slot unavailable")
	node := &partialPersonDirectoryBatchNode{admissionErr: admissionErr}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 2

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- batcher.ensure(context.Background(), testPersonDirectoryMutation(0)) }()
	go func() { second <- batcher.ensure(context.Background(), testPersonDirectoryMutation(1)) }()

	if err := <-first; err != nil {
		t.Fatalf("first aligned admission error = %v, want success", err)
	}
	if err := <-second; !errors.Is(err, admissionErr) {
		t.Fatalf("second aligned admission error = %v, want %v", err, admissionErr)
	}
}

func TestPersonDirectoryBatcherWaitsForCapacityInsteadOfRejectingColdWave(t *testing.T) {
	const queuedItems = 32
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	node := &blockingPersonDirectoryBatchNode{release: release}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 1
	batcher.maxQueued = queuedItems

	results := make(chan error, queuedItems+1)
	for index := 0; index < queuedItems; index++ {
		index := index
		go func() {
			results <- batcher.ensure(context.Background(), testPersonDirectoryMutation(index))
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		batcher.mu.Lock()
		queued := batcher.queuedItems
		batcher.mu.Unlock()
		if queued == queuedItems {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued person directories = %d, want %d", queued, queuedItems)
		}
		time.Sleep(time.Millisecond)
	}

	extra := make(chan error, 1)
	go func() {
		extra <- batcher.ensure(context.Background(), testPersonDirectoryMutation(queuedItems))
	}()
	select {
	case err := <-extra:
		releaseAll()
		for range queuedItems {
			<-results
		}
		t.Fatalf("extra ensure returned %v while the bounded queue was transiently full; want it to wait", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseAll()
	for range queuedItems {
		if err := <-results; err != nil {
			t.Fatalf("queued ensure error = %v", err)
		}
	}
	if err := <-extra; err != nil {
		t.Fatalf("extra ensure after capacity release error = %v", err)
	}
}

func TestPersonDirectoryBatcherRunsEightColdDirectoryBatchesConcurrently(t *testing.T) {
	const concurrentBatches = 8
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	node := &blockingPersonDirectoryBatchNode{
		started: make(chan struct{}, concurrentBatches),
		release: release,
	}
	batcher := newPersonDirectoryBatcher(node, nil)
	batcher.collectWait = time.Hour
	batcher.targetItems = 1

	results := make(chan error, concurrentBatches)
	for index := 0; index < concurrentBatches; index++ {
		index := index
		go func() {
			results <- batcher.ensure(context.Background(), testPersonDirectoryMutation(index))
		}()
		select {
		case <-node.started:
		case <-time.After(time.Second):
			releaseAll()
			for range index + 1 {
				<-results
			}
			t.Fatalf("active person-directory batches = %d, want %d", index, concurrentBatches)
		}
	}
	releaseAll()
	for range concurrentBatches {
		if err := <-results; err != nil {
			t.Fatalf("ensure() error = %v", err)
		}
	}
}

func testPersonDirectoryMutation(index int) personDirectoryMutation {
	channelID := string(rune('a'+index)) + "@z"
	return personDirectoryMutation{
		task: metadb.PersonDirectoryTask{ChannelID: channelID, ChannelType: 1, CommittedTail: uint64(index), CreatedAt: 1},
	}
}

type recordingPersonDirectoryBatchNode struct {
	mu sync.Mutex

	admissionCalls int
	tasks          []metadb.PersonDirectoryTask
	batchSizes     []int
	admissionErr   error
}

type blockingPersonDirectoryBatchNode struct {
	started chan struct{}
	release <-chan struct{}
}

type partialPersonDirectoryBatchNode struct {
	admissionErr error
}

type ambiguousAppliedPersonDirectoryBatchNode struct {
	admissionCalls int
	readCalls      int
	task           metadb.PersonDirectoryTask
}

type attemptTimeoutAppliedPersonDirectoryBatchNode struct {
	admissionCalls int
	readCalls      int
}

type nonRetryablePersonDirectoryBatchNode struct {
	admissionErr   error
	admissionCalls int
	readCalls      int
}

type generationMismatchPersonDirectoryBatchNode struct {
	admissionCalls int
	readCalls      int
}

type missingUntilElectionPersonDirectoryBatchNode struct {
	failuresBeforeElection int
	readCalls              int
	tasks                  []metadb.PersonDirectoryTask
}

func (n *missingUntilElectionPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(_ context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	n.tasks = append(n.tasks, tasks[0])
	if len(n.tasks) <= n.failuresBeforeElection {
		emit(0, context.DeadlineExceeded)
		return
	}
	emit(0, nil)
}

func (n *missingUntilElectionPersonDirectoryBatchNode) GetChannelMetadataAuthoritative(_ context.Context, _ string, _ int64) (metadb.Channel, error) {
	n.readCalls++
	return metadb.Channel{}, metadb.ErrNotFound
}

func (n *ambiguousAppliedPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(_ context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	n.admissionCalls++
	n.task = tasks[0]
	emit(0, context.DeadlineExceeded)
}

func (n *ambiguousAppliedPersonDirectoryBatchNode) GetChannelMetadataAuthoritative(_ context.Context, channelID string, channelType int64) (metadb.Channel, error) {
	n.readCalls++
	return metadb.Channel{ChannelID: channelID, ChannelType: channelType, DirectoryProjectionState: metadb.DirectoryProjectionPending}, nil
}

func (n *attemptTimeoutAppliedPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(ctx context.Context, _ []metadb.PersonDirectoryTask, emit func(int, error)) {
	n.admissionCalls++
	<-ctx.Done()
	emit(0, ctx.Err())
}

func (n *attemptTimeoutAppliedPersonDirectoryBatchNode) GetChannelMetadataAuthoritative(ctx context.Context, channelID string, channelType int64) (metadb.Channel, error) {
	n.readCalls++
	if err := ctx.Err(); err != nil {
		return metadb.Channel{}, err
	}
	return metadb.Channel{ChannelID: channelID, ChannelType: channelType, DirectoryProjectionState: metadb.DirectoryProjectionPending}, nil
}

func (n *nonRetryablePersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(_ context.Context, _ []metadb.PersonDirectoryTask, emit func(int, error)) {
	n.admissionCalls++
	emit(0, n.admissionErr)
}

func (n *nonRetryablePersonDirectoryBatchNode) GetChannelMetadataAuthoritative(_ context.Context, _ string, _ int64) (metadb.Channel, error) {
	n.readCalls++
	return metadb.Channel{}, metadb.ErrNotFound
}

func (n *generationMismatchPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(_ context.Context, _ []metadb.PersonDirectoryTask, emit func(int, error)) {
	n.admissionCalls++
	emit(0, context.DeadlineExceeded)
}

func (n *generationMismatchPersonDirectoryBatchNode) GetChannelMetadataAuthoritative(_ context.Context, channelID string, channelType int64) (metadb.Channel, error) {
	n.readCalls++
	return metadb.Channel{
		ChannelID:                     channelID,
		ChannelType:                   channelType,
		DirectoryProjectionState:      metadb.DirectoryProjectionPending,
		DirectoryProjectionGeneration: 8,
	}, nil
}

type stagedPersonDirectoryBatchNode struct {
	releaseSlow <-chan struct{}
}

type stagedIntraBatchPersonDirectoryBatchNode struct {
	releaseSlow <-chan struct{}
}

func (n *stagedIntraBatchPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(ctx context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	if len(tasks) > 0 {
		emit(0, nil)
	}
	if len(tasks) < 2 {
		return
	}
	select {
	case <-ctx.Done():
		emit(1, ctx.Err())
	case <-n.releaseSlow:
		emit(1, nil)
	}
}

func (n *stagedIntraBatchPersonDirectoryBatchNode) AdmitPersonDirectoryTasks(ctx context.Context, tasks []metadb.PersonDirectoryTask) []error {
	results := make([]error, len(tasks))
	n.AdmitPersonDirectoryTaskWaves(ctx, tasks, func(index int, err error) { results[index] = err })
	return results
}

func (n *stagedPersonDirectoryBatchNode) AdmitPersonDirectoryTasks(ctx context.Context, tasks []metadb.PersonDirectoryTask) []error {
	results := make([]error, len(tasks))
	if len(tasks) == 0 || tasks[0].ChannelID != "c@z" {
		return results
	}
	select {
	case <-ctx.Done():
		for i := range results {
			results[i] = ctx.Err()
		}
	case <-n.releaseSlow:
	}
	return results
}

func (n *stagedPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(ctx context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	emitPersonDirectoryAdmissionResults(n.AdmitPersonDirectoryTasks(ctx, tasks), emit)
}

func (n *partialPersonDirectoryBatchNode) AdmitPersonDirectoryTasks(_ context.Context, tasks []metadb.PersonDirectoryTask) []error {
	results := make([]error, len(tasks))
	for i, task := range tasks {
		if task.ChannelID == "b@z" {
			results[i] = n.admissionErr
		}
	}
	return results
}

func (n *partialPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(ctx context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	emitPersonDirectoryAdmissionResults(n.AdmitPersonDirectoryTasks(ctx, tasks), emit)
}

func (n *blockingPersonDirectoryBatchNode) AdmitPersonDirectoryTasks(ctx context.Context, tasks []metadb.PersonDirectoryTask) []error {
	if n.started != nil {
		n.started <- struct{}{}
	}
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-n.release:
	}
	results := make([]error, len(tasks))
	for i := range results {
		results[i] = err
	}
	return results
}

func (n *blockingPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(ctx context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	emitPersonDirectoryAdmissionResults(n.AdmitPersonDirectoryTasks(ctx, tasks), emit)
}

func (n *recordingPersonDirectoryBatchNode) AdmitPersonDirectoryTasks(_ context.Context, tasks []metadb.PersonDirectoryTask) []error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.admissionCalls++
	n.tasks = append(n.tasks, tasks...)
	n.batchSizes = append(n.batchSizes, len(tasks))
	results := make([]error, len(tasks))
	for i := range results {
		results[i] = n.admissionErr
	}
	return results
}

func (n *recordingPersonDirectoryBatchNode) AdmitPersonDirectoryTaskWaves(ctx context.Context, tasks []metadb.PersonDirectoryTask, emit func(int, error)) {
	emitPersonDirectoryAdmissionResults(n.AdmitPersonDirectoryTasks(ctx, tasks), emit)
}

func emitPersonDirectoryAdmissionResults(results []error, emit func(int, error)) {
	for i, err := range results {
		emit(i, err)
	}
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[int]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}

func directoryOutcomeIndexes(outcomes []personDirectoryBatchOutcome) []int {
	indexes := make([]int, len(outcomes))
	for i, outcome := range outcomes {
		indexes[i] = outcome.index
	}
	return indexes
}
