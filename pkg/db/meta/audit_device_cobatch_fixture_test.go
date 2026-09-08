//go:build integration

package meta

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/commit"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/engine"
)

// AuditDeviceCommitObserver exposes physical-group evidence only to the external
// FSM integration tests; no instrumentation is added to the product API.
type AuditDeviceCommitObserver struct {
	mu     sync.Mutex
	events []commit.BatchEvent
	db     *DB
}

func (*AuditDeviceCommitObserver) SetQueueDepth(int) {}
func (o *AuditDeviceCommitObserver) ObserveBatch(event commit.BatchEvent) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
}

func (o *AuditDeviceCommitObserver) Reset() {
	o.mu.Lock()
	o.events = nil
	o.mu.Unlock()
}

func (o *AuditDeviceCommitObserver) Groups() []commit.BatchEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]commit.BatchEvent(nil), o.events...)
}

// BlockNextCommit holds a real physical commit before fsync, with the logical
// request's hash-slot lock still owned by the coordinator terminal finalizer.
func (o *AuditDeviceCommitObserver) BlockNextCommit() (<-chan struct{}, chan<- struct{}) {
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	o.db.meta.committer.SetCommitFunc(func(batch *engine.Batch) error {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return batch.Commit(true)
	})
	return entered, release
}

// SetCommitError keeps physical infrastructure failure distinct from an expected
// command-local no-op. A nil error restores the real durable engine commit.
func (o *AuditDeviceCommitObserver) SetCommitError(err error) {
	o.db.meta.committer.SetCommitFunc(func(batch *engine.Batch) error {
		if err != nil {
			return err
		}
		return batch.Commit(true)
	})
}

// AuditDeviceCoBatchDB creates real Pebble-backed metadata and its real commit
// coordinator. MaxRequests and observed groups make co-batch coverage explicit.
func AuditDeviceCoBatchDB(t *testing.T, maxRequests int) (*DB, *AuditDeviceCommitObserver) {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	db.meta.committer.Close()
	observer := &AuditDeviceCommitObserver{db: db}
	db.meta.committer = commit.NewCoordinator(db.engine, commit.Config{
		FlushWindow: 100 * time.Millisecond, QueueSize: 8, MaxRequests: maxRequests, Observer: observer,
	})
	return db, observer
}
