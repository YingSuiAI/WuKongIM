package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/internal/lifecycle"
)

func TestNodeStopInvalidatesPublishedReadiness(t *testing.T) {
	node, _ := newStopReadinessTestNode(t, &recordingReconciler{})
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if before := node.Snapshot(); !before.RoutesReady || !before.SlotsReady || !before.ChannelsReady {
		_ = node.Stop(context.Background())
		t.Fatalf("Snapshot() before Stop = %+v, want published readiness", before)
	}

	if err := node.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if after := node.Snapshot(); after.RoutesReady || after.SlotsReady || after.ChannelsReady {
		t.Fatalf("Snapshot() after Stop = %+v, want all runtime readiness invalidated", after)
	}
}

func TestNodeStopFencesInFlightControlSnapshotReadiness(t *testing.T) {
	reconciler := &blockingSecondStopReadinessReconciler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	node, controller := newStopReadinessTestNode(t, reconciler)
	t.Cleanup(func() {
		reconciler.Release()
		_ = node.Stop(context.Background())
	})
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	next := nodeControlSnapshot()
	next.Revision = 2
	next.HashSlots.Revision = 2
	next.Slots[0].ConfigEpoch = 2
	if err := controller.Publish(next); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-reconciler.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("watched snapshot did not enter Slot reconciliation")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- node.Stop(context.Background()) }()
	waitUntil(t, func() bool {
		snapshot := node.Snapshot()
		return !snapshot.RoutesReady && !snapshot.SlotsReady && !snapshot.ChannelsReady
	})

	reconciler.Release()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not wait for the watched snapshot to finish")
	}
	after := node.Snapshot()
	if after.StateRevision != 2 {
		t.Fatalf("Snapshot() after Stop revision = %d, want completed watched revision 2", after.StateRevision)
	}
	if after.RoutesReady || after.SlotsReady || after.ChannelsReady {
		t.Fatalf("Snapshot() after Stop = %+v, want in-flight apply fenced from republishing readiness", after)
	}
}

func TestNodeStopFencesInFlightSlotReadinessPublication(t *testing.T) {
	node, _ := newStopReadinessTestNode(t, &recordingReconciler{})
	blocker := newBlockingStopReadinessResource()
	node.resources = []lifecycle.NamedResource{{Name: "blocking-shutdown", Resource: blocker}}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		blocker.Release()
		_ = node.Stop(context.Background())
	})

	stopDone := make(chan error, 1)
	go func() { stopDone <- node.Stop(context.Background()) }()
	blocker.WaitForStop(t)

	revision := node.Snapshot().StateRevision
	node.updateDefaultSlotsReady(revision, true)
	if snapshot := node.Snapshot(); snapshot.SlotsReady {
		t.Fatalf("Snapshot() during Stop = %+v, want Slot readiness publication fenced", snapshot)
	}

	blocker.Release()
	waitForStopReadinessTest(t, stopDone)
	if snapshot := node.Snapshot(); snapshot.RoutesReady || snapshot.SlotsReady || snapshot.ChannelsReady {
		t.Fatalf("Snapshot() after Stop = %+v, want all runtime readiness invalidated", snapshot)
	}
}

func TestNodeStopFencesRestoreChannelReadinessPublication(t *testing.T) {
	node, _ := newStopReadinessTestNode(t, &recordingReconciler{})
	blocker := newBlockingStopReadinessResource()
	node.resources = []lifecycle.NamedResource{{Name: "blocking-shutdown", Resource: blocker}}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		blocker.Release()
		_ = node.Stop(context.Background())
	})

	stopDone := make(chan error, 1)
	go func() { stopDone <- node.Stop(context.Background()) }()
	blocker.WaitForStop(t)

	node.markChannelsReady(true)
	if snapshot := node.Snapshot(); snapshot.ChannelsReady {
		t.Fatalf("Snapshot() during Stop = %+v, want restore Channel readiness publication fenced", snapshot)
	}

	blocker.Release()
	waitForStopReadinessTest(t, stopDone)
	if snapshot := node.Snapshot(); snapshot.RoutesReady || snapshot.SlotsReady || snapshot.ChannelsReady {
		t.Fatalf("Snapshot() after Stop = %+v, want all runtime readiness invalidated", snapshot)
	}
}

func TestNodeStopFencesRestoreRuntimeResume(t *testing.T) {
	node, _ := newStopReadinessTestNode(t, &recordingReconciler{})
	blocker := newBlockingStopReadinessResource()
	node.resources = []lifecycle.NamedResource{{Name: "blocking-shutdown", Resource: blocker}}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		blocker.Release()
		_ = node.Stop(context.Background())
	})
	node.PauseLocalRestoreRuntime()
	if node.channelTickCancel != nil {
		t.Fatal("channelTickCancel remained set after restore pause")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- node.Stop(context.Background()) }()
	blocker.WaitForStop(t)

	node.ResumeLocalRestoreRuntime()
	if node.channelTickCancel != nil || node.channelRetentionCancel != nil || node.channelMigrationCancel != nil {
		t.Fatal("ResumeLocalRestoreRuntime() restarted Channel background work during Stop")
	}
	node.messageEventStreamCache.mu.Lock()
	restorePaused := node.messageEventStreamCache.restorePaused
	node.messageEventStreamCache.mu.Unlock()
	if !restorePaused {
		t.Fatal("ResumeLocalRestoreRuntime() reopened restore cache during Stop")
	}

	blocker.Release()
	waitForStopReadinessTest(t, stopDone)
	if node.channelTickCancel != nil || node.channelRetentionCancel != nil || node.channelMigrationCancel != nil {
		t.Fatal("Channel background work remained after Stop")
	}
}

func newStopReadinessTestNode(t *testing.T, reconciler slotReconciler) (*Node, *control.StaticController) {
	t.Helper()
	controller := control.NewStaticController(nodeControlSnapshot())
	node, err := New(
		validNodeConfig(t),
		withController(controller),
		withSlotReconciler(reconciler),
		WithProposer(&recordingProposer{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	node.channels = noopChannelService{}
	return node, controller
}

type blockingSecondStopReadinessReconciler struct {
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	calls       int
}

func (r *blockingSecondStopReadinessReconciler) Reconcile(context.Context, control.Snapshot) error {
	r.calls++
	if r.calls != 2 {
		return nil
	}
	close(r.entered)
	<-r.release
	return nil
}

func (r *blockingSecondStopReadinessReconciler) Release() {
	r.releaseOnce.Do(func() { close(r.release) })
}

type blockingStopReadinessResource struct {
	stopEntered chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newBlockingStopReadinessResource() *blockingStopReadinessResource {
	return &blockingStopReadinessResource{
		stopEntered: make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (r *blockingStopReadinessResource) Start(context.Context) error { return nil }

func (r *blockingStopReadinessResource) Stop(context.Context) error {
	r.enterOnce.Do(func() { close(r.stopEntered) })
	<-r.release
	return nil
}

func (r *blockingStopReadinessResource) WaitForStop(t *testing.T) {
	t.Helper()
	select {
	case <-r.stopEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not reach the blocking lifecycle resource")
	}
}

func (r *blockingStopReadinessResource) Release() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func waitForStopReadinessTest(t *testing.T, stopDone <-chan error) {
	t.Helper()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not finish after releasing the blocking lifecycle resource")
	}
}
