//go:build integration

package controller

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSlotRequestsFailClosedWhileRuntimeIsStarting(t *testing.T) {
	runtime := newSlotRequestLifecycleRuntime(t, true, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- runtime.Start(ctx) }()

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for runtimeLifecycle(runtime.lifecycle.Load()) != runtimeLifecycleRaftStarted {
		select {
		case err := <-startDone:
			t.Fatalf("Start() completed before the runtime admission boundary was exercised: %v", err)
		case <-deadline.C:
			t.Fatal("runtime did not reach the raft-started lifecycle phase")
		case <-ticker.C:
		}
	}

	assertSlotRequestsNotStarted(t, runtime)
	cancel()
	if err := <-startDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want context.Canceled", err)
	}
	if got := runtimeLifecycle(runtime.lifecycle.Load()); got != runtimeLifecycleStopped {
		t.Fatalf("lifecycle after canceled Start() = %d, want stopped", got)
	}
}

func TestSlotRequestsFailClosedAfterRuntimeStops(t *testing.T) {
	runtime := startSingleVoterRuntime(t, "cluster-slot-request-stopped")
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := runtimeLifecycle(runtime.lifecycle.Load()); got != runtimeLifecycleStopped {
		t.Fatalf("lifecycle after Stop() = %d, want stopped", got)
	}
	assertSlotRequestsNotStarted(t, runtime)
}

func TestSlotRequestsFailClosedAfterRuntimeStartFails(t *testing.T) {
	runtime := newSlotRequestLifecycleRuntime(t, false, 5*time.Millisecond)
	err := runtime.Start(context.Background())
	if err == nil {
		t.Fatal("Start() succeeded with bootstrap disabled and no materialized state")
	}
	if runtime.raft == nil {
		t.Fatal("failed Start() did not retain the stopped Raft service needed to exercise the lifecycle fence")
	}
	if got := runtimeLifecycle(runtime.lifecycle.Load()); got != runtimeLifecycleStopped {
		t.Fatalf("lifecycle after failed Start() = %d, want stopped", got)
	}
	assertSlotRequestsNotStarted(t, runtime)
}

func newSlotRequestLifecycleRuntime(t *testing.T, allowBootstrap bool, tick time.Duration) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(RuntimeConfig{
		NodeID:           1,
		Addr:             "n1",
		StateDir:         t.TempDir(),
		ClusterID:        "cluster-slot-request-lifecycle",
		Role:             RuntimeRoleVoter,
		Voters:           []Voter{{NodeID: 1, Addr: "n1"}},
		AllowBootstrap:   allowBootstrap,
		InitialSlotCount: 1,
		HashSlotCount:    4,
		ReplicaCount:     1,
		TickInterval:     tick,
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	return runtime
}

func assertSlotRequestsNotStarted(t *testing.T, runtime *Runtime) {
	t.Helper()
	operations := []struct {
		name string
		run  func() error
	}{
		{name: "leader transfer", run: func() error {
			_, err := runtime.RequestSlotLeaderTransfer(context.Background(), SlotLeaderTransferRequest{})
			return err
		}},
		{name: "replica move", run: func() error {
			_, err := runtime.RequestSlotReplicaMove(context.Background(), SlotReplicaMoveRequest{})
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); !errors.Is(err, ErrNotStarted) {
				t.Fatalf("error = %v, want ErrNotStarted", err)
			}
		})
	}
}
