package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/controller/fsm"
	controllerraft "github.com/WuKongIM/WuKongIM/pkg/controller/raft"
	"github.com/WuKongIM/WuKongIM/pkg/controller/server"
	"github.com/WuKongIM/WuKongIM/pkg/controller/statefile"
	cv2sync "github.com/WuKongIM/WuKongIM/pkg/controller/sync"
	"go.etcd.io/raft/v3/raftpb"
)

// Runtime hosts Controller Raft or mirror sync behind the public facade.
type Runtime struct {
	cfg RuntimeConfig

	// lifecycleMu serializes Start and Stop while admitting Slot mutations only
	// across the fully started interval represented by lifecycle.
	lifecycleMu sync.RWMutex
	lifecycle   atomic.Uint32

	mu    sync.RWMutex
	state ClusterState
	watch chan StateEvent

	store  *statefile.Store
	sm     *fsm.StateMachine
	raft   *controllerraft.Service
	server *server.Server

	syncServer *cv2sync.Server
	syncClient *cv2sync.Client

	refreshCancel context.CancelFunc
	refreshWG     sync.WaitGroup
}

// NewRuntime creates a Controller runtime facade.
func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if cfg.Role == "" {
		cfg.Role = RuntimeRoleVoter
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 20 * time.Millisecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NodeID == 0 || cfg.StateDir == "" || cfg.ClusterID == "" || len(cfg.Voters) == 0 {
		return nil, fmt.Errorf("controller: invalid runtime config")
	}
	return &Runtime{cfg: cfg, watch: make(chan StateEvent, 16)}, nil
}

// Start starts the local Controller runtime.
func (r *Runtime) Start(ctx context.Context) error {
	if r == nil {
		return ErrNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	switch runtimeLifecycle(r.lifecycle.Load()) {
	case runtimeLifecycleStarted:
		return nil
	case runtimeLifecycleStarting, runtimeLifecycleRaftStarted, runtimeLifecycleStopping:
		return ErrNotStarted
	}
	r.lifecycle.Store(uint32(runtimeLifecycleStarting))
	succeeded := false
	defer func() {
		if !succeeded {
			r.lifecycle.CompareAndSwap(uint32(runtimeLifecycleStarting), uint32(runtimeLifecycleStopped))
			r.lifecycle.CompareAndSwap(uint32(runtimeLifecycleRaftStarted), uint32(runtimeLifecycleStopped))
		}
	}()
	if err := os.MkdirAll(r.cfg.StateDir, 0o755); err != nil {
		return err
	}
	r.store = statefile.New(filepath.Join(r.cfg.StateDir, "cluster-state.json"))
	var err error
	switch r.cfg.Role {
	case RuntimeRoleVoter:
		err = r.startVoter(ctx)
	case RuntimeRoleMirror:
		err = r.startMirror(ctx)
	default:
		err = fmt.Errorf("controller: invalid runtime role %q", r.cfg.Role)
	}
	if err != nil {
		return err
	}
	if !r.lifecycle.CompareAndSwap(uint32(runtimeLifecycleStarting), uint32(runtimeLifecycleStarted)) &&
		!r.lifecycle.CompareAndSwap(uint32(runtimeLifecycleRaftStarted), uint32(runtimeLifecycleStarted)) {
		return ErrStopped
	}
	succeeded = true
	return nil
}

// Stop stops local Controller resources.
func (r *Runtime) Stop(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	for {
		lifecycle := runtimeLifecycle(r.lifecycle.Load())
		switch lifecycle {
		case runtimeLifecycleStopped:
			return nil
		case runtimeLifecycleStopping:
			r.lifecycleMu.Lock()
			r.lifecycleMu.Unlock()
			return nil
		default:
			if r.lifecycle.CompareAndSwap(uint32(lifecycle), uint32(runtimeLifecycleStopping)) {
				goto stop
			}
		}
	}

stop:
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	defer r.lifecycle.Store(uint32(runtimeLifecycleStopped))
	r.stopRefreshLoop()
	if r.raft != nil {
		return r.raft.Stop()
	}
	return nil
}

// LocalState returns a deep copy of the latest locally visible cluster state.
func (r *Runtime) LocalState(ctx context.Context) (ClusterState, error) {
	if err := ctxErr(ctx); err != nil {
		return ClusterState{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state.Clone(), nil
}

// LeaderID returns the best-known Controller leader ID.
func (r *Runtime) LeaderID() uint64 {
	if r.raft != nil {
		return r.raft.LeaderID()
	}
	if r.syncClient != nil {
		if leaderID := r.syncClient.LeaderID(); leaderID != 0 {
			return leaderID
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.state.Controllers) > 0 {
		return r.state.Controllers[0].NodeID
	}
	return 0
}

// ProbePropose verifies the hosted Controller proposal path when this runtime is a voter.
func (r *Runtime) ProbePropose(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if r == nil || r.raft == nil {
		return ErrNotStarted
	}
	return r.raft.ProbePropose(ctx)
}

// ControllerRaftStatus returns the local Controller Raft status snapshot.
func (r *Runtime) ControllerRaftStatus(ctx context.Context) (RaftStatus, error) {
	if err := ctxErr(ctx); err != nil {
		return RaftStatus{}, err
	}
	if r == nil || r.raft == nil {
		return RaftStatus{}, ErrNotStarted
	}
	return r.raft.Status(), nil
}

// CompactControllerRaftLog forces local Controller Raft log compaction.
func (r *Runtime) CompactControllerRaftLog(ctx context.Context) (LogCompactionResult, error) {
	if err := ctxErr(ctx); err != nil {
		return LogCompactionResult{}, err
	}
	if r == nil || r.raft == nil {
		return LogCompactionResult{}, ErrNotStarted
	}
	return r.raft.CompactLog(ctx)
}

// Step applies an inbound Controller Raft message to the local Raft service.
func (r *Runtime) Step(ctx context.Context, msg raftpb.Message) error {
	if r == nil || r.raft == nil {
		return nil
	}
	return r.raft.Step(ctx, msg)
}

// GetState serves Controller state sync requests from local voter state.
func (r *Runtime) GetState(ctx context.Context, req GetStateRequest) (GetStateResponse, error) {
	if r == nil || r.syncServer == nil {
		return GetStateResponse{NotReady: true}, nil
	}
	return r.syncServer.GetState(ctx, req)
}

// Watch returns state update events.
func (r *Runtime) Watch() <-chan StateEvent { return r.watch }

func (r *Runtime) stopRefreshLoop() {
	if r.refreshCancel == nil {
		return
	}
	r.refreshCancel()
	r.refreshWG.Wait()
	r.refreshCancel = nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

type runtimeLifecycle uint32

const (
	runtimeLifecycleStopped runtimeLifecycle = iota
	runtimeLifecycleStarting
	runtimeLifecycleRaftStarted
	runtimeLifecycleStarted
	runtimeLifecycleStopping
)

func (r *Runtime) lockStartedSlotRequest() (func(), error) {
	if r == nil || runtimeLifecycle(r.lifecycle.Load()) != runtimeLifecycleStarted {
		return nil, ErrNotStarted
	}
	r.lifecycleMu.RLock()
	if runtimeLifecycle(r.lifecycle.Load()) != runtimeLifecycleStarted || r.raft == nil {
		r.lifecycleMu.RUnlock()
		return nil, ErrNotStarted
	}
	return r.lifecycleMu.RUnlock, nil
}
