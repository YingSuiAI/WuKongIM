package multiraft

import (
	"context"
	"encoding/binary"
	"sync/atomic"

	raft "go.etcd.io/raft/v3"
)

// ReadBarrierResult identifies a quorum-confirmed read after durable FSM apply.
// It is not a public message precondition and does not append a Raft log entry.
type ReadBarrierResult struct {
	Index    uint64
	Term     uint64
	LeaderID NodeID
}

type readBarrierRequest struct {
	ctx  context.Context
	resp chan readBarrierResponse
	done atomic.Bool
	// term and index are accessed only while the owning Slot mutex is held.
	term  uint64
	index uint64
}

type readBarrierResponse struct {
	result ReadBarrierResult
	err    error
}

func (r *readBarrierRequest) finish(response readBarrierResponse) bool {
	if r == nil || !r.done.CompareAndSwap(false, true) {
		return false
	}
	r.resp <- response
	return true
}

// ReadBarrier uses Raft's safe ReadIndex quorum protocol, then waits for the
// returned index to finish durable application. Role/commit snapshots alone
// cannot prove a current read during elections or a network partition.
func (r *Runtime) ReadBarrier(ctx context.Context, slotID SlotID) (ReadBarrierResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ReadBarrierResult{}, err
	}
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ReadBarrierResult{}, ErrRuntimeClosed
	}
	g, ok := r.slots[slotID]
	r.mu.RUnlock()
	if !ok {
		return ReadBarrierResult{}, ErrSlotNotFound
	}
	request := &readBarrierRequest{ctx: ctx, resp: make(chan readBarrierResponse, 1)}
	if err := g.enqueueControl(controlAction{kind: controlReadBarrier, readBarrier: request}); err != nil {
		return ReadBarrierResult{}, err
	}
	r.scheduler.enqueue(slotID)
	select {
	case response := <-request.resp:
		return response.result, response.err
	case <-ctx.Done():
		if request.finish(readBarrierResponse{err: ctx.Err()}) {
			return ReadBarrierResult{}, ctx.Err()
		}
		response := <-request.resp
		return response.result, response.err
	}
}

// startReadBarrier runs on the RawNode owner, never an application goroutine.
func (g *slot) startReadBarrier(request *readBarrierRequest) {
	if request == nil || request.done.Load() {
		return
	}
	if err := request.ctx.Err(); err != nil {
		request.finish(readBarrierResponse{err: err})
		return
	}
	// One critical section owns lifecycle validation, registration and issuing
	// the nonblocking RawNode request. Close cannot pass between those steps.
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.admissionErrLocked(); err != nil {
		request.finish(readBarrierResponse{err: err})
		return
	}
	status := g.rawNode.BasicStatus()
	if status.RaftState != raft.StateLeader || status.Lead != status.ID {
		request.finish(readBarrierResponse{err: ErrNotLeader})
		return
	}
	g.resolveReadBarriersLocked()
	limit := g.maxQueuedControls
	if limit <= 0 {
		limit = 1024
	}
	if len(g.pendingReads) >= limit || g.readSequence == ^uint64(0) {
		request.finish(readBarrierResponse{err: ErrSlotBusy})
		return
	}
	g.readSequence++
	var key [24]byte
	binary.BigEndian.PutUint64(key[0:8], status.ID)
	binary.BigEndian.PutUint64(key[8:16], status.Term)
	binary.BigEndian.PutUint64(key[16:24], g.readSequence)
	request.term = status.Term
	if g.pendingReads == nil {
		g.pendingReads = make(map[string]*readBarrierRequest)
	}
	g.pendingReads[string(key[:])] = request
	// ReadOnlySafe is the Raft default. It waits for current-term commitment and
	// a fresh quorum heartbeat acknowledgement; no lease/timing assumption.
	g.rawNode.ReadIndex(key[:])
}

func (g *slot) observeReadStates(states []raft.ReadState) {
	if len(states) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, state := range states {
		if request := g.pendingReads[string(state.RequestCtx)]; request != nil {
			request.index = state.Index
		}
	}
	// Completion is driven by the subsequent fresh Raft status or durable apply.
}

func (g *slot) resolveReadBarriersLocked() {
	for key, request := range g.pendingReads {
		if g.status.Role != RoleLeader || g.status.Term != request.term || g.status.LeaderID != g.status.NodeID {
			request.finish(readBarrierResponse{err: ErrNotLeader})
			delete(g.pendingReads, key)
			continue
		}
		if request.done.Load() || request.ctx.Err() != nil {
			if err := request.ctx.Err(); err != nil {
				request.finish(readBarrierResponse{err: err})
			}
			// Raft cannot cancel an issued ReadIndex context. Retain its bounded
			// slot until ReadStates arrives or leadership changes; otherwise
			// canceled callers could grow RawNode's pending-read map unbounded.
			if request.index != 0 {
				delete(g.pendingReads, key)
			}
			continue
		}
		if request.index != 0 && g.durableAppliedIndex >= request.index {
			request.finish(readBarrierResponse{result: ReadBarrierResult{Index: request.index, Term: request.term, LeaderID: g.status.NodeID}})
			delete(g.pendingReads, key)
		}
	}
}

func (g *slot) failReadBarriersLocked(err error) {
	for key, request := range g.pendingReads {
		request.finish(readBarrierResponse{err: err})
		delete(g.pendingReads, key)
	}
}
