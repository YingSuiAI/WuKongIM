package multiraft

import (
	"context"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
)

// OfflineSnapshotBoundary verifies that a stopped Slot has no unapplied or
// uncommitted tail. It never discards a tail or infers application from commit.
func OfflineSnapshotBoundary(ctx context.Context, storage Storage, machine StateMachine) (BootstrapState, error) {
	state, err := storage.InitialState(ctx)
	if err != nil {
		return BootstrapState{}, err
	}
	if durable, ok := machine.(DurableAppliedStateMachine); ok {
		index, err := durable.DurableAppliedIndex(ctx)
		if err != nil {
			return BootstrapState{}, err
		}
		if index > state.AppliedIndex {
			state.AppliedIndex = index
		}
	}
	last, err := storage.LastIndex(ctx)
	if err != nil {
		return BootstrapState{}, err
	}
	if state.AppliedIndex == 0 || state.AppliedIndex != state.HardState.Commit || state.AppliedIndex != last {
		return BootstrapState{}, fmt.Errorf("offline snapshot requires drained Slot: applied=%d commit=%d last=%d; recover and drain with the source binary first", state.AppliedIndex, state.HardState.Commit, last)
	}
	return state, nil
}

// ReplaceOfflineStateSnapshot publishes the stopped FSM at its exact applied
// boundary, replacing even a same-index old-format snapshot. The caller must
// hold exclusive ownership of both stores and fence startup until every Slot
// has been upgraded. No historical command is decoded or replayed here.
func ReplaceOfflineStateSnapshot(ctx context.Context, storage Storage, machine StateMachine) error {
	replacer, ok := storage.(ExternalSnapshotStorage)
	if !ok {
		return fmt.Errorf("offline snapshot requires atomic snapshot replacement")
	}
	state, err := OfflineSnapshotBoundary(ctx, storage, machine)
	if err != nil {
		return err
	}
	previous, err := storage.Snapshot(ctx)
	if err != nil {
		return err
	}
	_, previousConfig, err := decodeSlotSnapshotData(previous.Data)
	if err != nil {
		return err
	}
	configIndex := latestValidConfigAppliedIndex(state.AppliedIndex, state.ConfigAppliedIndex, previousConfig)
	first, err := storage.FirstIndex(ctx)
	if err != nil {
		return err
	}
	// Read bounded batches only to find membership fences, never decode business
	// payloads or restore the superseded recovery snapshot into live metadata.
	for first <= state.AppliedIndex {
		entries, err := storage.Entries(ctx, first, state.AppliedIndex+1, 1<<20)
		if err != nil {
			return err
		}
		if len(entries) == 0 || entries[0].Index != first {
			return fmt.Errorf("offline snapshot log gap at %d", first)
		}
		for _, entry := range entries {
			if entry.Index != first {
				return fmt.Errorf("offline snapshot log gap at %d", first)
			}
			if isConfigChangeEntry(entry) && entry.Index > configIndex {
				configIndex = entry.Index
			}
			first++
		}
	}
	term, err := storage.Term(ctx, state.AppliedIndex)
	if err != nil {
		return err
	}
	if term == 0 {
		return fmt.Errorf("offline snapshot applied term is missing")
	}
	snapshot, err := machine.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := storage.MarkApplied(ctx, state.AppliedIndex); err != nil {
		return err
	}
	return replacer.ReplaceSnapshot(ctx, raftpb.Snapshot{
		Data:     encodeSlotSnapshotData(snapshot.Data, configIndex),
		Metadata: raftpb.SnapshotMetadata{Index: state.AppliedIndex, Term: term, ConfState: state.ConfState},
	})
}
