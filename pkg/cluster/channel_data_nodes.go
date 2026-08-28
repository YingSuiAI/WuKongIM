package cluster

import (
	"context"
	"fmt"
	"sync"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
)

// dataNodeView stores active membership and schedulable data-node IDs from one
// exact control snapshot.
type dataNodeView struct {
	mu          sync.RWMutex
	revision    uint64
	active      []uint64
	schedulable []uint64
	changed     chan struct{}
}

// UpdateAtRevision atomically replaces active and schedulable placement nodes
// and the exact control revision from which they were derived.
func (v *dataNodeView) UpdateAtRevision(revision uint64, active, schedulable []uint64) {
	v.mu.Lock()
	v.revision = revision
	v.active = append([]uint64(nil), active...)
	v.schedulable = append([]uint64(nil), schedulable...)
	previous := v.changed
	v.changed = make(chan struct{})
	if previous != nil {
		close(previous)
	}
	v.mu.Unlock()
}

// SchedulableDataNodes returns a defensive copy of the latest healthy data-node set.
func (v *dataNodeView) SchedulableDataNodes() []uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return append([]uint64(nil), v.schedulable...)
}

// PlacementDataNodes waits for and returns the exact control revision used by
// an already-routed create batch. A newer candidate generation proves the
// supplied route stale and must be rerouted by the caller.
func (v *dataNodeView) PlacementDataNodes(ctx context.Context, expectedRevision uint64) (channels.PlacementDataNodeSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		v.mu.Lock()
		switch {
		case v.revision == expectedRevision:
			nodes := channels.PlacementDataNodeSet{
				Active:      append([]uint64(nil), v.active...),
				Schedulable: append([]uint64(nil), v.schedulable...),
			}
			v.mu.Unlock()
			return nodes, nil
		case v.revision > expectedRevision:
			actual := v.revision
			v.mu.Unlock()
			return channels.PlacementDataNodeSet{}, fmt.Errorf("%w: channel placement route revision=%d candidates=%d", ch.ErrStaleMeta, expectedRevision, actual)
		}
		if v.changed == nil {
			v.changed = make(chan struct{})
		}
		changed := v.changed
		v.mu.Unlock()
		select {
		case <-ctx.Done():
			return channels.PlacementDataNodeSet{}, ctx.Err()
		case <-changed:
		}
	}
}
