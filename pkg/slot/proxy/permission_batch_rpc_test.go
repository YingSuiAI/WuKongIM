package proxy

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goruntimeregistry "github.com/WuKongIM/WuKongIM/pkg/goroutine"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestPermissionMetadataSlotWorkersAreBounded(t *testing.T) {
	const groups = permissionBatchSlotWorkers * 2
	entered := make(chan struct{}, groups)
	release := make(chan struct{})
	done := make(chan struct{})
	var active atomic.Int64
	var peak atomic.Int64
	go func() {
		runSlotMetadataBatchWorkers(goruntimeregistry.TaskSlotPermissionBatch, groups, func(int) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := peak.Load()
				if current <= observed || peak.CompareAndSwap(observed, current) {
					break
				}
			}
			entered <- struct{}{}
			<-release
		})
		close(done)
	}()

	for i := 0; i < permissionBatchSlotWorkers; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("workers entered = %d, want %d", i, permissionBatchSlotWorkers)
		}
	}
	select {
	case <-entered:
		t.Fatalf("workers exceeded bound %d", permissionBatchSlotWorkers)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not finish after release")
	}
	if got := peak.Load(); got != permissionBatchSlotWorkers {
		t.Fatalf("peak workers = %d, want %d", got, permissionBatchSlotWorkers)
	}
}

func TestPermissionMetadataReadContinuesPastUnavailableStaleLeader(t *testing.T) {
	cluster := &stalePermissionLeaderCluster{}
	store := &Store{cluster: cluster}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()

	results, err := store.readPermissionMetadataGroup(ctx, 1, []PermissionMetadataRead{{
		Kind: PermissionMetadataReadChannel, ChannelID: "person", ChannelType: 1,
	}})

	if err != nil {
		t.Fatalf("readPermissionMetadataGroup() error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("readPermissionMetadataGroup() results = %#v, want one aligned result", results)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("readPermissionMetadataGroup() took %s, stale leader consumed the foreground deadline", elapsed)
	}
	if got := cluster.callsSnapshot(); !reflect.DeepEqual(got, []multiraft.NodeID{1, 3}) {
		t.Fatalf("RPC calls = %#v, want stale leader 1 followed by healthy peer 3", got)
	}
}

type stalePermissionLeaderCluster struct {
	mu    sync.Mutex
	calls []multiraft.NodeID
}

func (*stalePermissionLeaderCluster) SlotIDs() []multiraft.SlotID             { return []multiraft.SlotID{1} }
func (*stalePermissionLeaderCluster) SlotForKey(string) multiraft.SlotID      { return 1 }
func (*stalePermissionLeaderCluster) HashSlotForKey(string) uint16            { return 0 }
func (*stalePermissionLeaderCluster) HashSlotsOf(multiraft.SlotID) []uint16   { return []uint16{0} }
func (*stalePermissionLeaderCluster) HashSlotTableVersion() uint64            { return 1 }
func (*stalePermissionLeaderCluster) IsLocalSlotLeader(multiraft.SlotID) bool { return false }
func (*stalePermissionLeaderCluster) LeaderOf(multiraft.SlotID) (multiraft.NodeID, error) {
	return 1, nil
}
func (*stalePermissionLeaderCluster) IsLocal(multiraft.NodeID) bool { return false }
func (*stalePermissionLeaderCluster) PeersForSlot(multiraft.SlotID) []multiraft.NodeID {
	return []multiraft.NodeID{1, 3}
}
func (c *stalePermissionLeaderCluster) RPCService(ctx context.Context, nodeID multiraft.NodeID, _ multiraft.SlotID, _ uint8, _ []byte) ([]byte, error) {
	c.mu.Lock()
	c.calls = append(c.calls, nodeID)
	c.mu.Unlock()
	if nodeID == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return encodePermissionBatchRPCResponse(permissionBatchRPCResponse{
		Status: rpcStatusOK, Results: []PermissionMetadataReadResult{{}},
	})
}

func (c *stalePermissionLeaderCluster) callsSnapshot() []multiraft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]multiraft.NodeID(nil), c.calls...)
}
