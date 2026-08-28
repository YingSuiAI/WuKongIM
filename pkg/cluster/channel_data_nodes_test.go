package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
)

func TestDataNodeViewWaitsForExactControlRevision(t *testing.T) {
	var view dataNodeView
	view.UpdateAtRevision(1, []uint64{1, 2, 3}, []uint64{1, 2, 3})

	type result struct {
		nodes channels.PlacementDataNodeSet
		err   error
	}
	done := make(chan result, 1)
	go func() {
		nodes, err := view.PlacementDataNodes(context.Background(), 2)
		done <- result{nodes: nodes, err: err}
	}()

	select {
	case got := <-done:
		t.Fatalf("PlacementDataNodes() returned before revision 2 was published: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	view.UpdateAtRevision(2, []uint64{2, 3, 4, 5}, []uint64{2, 3, 4})

	select {
	case got := <-done:
		if got.err != nil || !equalUint64s(got.nodes.Active, []uint64{2, 3, 4, 5}) || !equalUint64s(got.nodes.Schedulable, []uint64{2, 3, 4}) {
			t.Fatalf("PlacementDataNodes() = nodes %#v error %v, want revision-2 active and schedulable nodes", got.nodes, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("PlacementDataNodes() did not wake after revision 2 was published")
	}
}

func TestDataNodeViewRejectsRouteFromOlderControlRevision(t *testing.T) {
	var view dataNodeView
	view.UpdateAtRevision(3, []uint64{1, 2, 3}, []uint64{1, 2, 3})

	_, err := view.PlacementDataNodes(context.Background(), 2)
	if !errors.Is(err, ch.ErrStaleMeta) {
		t.Fatalf("PlacementDataNodes() error = %v, want ErrStaleMeta", err)
	}
}

func TestProbeChannelPlacementReadyAcceptsActiveReplicaSetWithMinISRSchedulable(t *testing.T) {
	cfg := validNodeConfig(t)
	cfg.Channel.ReplicaCount = 3
	router := routing.NewRouter()
	if err := router.UpdateControlSnapshot(control.Snapshot{
		Revision: 1,
		Nodes: []control.Node{
			{NodeID: 1, Roles: []control.Role{control.RoleData}, JoinState: control.NodeJoinStateActive},
			{NodeID: 2, Roles: []control.Role{control.RoleData}, JoinState: control.NodeJoinStateActive},
			{NodeID: 3, Roles: []control.Role{control.RoleData}, JoinState: control.NodeJoinStateActive},
		},
		Slots:     []control.SlotAssignment{{SlotID: 1, DesiredPeers: []uint64{1, 2, 3}, ConfigEpoch: 1, PreferredLeader: 1}},
		HashSlots: control.HashSlotTable{Count: 1, Ranges: []control.HashSlotRange{{From: 0, To: 0, SlotID: 1}}},
	}); err != nil {
		t.Fatalf("UpdateControlSnapshot() error = %v", err)
	}
	node := &Node{cfg: cfg, router: router}
	node.channelDataNodes.UpdateAtRevision(1, []uint64{1, 2, 3}, []uint64{1, 2})

	if err := node.probeChannelPlacementReady(context.Background()); err != nil {
		t.Fatalf("probeChannelPlacementReady() error = %v, want degraded quorum ready", err)
	}

	node.channelDataNodes.UpdateAtRevision(1, []uint64{1, 2, 3}, []uint64{1})
	if err := node.probeChannelPlacementReady(context.Background()); err == nil {
		t.Fatal("probeChannelPlacementReady() error = nil, want schedulable nodes below MinISR rejected")
	}

	node.channelDataNodes.UpdateAtRevision(1, []uint64{1, 2, 3}, []uint64{1, 4})
	if err := node.probeChannelPlacementReady(context.Background()); err == nil {
		t.Fatal("probeChannelPlacementReady() error = nil, want non-active schedulable node excluded")
	}
}
