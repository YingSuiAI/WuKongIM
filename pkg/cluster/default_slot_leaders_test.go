package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
)

func TestRemoteSlotLeaderWorkerCountIsBounded(t *testing.T) {
	tests := []struct {
		peers int
		want  int
	}{
		{peers: 0, want: 0},
		{peers: 1, want: 1},
		{peers: remoteSlotLeaderMaxConcurrency, want: remoteSlotLeaderMaxConcurrency},
		{peers: remoteSlotLeaderMaxConcurrency + 4, want: remoteSlotLeaderMaxConcurrency},
	}
	for _, test := range tests {
		if got := remoteSlotLeaderWorkerCount(test.peers); got != test.want {
			t.Fatalf("remoteSlotLeaderWorkerCount(%d)=%d, want %d", test.peers, got, test.want)
		}
	}
}

func TestRemoteSlotLeaderStatusesWaitsForHighestTermPeerObservation(t *testing.T) {
	caller := &orderedSlotStatusCaller{responses: map[uint64]orderedSlotStatusResponse{
		1: {delay: time.Millisecond, statuses: []routing.SlotStatus{{SlotID: 2, Leader: 1, LeaderTerm: 37}}},
		3: {delay: 20 * time.Millisecond, statuses: []routing.SlotStatus{{SlotID: 2, Leader: 3, LeaderTerm: 38}}},
		4: {delay: 2 * time.Millisecond, statuses: []routing.SlotStatus{{SlotID: 2, Leader: 1, LeaderTerm: 37}}},
	}}
	node := &Node{cfg: Config{NodeID: 2}, slotStatusCaller: caller}
	snapshot := control.Snapshot{
		Nodes: []control.Node{{NodeID: 1}, {NodeID: 2}, {NodeID: 3}, {NodeID: 4}},
		Slots: []control.SlotAssignment{{SlotID: 2, DesiredPeers: []uint64{1, 3, 4}}},
	}

	got := node.remoteSlotLeaderStatuses(context.Background(), snapshot, []uint32{2})
	if len(got) != 1 || got[0].SlotID != 2 || got[0].Leader != 3 || got[0].LeaderTerm != 38 {
		t.Fatalf("remoteSlotLeaderStatuses() = %#v, want highest observed leader 3 term 38", got)
	}
}

type orderedSlotStatusResponse struct {
	delay    time.Duration
	statuses []routing.SlotStatus
}

type orderedSlotStatusCaller struct {
	responses map[uint64]orderedSlotStatusResponse
}

func (c *orderedSlotStatusCaller) Call(ctx context.Context, nodeID uint64, serviceID uint8, _ []byte) ([]byte, error) {
	if serviceID != clusternet.RPCSlotStatus {
		return nil, fmt.Errorf("unexpected service id %d", serviceID)
	}
	response, ok := c.responses[nodeID]
	if !ok {
		return nil, fmt.Errorf("missing response for node %d", nodeID)
	}
	timer := time.NewTimer(response.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return encodeSlotStatusResponse(response.statuses)
	}
}
