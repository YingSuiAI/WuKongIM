package proxy

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestNewRegistersRPCHandlersOnPromotedCluster(t *testing.T) {
	cluster := &promotedRPCRegistrationCluster{}

	New(cluster, nil)

	got := make([]int, 0, len(cluster.handlers))
	for serviceID := range cluster.handlers {
		got = append(got, int(serviceID))
	}
	sort.Ints(got)
	want := []int{
		int(runtimeMetaRPCServiceID),
		int(identityRPCServiceID),
		int(subscriberRPCServiceID),
		int(channelRPCServiceID),
		int(permissionBatchRPCServiceID),
		int(channelMigrationRPCServiceID),
		int(pluginBindingRPCServiceID),
		int(membershipRPCServiceID),
	}
	sort.Ints(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registered RPC service IDs = %v, want %v", got, want)
	}
}

func TestNewChannelMetadataStoreRegistersAuthoritativeReadHandlers(t *testing.T) {
	cluster := &promotedRPCRegistrationCluster{}

	NewChannelMetadataStore(cluster, nil)

	got := make([]int, 0, len(cluster.handlers))
	for serviceID := range cluster.handlers {
		got = append(got, int(serviceID))
	}
	sort.Ints(got)
	want := []int{
		int(runtimeMetaRPCServiceID),
		int(identityRPCServiceID),
		int(subscriberRPCServiceID),
		int(channelRPCServiceID),
		int(permissionBatchRPCServiceID),
		int(membershipRPCServiceID),
	}
	sort.Ints(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registered channel metadata RPC service IDs = %v, want %v", got, want)
	}
}

func TestAuthoritativeReadsRequireConfirmedLocalLeaderForSinglePeerSlot(t *testing.T) {
	cluster := &promotedRPCRegistrationCluster{localNodeID: 1}
	store := &Store{cluster: cluster}

	if store.shouldServeSlotLocally(1) {
		t.Fatal("shouldServeSlotLocally() = true without a confirmed leader")
	}
	cluster.localSlotLeader = true
	if !store.shouldServeSlotLocally(1) {
		t.Fatal("shouldServeSlotLocally() = false for confirmed local leader")
	}
	cluster.localSlotLeader = false
	cluster.leaderID = 2
	if store.shouldServeSlotLocally(1) {
		t.Fatal("shouldServeSlotLocally() = true for a remote leader")
	}
}

func TestAuthoritativeRPCPrefersLocalRaftLeadershipOverStaleRemoteHint(t *testing.T) {
	cluster := &promotedRPCRegistrationCluster{localNodeID: 1, localSlotLeader: true, leaderID: 2}
	store := &Store{cluster: cluster}

	body, handled, err := store.handleAuthoritativeRPC(1, func(status string, leaderID uint64) ([]byte, error) {
		return []byte(status), nil
	})
	if err != nil || handled || body != nil {
		t.Fatalf("handleAuthoritativeRPC() = (%q, %v, %v), want local handling", body, handled, err)
	}
}

func TestAuthoritativeRPCDoesNotServeStaleLocalRouterLeader(t *testing.T) {
	cluster := &promotedRPCRegistrationCluster{localNodeID: 1, leaderID: 1}
	store := &Store{cluster: cluster}

	body, handled, err := store.handleAuthoritativeRPC(1, func(status string, leaderID uint64) ([]byte, error) {
		return []byte(status), nil
	})
	if err != nil || !handled || string(body) != rpcStatusNoLeader {
		t.Fatalf("handleAuthoritativeRPC() = (%q, %v, %v), want no_leader", body, handled, err)
	}
}

func TestAuthoritativeRPCRetainsRemoteLeaderHint(t *testing.T) {
	cluster := &promotedRPCRegistrationCluster{localNodeID: 1, leaderID: 2}
	store := &Store{cluster: cluster}

	body, handled, err := store.handleAuthoritativeRPC(1, func(status string, leaderID uint64) ([]byte, error) {
		return []byte(fmt.Sprintf("%s:%d", status, leaderID)), nil
	})
	if err != nil || !handled || string(body) != "not_leader:2" {
		t.Fatalf("handleAuthoritativeRPC() = (%q, %v, %v), want remote leader hint", body, handled, err)
	}
}

type promotedRPCRegistrationCluster struct {
	handlers        map[uint8]func(context.Context, []byte) ([]byte, error)
	leaderID        multiraft.NodeID
	localNodeID     multiraft.NodeID
	localSlotLeader bool
}

func (c *promotedRPCRegistrationCluster) RegisterSlotProxyRPC(serviceID uint8, handler func(context.Context, []byte) ([]byte, error)) {
	if c.handlers == nil {
		c.handlers = make(map[uint8]func(context.Context, []byte) ([]byte, error))
	}
	c.handlers[serviceID] = handler
}

func (c *promotedRPCRegistrationCluster) SlotIDs() []multiraft.SlotID { return nil }

func (c *promotedRPCRegistrationCluster) SlotForKey(string) multiraft.SlotID { return 0 }

func (c *promotedRPCRegistrationCluster) HashSlotForKey(string) uint16 { return 0 }

func (c *promotedRPCRegistrationCluster) HashSlotsOf(multiraft.SlotID) []uint16 { return nil }

func (c *promotedRPCRegistrationCluster) HashSlotTableVersion() uint64 { return 0 }

func (c *promotedRPCRegistrationCluster) IsLocalSlotLeader(multiraft.SlotID) bool {
	return c.localSlotLeader
}

func (c *promotedRPCRegistrationCluster) LeaderOf(multiraft.SlotID) (multiraft.NodeID, error) {
	if c.leaderID == 0 {
		return 0, errNoLeader
	}
	return c.leaderID, nil
}

func (c *promotedRPCRegistrationCluster) IsLocal(nodeID multiraft.NodeID) bool {
	return c.localNodeID != 0 && nodeID == c.localNodeID
}

func (c *promotedRPCRegistrationCluster) PeersForSlot(multiraft.SlotID) []multiraft.NodeID {
	return nil
}

func (c *promotedRPCRegistrationCluster) RPCService(context.Context, multiraft.NodeID, multiraft.SlotID, uint8, []byte) ([]byte, error) {
	return nil, errNoLeader
}
