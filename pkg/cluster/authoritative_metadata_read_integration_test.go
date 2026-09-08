//go:build integration

package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	slotproxy "github.com/WuKongIM/WuKongIM/pkg/slot/proxy"
	"github.com/stretchr/testify/require"
)

type metadataReadFaultTransport struct {
	base    multiraft.Transport
	dropped *atomic.Bool
	target  multiraft.NodeID
}

func (t metadataReadFaultTransport) Send(ctx context.Context, batch []multiraft.Envelope) error {
	kept := make([]multiraft.Envelope, 0, len(batch))
	for _, env := range batch {
		if !t.dropped.Load() || env.Message.To != uint64(t.target) {
			kept = append(kept, env)
		}
	}
	return t.base.Send(ctx, kept)
}

func TestAuthoritativeMetadataReadRejectsIsolatedPriorSlotLeader(t *testing.T) {
	nodes := newDefaultThreeNodeCluster(t)
	var dropped atomic.Bool
	for _, node := range nodes {
		node.cfg.Slots.HashSlotCount = 256
		node.slotTransportDecorator = func(base multiraft.Transport) multiraft.Transport {
			return metadataReadFaultTransport{base: base, dropped: &dropped, target: 2}
		}
	}
	// The isolated node cannot learn a newer term or self-expire leadership by
	// ticking; only its Slot traffic is dropped, while real TCP RPC remains live.
	nodes[1].cfg.Slots.TickInterval = time.Hour
	startNodes(t, nodes...)
	t.Cleanup(func() { dropped.Store(false); stopNodes(t, nodes...) })
	waitClusterReady(t, nodes...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const channelID = "metadata-read-current-value"
	route := waitRouteKeyLeaderConverged(t, nodes, channelID)
	require.NoError(t, nodes[route.Leader-1].UpsertChannelMetadata(ctx, metadb.Channel{ChannelID: channelID, ChannelType: 2, Ban: 0}))
	require.NoError(t, nodes[route.Leader-1].UpsertUserChannelMemberships(ctx, channelID, 2, []string{"metadata-user"}, 0, 1, 1))
	require.NoError(t, nodes[route.Leader-1].AddChannelSubscribers(ctx, channelID, 2, []string{"metadata-user"}, 1))
	device := metadb.Device{UID: "metadata-user", DeviceFlag: 1, DeviceID: "device", AppInstanceID: "app", InstallationGeneration: 1, SessionGeneration: 1, AuthorizationFence: 1}
	require.NoError(t, nodes[route.Leader-1].UpsertDeviceMetadata(ctx, device))
	if route.Leader != 2 {
		require.NoError(t, nodes[route.Leader-1].defaultSlotRuntime.TransferLeadership(ctx, multiraft.SlotID(route.SlotID), 2))
	}
	require.Eventually(t, func() bool {
		status, err := nodes[1].defaultSlotRuntime.Status(multiraft.SlotID(route.SlotID))
		return err == nil && status.Role == multiraft.RoleLeader
	}, 5*time.Second, 10*time.Millisecond)
	old, err := nodes[1].defaultSlotProxy.GetChannelForPermission(ctx, channelID, 2)
	require.NoError(t, err)
	require.Zero(t, old.Ban)
	dropped.Store(true)
	active := []*Node{nodes[0], nodes[2]}
	var leader *Node
	require.Eventually(t, func() bool {
		for _, node := range active {
			status, err := node.defaultSlotRuntime.Status(multiraft.SlotID(route.SlotID))
			if err == nil && status.Role == multiraft.RoleLeader {
				leader = node
				return true
			}
		}
		return false
	}, 8*time.Second, 20*time.Millisecond)
	waitRouteKeyLeaderConverged(t, active, channelID)
	require.NoError(t, leader.PatchChannelBusinessFlags(ctx, channelID, 2, metadb.ChannelBusinessFlags{Ban: 1}))
	require.NoError(t, leader.RemoveChannelSubscribers(ctx, channelID, 2, []string{"metadata-user"}, 2))
	require.NoError(t, leader.HideUserChannelMembership(ctx, "metadata-user", channelID, 2, 9, 2))
	device.SessionGeneration = 2
	device.AuthorizationFence = 2
	require.NoError(t, leader.UpsertDeviceMetadata(ctx, device))
	current, err := leader.defaultSlotProxy.GetChannelForPermission(ctx, channelID, 2)
	require.NoError(t, err)
	require.Equal(t, int64(1), current.Ban)
	stillOld, err := nodes[1].defaultSlotRuntime.FreshStatus(ctx, multiraft.SlotID(route.SlotID))
	require.NoError(t, err)
	require.Equal(t, multiraft.RoleLeader, stillOld.Role)
	t.Run("channel", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed, err := nodes[1].defaultSlotProxy.GetChannelForPermission(readCtx, channelID, 2)
		if err == nil {
			require.Equal(t, int64(1), observed.Ban, "an acknowledged new value must not be replaced by the isolated prior leader's cached value")
		}
	})
	t.Run("permission_batch", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed := nodes[1].defaultSlotProxy.ReadPermissionMetadataBatch(readCtx, []slotproxy.PermissionMetadataRead{{Kind: slotproxy.PermissionMetadataReadChannel, ChannelID: channelID, ChannelType: 2}})
		require.Len(t, observed, 1)
		if observed[0].Err == nil {
			require.Equal(t, int64(1), observed[0].Channel.Ban)
		}
	})
	t.Run("ordinary_membership", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed, found, err := nodes[1].GetUserChannelMembership(readCtx, "metadata-user", channelID, 2)
		if err == nil {
			require.True(t, found)
			require.Equal(t, uint64(9), observed.DeletedToSeq)
		}
	})
	t.Run("ordinary_membership_page", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed, _, _, err := nodes[1].ListUserChannelMembershipPage(readCtx, "metadata-user", metadb.UserChannelMembershipCursor{}, 10)
		if err == nil {
			for _, row := range observed {
				require.GreaterOrEqual(t, row.DeletedToSeq, uint64(9))
			}
		}
	})
	t.Run("gateway_device_identity", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed, err := nodes[1].GetDeviceMetadataAuthoritative(readCtx, device.UID, device.DeviceFlag, device.DeviceID, device.AppInstanceID)
		if err == nil {
			require.Equal(t, uint64(2), observed.SessionGeneration, "gateway identity lookup must not return the prior session generation")
		}
	})
	t.Run("subscriber_point", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed, err := nodes[1].ContainsChannelSubscriberAuthoritative(readCtx, channelID, 2, "metadata-user")
		if err == nil {
			require.False(t, observed, "source membership point must observe the acknowledged removal")
		}
	})
	t.Run("subscriber_nonempty", func(t *testing.T) {
		readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		observed, err := nodes[1].HasChannelSubscribersAuthoritative(readCtx, channelID, 2)
		if err == nil {
			require.False(t, observed)
		}
	})
	dropped.Store(false)
	require.Eventually(t, func() bool {
		healed, err := nodes[1].defaultSlotProxy.GetChannelForPermission(ctx, channelID, 2)
		return err == nil && healed.Ban == 1
	}, 5*time.Second, 20*time.Millisecond)
	healed, found, err := nodes[1].GetUserChannelMembership(ctx, "metadata-user", channelID, 2)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), healed.DeletedToSeq)
	healedDevice, err := nodes[1].GetDeviceMetadataAuthoritative(ctx, device.UID, device.DeviceFlag, device.DeviceID, device.AppInstanceID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), healedDevice.SessionGeneration)
	healedContains, err := nodes[1].ContainsChannelSubscriberAuthoritative(ctx, channelID, 2, "metadata-user")
	require.NoError(t, err)
	require.False(t, healedContains)
}
