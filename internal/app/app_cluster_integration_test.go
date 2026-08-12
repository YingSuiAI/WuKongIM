//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterpkg "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func TestStaticMultiNodeClusterStartsControllerVoters(t *testing.T) {
	addrs := []string{freeSendackSmokeTCPAddr(t), freeSendackSmokeTCPAddr(t), freeSendackSmokeTCPAddr(t)}
	voters := []clusterpkg.ControlVoter{
		{NodeID: 1, Addr: addrs[0]},
		{NodeID: 2, Addr: addrs[1]},
		{NodeID: 3, Addr: addrs[2]},
	}
	apps := make([]*App, 0, len(voters))
	for _, voter := range voters {
		cfg := Config{
			NodeID:  voter.NodeID,
			DataDir: shortAppTestDataDir(t),
			Cluster: clusterpkg.Config{
				NodeID:     voter.NodeID,
				ListenAddr: voter.Addr,
				DataDir:    shortAppTestDataDir(t),
				Control: clusterpkg.ControlConfig{
					ClusterID:      "internal-app-static-three",
					Voters:         voters,
					AllowBootstrap: true,
				},
				Slots: clusterpkg.SlotConfig{
					InitialSlotCount: 1,
					HashSlotCount:    4,
					ReplicaCount:     3,
				},
				Channel:  clusterpkg.ChannelConfig{TickInterval: time.Millisecond},
				Timeouts: clusterpkg.TimeoutConfig{Start: 5 * time.Second},
			},
		}
		app, err := newTestApp(t, cfg)
		if err != nil {
			t.Fatalf("New(node=%d) error = %v", voter.NodeID, err)
		}
		apps = append(apps, app)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	errs := make(chan error, len(apps))
	for _, app := range apps {
		app := app
		go func() { errs <- app.Start(startCtx) }()
		t.Cleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			_ = app.Stop(stopCtx)
		})
	}
	for range apps {
		if err := <-errs; err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	}

	nodes := make([]*clusterpkg.Node, 0, len(apps))
	for _, app := range apps {
		node, ok := app.cluster.(*clusterpkg.Node)
		if !ok {
			t.Fatalf("cluster runtime = %T, want *clusterpkg.Node", app.cluster)
		}
		nodes = append(nodes, node)
	}
	waitAppClusterSnapshotsConverge(t, nodes)

	ack := sendDefaultMetaSmokePacket(t, apps[0], channelruntime.ChannelID{ID: "room-static-three", Type: 1}, 1, "client-static-three-1")
	if ack.ReasonCode != frame.ReasonSuccess {
		t.Fatalf("sendack reason = %v, want %v", ack.ReasonCode, frame.ReasonSuccess)
	}
	if ack.MessageSeq != 1 {
		t.Fatalf("sendack message seq = %d, want 1", ack.MessageSeq)
	}
}

func TestWKProtoTokenAuthReadsCurrentSlotLeaderFromNonLeaderGateway(t *testing.T) {
	apps, nodes := startThreeNodeAuthApps(t)
	const (
		uid           = "auth-rotation-user"
		deviceID      = "device-web-1"
		appInstanceID = "app-instance-web-1"
	)
	leader := waitAuthSlotLeader(t, nodes, uid)
	var nonLeader *App
	for i, node := range nodes {
		if node.NodeID() != leader {
			nonLeader = apps[i]
			break
		}
	}
	if nonLeader == nil {
		t.Fatal("non-leader gateway not found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer := nodes[0]
	if err := writer.UpsertDeviceMetadata(ctx, metadb.Device{
		UID: uid, DeviceFlag: int64(frame.WEB), DeviceID: deviceID, AppInstanceID: appInstanceID,
		DeviceSessionID: "device-session-v1", IMSessionID: "im-session-v1",
		InstallationGeneration: 7, SessionGeneration: 1, AuthorizationFence: 11,
		Token: "token-v1", DeviceLevel: int64(frame.DeviceLevelSlave),
	}); err != nil {
		t.Fatalf("UpsertDeviceMetadata(v1) error = %v", err)
	}
	credential, err := nonLeader.verifyWKProtoToken(uid, frame.WEB, deviceID, appInstanceID, 1, "token-v1")
	if err != nil {
		t.Fatalf("verifyWKProtoToken(v1) error=%v", err)
	}
	if credential.DeviceLevel != frame.DeviceLevelSlave || credential.DeviceSessionID != "device-session-v1" || credential.IMSessionID != "im-session-v1" || credential.InstallationGeneration != 7 || credential.AuthorizationFence != 11 {
		t.Fatalf("verifyWKProtoToken(v1) credential=%#v, want exact v1 credential", credential)
	}

	if err := writer.UpsertDeviceMetadata(ctx, metadb.Device{
		UID: uid, DeviceFlag: int64(frame.WEB), DeviceID: deviceID, AppInstanceID: appInstanceID,
		DeviceSessionID: "device-session-v2", IMSessionID: "im-session-v2",
		InstallationGeneration: 7, SessionGeneration: 2, AuthorizationFence: 12,
		Token: "token-v2", DeviceLevel: int64(frame.DeviceLevelMaster),
	}); err != nil {
		t.Fatalf("UpsertDeviceMetadata(v2) error = %v", err)
	}
	if _, err := nonLeader.verifyWKProtoToken(uid, frame.WEB, deviceID, appInstanceID, 1, "token-v1"); err == nil {
		t.Fatal("verifyWKProtoToken(old token) error = nil after rotation")
	}
	if _, err := nonLeader.verifyWKProtoToken(uid, frame.WEB, deviceID, appInstanceID, 1, "token-v2"); err == nil {
		t.Fatal("verifyWKProtoToken(old session generation) error = nil after rotation")
	}
	credential, err = nonLeader.verifyWKProtoToken(uid, frame.WEB, deviceID, appInstanceID, 2, "token-v2")
	if err != nil {
		t.Fatalf("verifyWKProtoToken(v2) error=%v", err)
	}
	if credential.DeviceLevel != frame.DeviceLevelMaster || credential.DeviceSessionID != "device-session-v2" || credential.IMSessionID != "im-session-v2" || credential.InstallationGeneration != 7 || credential.AuthorizationFence != 12 {
		t.Fatalf("verifyWKProtoToken(v2) credential=%#v, want exact v2 credential", credential)
	}
}

func startThreeNodeAuthApps(t *testing.T) ([]*App, []*clusterpkg.Node) {
	t.Helper()
	addrs := []string{freeSendackSmokeTCPAddr(t), freeSendackSmokeTCPAddr(t), freeSendackSmokeTCPAddr(t)}
	voters := []clusterpkg.ControlVoter{
		{NodeID: 1, Addr: addrs[0]},
		{NodeID: 2, Addr: addrs[1]},
		{NodeID: 3, Addr: addrs[2]},
	}
	apps := make([]*App, 0, len(voters))
	for _, voter := range voters {
		cfg := Config{
			NodeID:  voter.NodeID,
			DataDir: shortAppTestDataDir(t),
			Cluster: clusterpkg.Config{
				NodeID: voter.NodeID, ListenAddr: voter.Addr, DataDir: shortAppTestDataDir(t),
				Control:  clusterpkg.ControlConfig{ClusterID: "internal-app-auth-three", Voters: voters, AllowBootstrap: true},
				Slots:    clusterpkg.SlotConfig{InitialSlotCount: 1, HashSlotCount: 4, ReplicaCount: 3},
				Channel:  clusterpkg.ChannelConfig{TickInterval: time.Millisecond},
				Timeouts: clusterpkg.TimeoutConfig{Start: 5 * time.Second},
			},
		}
		app, err := newTestApp(t, cfg)
		if err != nil {
			t.Fatalf("New(node=%d) error = %v", voter.NodeID, err)
		}
		apps = append(apps, app)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	errs := make(chan error, len(apps))
	for _, app := range apps {
		app := app
		go func() { errs <- app.Start(startCtx) }()
		t.Cleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			_ = app.Stop(stopCtx)
		})
	}
	for range apps {
		if err := <-errs; err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	}
	nodes := make([]*clusterpkg.Node, 0, len(apps))
	for _, app := range apps {
		node, ok := app.cluster.(*clusterpkg.Node)
		if !ok {
			t.Fatalf("cluster runtime = %T, want *clusterpkg.Node", app.cluster)
		}
		nodes = append(nodes, node)
	}
	waitAppClusterSnapshotsConverge(t, nodes)
	return apps, nodes
}

func waitAuthSlotLeader(t *testing.T, nodes []*clusterpkg.Node, uid string) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var leaders []uint64
	for time.Now().Before(deadline) {
		leaders = leaders[:0]
		for _, node := range nodes {
			route, err := node.RouteKey(uid)
			if err != nil {
				leaders = append(leaders, 0)
				continue
			}
			leaders = append(leaders, route.Leader)
		}
		if len(leaders) == len(nodes) && leaders[0] != 0 && leaders[0] == leaders[1] && leaders[1] == leaders[2] {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("slot leader did not converge for %q: %v", uid, leaders)
	return 0
}
