//go:build integration

package app

import (
	"context"
	"errors"
	"testing"
	"time"

	clusterinfra "github.com/WuKongIM/WuKongIM/internal/infra/cluster"
	"github.com/WuKongIM/WuKongIM/internal/runtime/channelappend"
	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusterpkg "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	coregateway "github.com/WuKongIM/WuKongIM/pkg/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

type failOnceRejoinMembershipIndex struct {
	*clusterinfra.ChannelMetadataStore
	fail     bool
	observed []metadb.PlatformMembershipRejoin
}

func sendThreeNodeGroupPacketAs(t *testing.T, app *App, uid, channelID, clientMsgNo string, clientSeq uint64) *frame.SendackPacket {
	t.Helper()
	writes := &sendackSmokeSessionWrites{}
	sess := newSendackSmokeSession(writes)
	sess.SetValue(coregateway.SessionValueUID, uid)
	sess.SetValue(coregateway.SessionValueProtocolVersion, uint8(frame.LatestVersion))
	packet := &frame.SendPacket{ClientSeq: clientSeq, ClientMsgNo: clientMsgNo, ChannelID: channelID, ChannelType: frame.ChannelTypeGroup, Payload: []byte(clientMsgNo)}
	if err := app.Handler().OnFrame(coregateway.Context{Session: sess, RequestContext: context.Background()}, packet); err != nil {
		t.Fatal(err)
	}
	ack := writes.requireOnlySendack(t)
	if ack.ReasonCode != frame.ReasonSuccess {
		t.Fatalf("send %s reason=%v", clientMsgNo, ack.ReasonCode)
	}
	return ack
}

func (i *failOnceRejoinMembershipIndex) RejoinUserChannelMembership(ctx context.Context, rejoin metadb.PlatformMembershipRejoin) error {
	i.observed = append(i.observed, rejoin)
	if i.fail {
		i.fail = false
		return errors.New("injected UID Slot failure")
	}
	return i.ChannelMetadataStore.RejoinUserChannelMembership(ctx, rejoin)
}

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

func TestThreeNodeSubscriberChunksInvalidateRemoteFanoutSnapshot(t *testing.T) {
	apps, nodes := startThreeNodeAuthApps(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const channelID = "group-subscriber-chunk-three"
	if err := nodes[0].UpsertChannelMetadata(ctx, metadb.Channel{ChannelID: channelID, ChannelType: 2}); err != nil {
		t.Fatal(err)
	}
	first, err := nodes[0].AddChannelSubscribersCounted(ctx, channelID, 2, []string{"u1"}, 1)
	if err != nil || first.Version != 1 {
		t.Fatalf("first chunk = %+v, err=%v", first, err)
	}
	page := func(app *App, version uint64) []string {
		t.Helper()
		result, err := app.deliveryMeta.NextSubscriberPage(ctx, channelappend.SubscriberPageRequest{
			ChannelID:                 channelappend.ChannelID{ID: channelID, Type: frame.ChannelTypeGroup},
			SubscriberMutationVersion: version, Limit: 10,
		})
		if err != nil {
			t.Fatalf("NextSubscriberPage(version=%d): %v", version, err)
		}
		uids := make([]string, 0, len(result.Recipients))
		for _, recipient := range result.Recipients {
			uids = append(uids, recipient.UID)
		}
		return uids
	}
	if got := page(apps[1], first.Version); len(got) != 1 || got[0] != "u1" {
		t.Fatalf("first remote fanout = %v", got)
	}
	second, err := nodes[2].AddChannelSubscribersCounted(ctx, channelID, 2, []string{"u2"}, 1)
	if err != nil || second.Version != 2 {
		t.Fatalf("second same-proposal chunk = %+v, err=%v", second, err)
	}
	if got := page(apps[1], second.Version); len(got) != 2 || got[0] != "u1" || got[1] != "u2" {
		t.Fatalf("cached remote fanout after second chunk = %v", got)
	}
	removed, err := nodes[0].RemoveChannelSubscribersCounted(ctx, channelID, 2, []string{"u1"}, 1)
	if err != nil || removed.Version != 3 {
		t.Fatalf("same-proposal removal = %+v, err=%v", removed, err)
	}
	if got := page(apps[1], removed.Version); len(got) != 1 || got[0] != "u2" {
		t.Fatalf("cached remote fanout after removal = %v", got)
	}
}

func TestThreeNodeServiceRejoinFailedUIDCompensatesThenRecovers(t *testing.T) {
	apps, nodes := startThreeNodeAuthApps(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	const channelID = "group-service-rejoin-three"
	const uid = "rejoin-u1"
	const sender = "rejoin-u2"
	if err := nodes[0].UpsertChannelMetadata(ctx, metadb.Channel{ChannelID: channelID, ChannelType: 2}); err != nil {
		t.Fatal(err)
	}
	member := channelusecase.SubscriberCommand{ChannelID: channelID, ChannelType: 2, Subscribers: []string{uid}}
	if err := apps[0].channels.AddSubscribers(ctx, channelusecase.SubscriberCommand{ChannelID: channelID, ChannelType: 2, Subscribers: []string{uid, sender}}); err != nil {
		t.Fatal(err)
	}
	remoteRecipients := func() []string {
		t.Helper()
		channel, err := nodes[1].GetChannelMetadata(ctx, channelID, 2)
		if err != nil {
			t.Fatal(err)
		}
		page, err := apps[1].deliveryMeta.NextSubscriberPage(ctx, channelappend.SubscriberPageRequest{
			ChannelID:                 channelappend.ChannelID{ID: channelID, Type: frame.ChannelTypeGroup},
			SubscriberMutationVersion: channel.SubscriberMutationVersion, Limit: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		uids := make([]string, 0, len(page.Recipients))
		for _, recipient := range page.Recipients {
			uids = append(uids, recipient.UID)
		}
		return uids
	}
	if got := remoteRecipients(); len(got) != 2 || got[0] != uid || got[1] != sender {
		t.Fatalf("initial remote fanout=%v", got)
	}
	first := sendThreeNodeGroupPacketAs(t, apps[0], uid, channelID, "before-remove", 1)
	if err := apps[0].channels.RemoveSubscribers(ctx, member); err != nil {
		t.Fatal(err)
	}
	if got := remoteRecipients(); len(got) != 1 || got[0] != sender {
		t.Fatalf("removed remote fanout=%v", got)
	}
	if _, err := apps[1].Messages().SyncChannelMessages(ctx, messageusecase.SyncChannelMessagesQuery{LoginUID: uid, ChannelID: channelID, ChannelType: 2, StartMessageSeq: 1, PullMode: messageusecase.PullModeUp, Limit: 10}); !errors.Is(err, messageusecase.ErrSyncMembershipRequired) {
		t.Fatalf("removed member remote history error=%v", err)
	}
	interval := sendThreeNodeGroupPacketAs(t, apps[0], sender, channelID, "removed-interval", 2)
	cmd := channelusecase.ServiceRejoinCommand{ChannelID: channelID, ChannelType: 2, UID: uid, MembershipEpoch: 3, PreviousRemovedMessageSeq: first.MessageSeq, JoinedMessageSeq: interval.MessageSeq, RepairSameEpoch: true}
	store := clusterinfra.NewChannelMetadataStore(nodes[2], apps[2].ensureChannelAppendMetadataCache(), apps[2].goroutines)
	faultIndex := &failOnceRejoinMembershipIndex{ChannelMetadataStore: store, fail: true}
	faulty := channelusecase.New(channelusecase.Options{Store: store, MembershipIndex: faultIndex, CommittedTail: store, SubscriberMutationObserver: channelAppendSubscriberMutationObserver{app: apps[2]}})
	if _, err := faulty.RejoinSubscriber(ctx, cmd); err == nil {
		t.Fatal("injected UID failure unexpectedly succeeded")
	}
	if got := remoteRecipients(); len(got) != 1 || got[0] != sender {
		t.Fatalf("failed rejoin left realtime fanout=%v", got)
	}
	if row, found, err := nodes[2].GetUserChannelMembership(ctx, uid, channelID, 2); err != nil || !found || !row.Tombstone {
		t.Fatalf("UID after compensation row=%+v found=%t err=%v", row, found, err)
	}
	state, err := faulty.RejoinSubscriber(ctx, cmd)
	if err != nil || !state.Ready || state.MembershipEpoch != 3 || state.JoinSeq != interval.MessageSeq+1 {
		t.Fatalf("retry state=%+v err=%v requests=%+v", state, err, faultIndex.observed)
	}
	readback, err := apps[1].channels.CheckRejoinSubscriber(ctx, cmd)
	if err != nil || !readback.Ready || readback.MembershipEpoch != 3 || readback.JoinSeq != interval.MessageSeq+1 {
		t.Fatalf("remote readback=%+v err=%v", readback, err)
	}
	if got := remoteRecipients(); len(got) != 2 || got[0] != uid || got[1] != sender {
		t.Fatalf("restored remote fanout=%v", got)
	}
	after := sendThreeNodeGroupPacketAs(t, apps[0], sender, channelID, "after-rejoin", 3)
	page, err := apps[1].Messages().SyncChannelMessages(ctx, messageusecase.SyncChannelMessagesQuery{LoginUID: uid, ChannelID: channelID, ChannelType: 2, StartMessageSeq: 1, PullMode: messageusecase.PullModeUp, Limit: 10})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].MessageSeq != after.MessageSeq {
		t.Fatalf("remote history after rejoin page=%+v err=%v", page, err)
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
