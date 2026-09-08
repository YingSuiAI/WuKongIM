//go:build integration

package user_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/contracts/protocolmeta"
	clusterinfra "github.com/WuKongIM/WuKongIM/internal/infra/cluster"
	userusecase "github.com/WuKongIM/WuKongIM/internal/usecase/user"
	"github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

type auditPausedDeviceWriter struct {
	userusecase.DeviceStore
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *auditPausedDeviceWriter) UpsertDevice(ctx context.Context, device metadb.Device) error {
	if device.SessionGeneration == 1 {
		s.once.Do(func() { close(s.ready) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.DeviceStore.UpsertDevice(ctx, device)
}

func TestAuditUpdateTokenConcurrentOlderRequestCannotReplaceNewIncarnation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	nodes := auditCredentialCluster(t, ctx)
	uid := "audit-token-user"
	route, err := nodes[0].RouteKey(uid)
	if err != nil {
		t.Fatal(err)
	}
	var leader *cluster.Node
	for _, node := range nodes {
		if node.NodeID() == route.Leader {
			leader = node
		}
	}
	if leader == nil {
		t.Fatal("credential slot leader missing")
	}
	store := clusterinfra.NewUserMetadataStore(leader)
	if err := store.CreateUser(ctx, metadb.User{UID: uid}); err != nil {
		t.Fatal(err)
	}
	writer := &auditPausedDeviceWriter{DeviceStore: store, ready: make(chan struct{}), release: make(chan struct{})}
	app := userusecase.New(userusecase.Options{Users: store, Devices: writer, DeviceReader: store})
	old := userusecase.UpdateTokenCommand{UID: uid, DeviceID: "audit-device", AppInstanceID: "audit-app", DeviceSessionID: "audit-ds-old", IMSessionID: "audit-im-old",
		InstallationGeneration: 1, SessionGeneration: 1, AuthorizationFence: 1, Token: "local-fiction-old", DeviceFlag: protocolmeta.DeviceFlagApp, DeviceLevel: protocolmeta.DeviceLevelSlave}
	oldDone := make(chan error, 1)
	go func() { oldDone <- app.UpdateToken(ctx, old) }()
	select {
	case <-writer.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	newer := old
	newer.SessionGeneration, newer.AuthorizationFence = 2, 2
	newer.DeviceSessionID, newer.IMSessionID, newer.Token = "audit-ds-new", "audit-im-new", "local-fiction-new"
	newErr := app.UpdateToken(ctx, newer)
	close(writer.release)
	var oldErr error
	select {
	case oldErr = <-oldDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if newErr != nil {
		t.Fatalf("new incarnation update: %v", newErr)
	}
	if !errors.Is(oldErr, metadb.ErrStaleMeta) {
		t.Errorf("delayed older UpdateToken error=%v, want ErrStaleMeta", oldErr)
	}
	stored, err := leader.GetDeviceMetadata(ctx, uid, int64(old.DeviceFlag), old.DeviceID, old.AppInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SessionGeneration != 2 || stored.Token != newer.Token || stored.IMSessionID != newer.IMSessionID {
		t.Errorf("new incarnation overwritten: session=%d fence=%d", stored.SessionGeneration, stored.AuthorizationFence)
	}
	if err := app.UpdateToken(ctx, newer); err != nil {
		t.Fatalf("same-incarnation retry must remain legal: %v", err)
	}
	for _, node := range nodes {
		if node.NodeID() == leader.NodeID() {
			continue
		}
		remoteStore := clusterinfra.NewUserMetadataStore(node)
		remoteApp := userusecase.New(userusecase.Options{Users: remoteStore, Devices: remoteStore, DeviceReader: remoteStore})
		// RPC errors retain their existing transport wrapper, but the stale
		// result must not become success on a non-owner HTTP ingress either.
		if err := remoteApp.UpdateToken(ctx, old); err == nil || !strings.Contains(err.Error(), metadb.ErrStaleMeta.Error()) {
			t.Fatalf("remote stale credential result=%v", err)
		}
		if err := remoteApp.UpdateToken(ctx, newer); err != nil {
			t.Fatalf("remote same-incarnation retry: %v", err)
		}
		break
	}
	stored, err = leader.GetDeviceMetadata(ctx, uid, int64(old.DeviceFlag), old.DeviceID, old.AppInstanceID)
	if err != nil || stored.SessionGeneration != 2 || stored.Token != newer.Token {
		t.Fatal("forwarded stale request replaced the current credential")
	}
}

func auditCredentialCluster(t *testing.T, ctx context.Context) []*cluster.Node {
	t.Helper()
	var voters []cluster.ControlVoter
	for index := 1; index <= 3; index++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		voters = append(voters, cluster.ControlVoter{NodeID: uint64(index), Addr: addr})
	}
	var nodes []*cluster.Node
	for _, voter := range voters {
		cfg := cluster.Config{NodeID: voter.NodeID, ListenAddr: voter.Addr, DataDir: t.TempDir()}
		cfg.Control.ClusterID, cfg.Control.Voters, cfg.Control.AllowBootstrap = "audit-device-credential", voters, true
		cfg.Slots.InitialSlotCount, cfg.Slots.HashSlotCount, cfg.Slots.ReplicaCount = 10, 256, 3
		node, err := cluster.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	results := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *cluster.Node) { results <- node.Start(ctx) }(node)
	}
	var firstErr error
	for range nodes {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if err := node.Stop(context.Background()); err != nil {
				t.Errorf("cluster stop: %v", err)
			}
		}
	})
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	for _, node := range nodes {
		for {
			if err := node.ProbeWriteReady(ctx); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return nodes
}
