//go:build integration

package cluster

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/control"
	"github.com/WuKongIM/WuKongIM/pkg/controller"
)

// Exercise the production Node composition: the listener is not the durable
// membership address used by the real Controller promotion fences.
func TestNodeDefaultControllerPromotionUsesAdvertisedIdentity(t *testing.T) {
	for _, wrongIdentity := range []bool{false, true} {
		name := "advertised_identity"
		if wrongIdentity {
			name = "wrong_advertised_identity_rejected"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			seedAddr, targetAddr := freeTCPAddr(t), freeTCPAddr(t)
			wildcard := func(addr string) string {
				_, port, err := net.SplitHostPort(addr)
				if err != nil {
					t.Fatal(err)
				}
				return net.JoinHostPort("0.0.0.0", port)
			}
			seed, err := New(Config{
				NodeID: 1, ListenAddr: wildcard(seedAddr), DataDir: t.TempDir(),
				Control: ControlConfig{ClusterID: "controller-advertised-identity", AllowBootstrap: true,
					Voters: []ControlVoter{{NodeID: 1, Addr: seedAddr}}},
				Slots: SlotConfig{InitialSlotCount: 1, HashSlotCount: 4, ReplicaCount: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = seed.Stop(context.Background()) })
			if err := seed.Start(ctx); err != nil {
				t.Fatal(err)
			}
			waitForControllerTasksDrained(t, ctx, seed)
			if _, err := seed.JoinNode(ctx, control.JoinNodeRequest{NodeID: 2, Addr: targetAddr}); err != nil {
				t.Fatal(err)
			}
			if _, err := seed.ActivateNode(ctx, control.ActivateNodeRequest{NodeID: 2}); err != nil {
				t.Fatal(err)
			}
			reserved, err := seed.PromoteControllerVoter(ctx, control.PromoteControllerVoterRequest{NodeID: 2, ReserveOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			advertiseAddr := targetAddr
			if wrongIdentity {
				advertiseAddr = "wrong-target:7001"
			}
			target, err := New(Config{
				NodeID: 2, ListenAddr: wildcard(targetAddr), DataDir: t.TempDir(),
				Control: ControlConfig{ClusterID: "controller-advertised-identity"},
				Join:    JoinConfig{Seeds: []string{seedAddr}, AdvertiseAddr: advertiseAddr, Token: "test-join-token"},
				Slots:   SlotConfig{ReplicaCount: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = target.Stop(context.Background()) })
			if err := target.Start(ctx); err != nil {
				t.Fatal(err)
			}
			conn, err := net.DialTimeout("tcp", targetAddr, time.Second)
			if err != nil {
				t.Fatalf("wildcard listener must remain reachable through advertised address: %v", err)
			}
			_ = conn.Close()
			request := controller.PrepareControllerVoterRequest{
				NodeID: 2, ClusterID: target.cfg.Control.ClusterID, ExpectedRevision: reserved.Revision,
				NextVoters: []controller.Voter{{NodeID: 1, Addr: seedAddr}, {NodeID: 2, Addr: targetAddr}},
			}
			if !wrongIdentity {
				request.NextVoters[1].Addr = target.cfg.ListenAddr
				if _, err := target.PrepareControllerVoter(ctx, request); err == nil || !strings.Contains(err.Error(), "does not match preserved addr") {
					t.Fatalf("listener substituted for next voter identity = %v; want preserved-address rejection", err)
				}
				request.NextVoters[1].Addr = targetAddr
			}
			result, err := target.PrepareControllerVoter(ctx, request)
			if wrongIdentity {
				if err == nil || !strings.Contains(err.Error(), "durable promotion reservation") {
					t.Fatalf("wrong advertised identity prepare = %#v, %v; want reservation rejection", result, err)
				}
				if _, err := os.Stat(filepath.Join(target.cfg.Control.StateDir, "cluster-state.mirror-before-controller-voter-promotion.json")); !os.IsNotExist(err) {
					t.Fatalf("rejected prepare moved durable mirror: %v", err)
				}
				return
			}
			if err != nil || !result.Prepared {
				t.Fatalf("prepare with listener=%s advertised=%s = %#v, %v", target.cfg.ListenAddr, advertiseAddr, result, err)
			}
			// The real source adds/catches up/promotes the target; no forged Raft proof.
			promoted, err := seed.PromoteControllerVoter(ctx, control.PromoteControllerVoterRequest{NodeID: 2})
			if err != nil || len(promoted.NextVoters) != 2 || promoted.NextVoters[0] != 1 || promoted.NextVoters[1] != 2 {
				t.Fatalf("live promotion = %#v, %v", promoted, err)
			}
			// After promotion, a static voter restart must use the same identity.
			cfg := target.cfg
			cfg.Join = JoinConfig{}
			cfg.Control.Role = ControlRoleVoter
			cfg.Control.Voters = []ControlVoter{{NodeID: 1, Addr: seedAddr}, {NodeID: 2, Addr: targetAddr}}
			if err := target.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			target, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if result, err := target.PrepareControllerVoter(ctx, request); err != nil || !result.Prepared {
				t.Fatalf("static voter restart preparation retry = %#v, %v", result, err)
			}
		})
	}
}
