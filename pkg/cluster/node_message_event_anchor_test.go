package cluster

import (
	"context"
	"errors"
	"testing"

	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channels"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestParseAgentRunAnchorIdentity(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		wantRunID string
		wantFence uint64
		wantErr   bool
	}{
		{
			name:      "standard v1 envelope",
			payload:   `{"type":"agent.run.anchor","version":1,"payload":{"run_id":" run-1 ","authorization_fence":2}}`,
			wantRunID: "run-1",
			wantFence: 2,
		},
		{
			name:    "flat legacy identity",
			payload: `{"type":"agent.run.anchor","version":1,"run_id":"run-1","authorization_fence":2}`,
			wantErr: true,
		},
		{
			name:    "wrong type",
			payload: `{"type":"message","version":1,"payload":{"run_id":"run-1","authorization_fence":2}}`,
			wantErr: true,
		},
		{
			name:    "wrong version",
			payload: `{"type":"agent.run.anchor","version":2,"payload":{"run_id":"run-1","authorization_fence":2}}`,
			wantErr: true,
		},
		{
			name:    "missing run id",
			payload: `{"type":"agent.run.anchor","version":1,"payload":{"authorization_fence":2}}`,
			wantErr: true,
		},
		{
			name:    "zero authorization fence",
			payload: `{"type":"agent.run.anchor","version":1,"payload":{"run_id":"run-1","authorization_fence":0}}`,
			wantErr: true,
		},
		{
			name:    "malformed json",
			payload: `{"type":`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runID, fence, err := parseAgentRunAnchorIdentity([]byte(tt.payload))
			if tt.wantErr {
				if !errors.Is(err, metadb.ErrInvalidArgument) {
					t.Fatalf("parseAgentRunAnchorIdentity() error = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAgentRunAnchorIdentity() error = %v", err)
			}
			if runID != tt.wantRunID || fence != tt.wantFence {
				t.Fatalf("parseAgentRunAnchorIdentity() = (%q, %d), want (%q, %d)", runID, fence, tt.wantRunID, tt.wantFence)
			}
		})
	}
}

func TestLookupMessageEventAnchorColdLoadsEvictedRuntime(t *testing.T) {
	ctx := context.Background()
	id := channelruntime.ChannelID{ID: "agent-anchor-cold-load", Type: 2}
	runtimeMeta := channelruntime.Meta{
		Key:         channelruntime.ChannelKeyForID(id),
		ID:          id,
		Epoch:       1,
		LeaderEpoch: 1,
		Leader:      1,
		Replicas:    []channelruntime.NodeID{1},
		ISR:         []channelruntime.NodeID{1},
		MinISR:      1,
		Status:      channelruntime.StatusActive,
	}
	storeFactory := channelstore.NewMemoryFactory()
	service, err := channels.NewService(channels.Config{
		LocalNode:    1,
		ReactorCount: 1,
		Store:        storeFactory,
		MetaSource:   channels.NewStaticMetaSource([]channelruntime.Meta{runtimeMeta}),
	})
	if err != nil {
		t.Fatalf("channels.NewService() error = %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })

	const messageID = uint64(2090209378475970560)
	payload := []byte(`{"type":"agent.run.anchor","version":1,"payload":{"run_id":"run-cold-load","authorization_fence":7}}`)
	appendResult, err := service.Append(ctx, channelruntime.AppendRequest{
		ChannelID:            id,
		CommitMode:           channelruntime.CommitModeLocal,
		ExpectedChannelEpoch: 1,
		ExpectedLeaderEpoch:  1,
		Message: channelruntime.Message{
			MessageID:   messageID,
			FromUID:     "agent:42",
			ClientMsgNo: "agent-anchor-run-cold-load",
			Payload:     payload,
		},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	store, err := storeFactory.ChannelStore(runtimeMeta.Key, id)
	if err != nil {
		t.Fatalf("ChannelStore() error = %v", err)
	}
	if err := store.StoreCheckpoint(ctx, channelruntime.Checkpoint{HW: appendResult.MessageSeq}); err != nil {
		t.Fatalf("StoreCheckpoint() error = %v", err)
	}

	evict, err := service.RuntimeEvict(ctx, channelruntime.RuntimeSelector{ChannelIDs: []channelruntime.ChannelID{id}})
	if err != nil {
		t.Fatalf("RuntimeEvict() error = %v", err)
	}
	if evict.Evicted != 1 {
		t.Fatalf("RuntimeEvict() = %#v, want one evicted runtime", evict)
	}
	probe, err := service.RuntimeProbe(ctx, channelruntime.RuntimeSelector{ChannelIDs: []channelruntime.ChannelID{id}})
	if err != nil {
		t.Fatalf("RuntimeProbe(after evict) error = %v", err)
	}
	if len(probe.Missing) != 1 || probe.Missing[0] != id {
		t.Fatalf("RuntimeProbe(after evict).Missing = %#v, want %#v", probe.Missing, []channelruntime.ChannelID{id})
	}

	node := &Node{cfg: Config{NodeID: 1}, channels: service}
	anchor, found, err := node.lookupMessageEventAnchorWithMeta(ctx, metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 1,
		LeaderEpoch:  1,
		Leader:       1,
		Replicas:     []uint64{1},
		ISR:          []uint64{1},
		MinISR:       1,
		Status:       uint8(channelruntime.StatusActive),
	}, messageID)
	if err != nil {
		t.Fatalf("lookupMessageEventAnchorWithMeta() error = %v", err)
	}
	if !found {
		t.Fatal("lookupMessageEventAnchorWithMeta() found = false, want true")
	}
	if anchor.ChannelID != id.ID || anchor.ChannelType != int64(id.Type) || anchor.MessageID != messageID || anchor.FromUID != "agent:42" || anchor.ClientMsgNo != "agent-anchor-run-cold-load" || anchor.RunID != "run-cold-load" || anchor.AuthorizationFence != "7" {
		t.Fatalf("lookupMessageEventAnchorWithMeta() = %#v, want durable anchor identity", anchor)
	}
	probe, err = service.RuntimeProbe(ctx, channelruntime.RuntimeSelector{ChannelIDs: []channelruntime.ChannelID{id}})
	if err != nil {
		t.Fatalf("RuntimeProbe(after lookup) error = %v", err)
	}
	if len(probe.Channels) != 1 || probe.Channels[0].ChannelID != id {
		t.Fatalf("RuntimeProbe(after lookup).Channels = %#v, want reloaded runtime %v", probe.Channels, id)
	}
}
