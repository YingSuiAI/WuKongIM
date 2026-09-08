package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/propose"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
)

func TestAuditDeviceCredentialStaleResultReachesProposerCaller(t *testing.T) {
	runtime := &recordingSlotRuntime{future: recordingSlotFuture{data: []byte(metafsm.ApplyResultStaleMeta)}}
	command := metafsm.EncodeUpsertDeviceCommand(metadb.Device{UID: "audit-user", DeviceFlag: 1, DeviceID: "audit-device", AppInstanceID: "audit-app"})
	err := (defaultSlotProposer{runtime: runtime}).Propose(context.Background(), 1, propose.EncodePayload(7, command))
	if !errors.Is(err, metadb.ErrStaleMeta) {
		t.Fatalf("stale credential result became success: %v", err)
	}
	if err := mapSlotApplyResult(metafsm.EncodeNoopCommand(), []byte(metafsm.ApplyResultStaleMeta)); err != nil {
		t.Fatalf("unrelated command mapping changed: %v", err)
	}
}
