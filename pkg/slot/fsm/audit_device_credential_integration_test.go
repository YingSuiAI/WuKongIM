//go:build integration

package fsm

import (
	"context"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestAuditDeviceCredentialCannotRegressWithinFSMBatch(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	newer := metadb.Device{UID: "audit-user", DeviceFlag: 1, DeviceID: "audit-device", AppInstanceID: "audit-app",
		DeviceSessionID: "audit-ds-new", IMSessionID: "audit-im-new", InstallationGeneration: 1, SessionGeneration: 2, AuthorizationFence: 2, Token: "local-fiction-new"}
	older := newer
	older.SessionGeneration, older.AuthorizationFence = 1, 1
	older.DeviceSessionID, older.IMSessionID, older.Token = "audit-ds-old", "audit-im-old", "local-fiction-old"
	results, err := sm.(multiraft.BatchStateMachine).ApplyBatch(ctx, []multiraft.Command{
		{SlotID: 11, Index: 1, Term: 1, Data: EncodeUpsertDeviceCommand(newer)},
		{SlotID: 11, Index: 2, Term: 1, Data: EncodeUpsertDeviceCommand(older)},
		{SlotID: 11, Index: 3, Term: 1, Data: EncodeCreateUserCommand(metadb.User{UID: "unrelated-user"})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 || string(results[0]) != ApplyResultOK || string(results[1]) != ApplyResultStaleMeta || string(results[2]) != ApplyResultOK {
		t.Errorf("FSM result alignment=%q, want ok/stale_meta/ok", results)
	}
	stored, err := db.ForSlot(11).GetDevice(ctx, newer.UID, newer.DeviceFlag, newer.DeviceID, newer.AppInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if stored != newer {
		t.Errorf("credential regressed: installation=%d session=%d fence=%d", stored.InstallationGeneration, stored.SessionGeneration, stored.AuthorizationFence)
	}
	if index, err := sm.(multiraft.DurableAppliedStateMachine).DurableAppliedIndex(ctx); err != nil || index != 3 {
		t.Fatalf("applied index=%d err=%v", index, err)
	}
}

func TestAuditDeviceCredentialPreservesExistingGenerationAndLegacySemantics(t *testing.T) {
	base := metadb.Device{UID: "audit-device-user", DeviceFlag: 1, DeviceID: "audit-device", AppInstanceID: "audit-app", DeviceSessionID: "audit-ds", IMSessionID: "audit-im",
		InstallationGeneration: 3, SessionGeneration: 4, AuthorizationFence: 5, Token: "local-fiction-base"}
	for _, test := range []struct {
		name   string
		mutate func(*metadb.Device)
		stale  bool
	}{
		{name: "same incarnation retry", mutate: func(*metadb.Device) {}},
		{name: "same incarnation token update", mutate: func(d *metadb.Device) { d.Token = "local-fiction-rotated" }},
		{name: "same incarnation quit", mutate: func(d *metadb.Device) { d.Token = "" }},
		{name: "older installation", mutate: func(d *metadb.Device) {
			d.InstallationGeneration = 2
			d.SessionGeneration = 100
			d.AuthorizationFence = 100
		}, stale: true},
		{name: "older session", mutate: func(d *metadb.Device) { d.SessionGeneration = 3; d.AuthorizationFence = 100 }, stale: true},
		{name: "older authorization", mutate: func(d *metadb.Device) { d.AuthorizationFence = 4 }, stale: true},
		{name: "new session resets subordinate fence", mutate: func(d *metadb.Device) { d.SessionGeneration = 5; d.AuthorizationFence = 1 }},
		{name: "new installation resets subordinate generation", mutate: func(d *metadb.Device) {
			d.InstallationGeneration = 4
			d.SessionGeneration = 1
			d.AuthorizationFence = 1
		}},
		{name: "legacy cannot replace issued incarnation", mutate: func(d *metadb.Device) {
			d.InstallationGeneration = 0
			d.SessionGeneration = 0
			d.AuthorizationFence = 0
		}, stale: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := openTestDB(t)
			sm := mustNewStateMachine(t, db, 11)
			if _, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, Index: 1, Term: 1, Data: EncodeUpsertDeviceCommand(base)}); err != nil {
				t.Fatal(err)
			}
			next := base
			test.mutate(&next)
			result, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, Index: 2, Term: 1, Data: EncodeUpsertDeviceCommand(next)})
			if err != nil {
				t.Fatal(err)
			}
			wantResult, wantRow := ApplyResultOK, next
			if test.stale {
				wantResult, wantRow = ApplyResultStaleMeta, base
			}
			if string(result) != wantResult {
				t.Errorf("result=%q want=%q", result, wantResult)
			}
			stored, err := db.ForSlot(11).GetDevice(ctx, base.UID, base.DeviceFlag, base.DeviceID, base.AppInstanceID)
			if err != nil {
				t.Fatal(err)
			}
			if stored != wantRow {
				t.Errorf("credential state differs from expected admitted incarnation")
			}
		})
	}
	t.Run("legacy zero generation row remains writable", func(t *testing.T) {
		ctx := context.Background()
		db := openTestDB(t)
		sm := mustNewStateMachine(t, db, 11)
		legacy := base
		legacy.InstallationGeneration, legacy.SessionGeneration, legacy.AuthorizationFence = 0, 0, 0
		for index := uint64(1); index <= 2; index++ {
			legacy.DeviceLevel = int64(index)
			result, err := sm.Apply(ctx, multiraft.Command{SlotID: 11, Index: index, Term: 1, Data: EncodeUpsertDeviceCommand(legacy)})
			if err != nil || string(result) != ApplyResultOK {
				t.Fatalf("legacy apply: %q %v", result, err)
			}
		}
	})
}
