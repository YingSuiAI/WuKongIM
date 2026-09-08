//go:build integration

package meta_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestAuditCredentialNoopCannotPoisonAnotherSlotInPhysicalCommit(t *testing.T) {
	ctx := context.Background()
	type replicaState struct {
		device       metadb.Device
		user         metadb.User
		userFound    bool
		slot1, slot2 uint64
	}
	var replicas []replicaState
	for _, groupSize := range []int{2, 1} {
		db, observer := metadb.AuditDeviceCoBatchDB(t, groupSize)
		slot1, err := fsm.NewStateMachine(db, 1)
		if err != nil {
			t.Fatal(err)
		}
		slot2, err := fsm.NewStateMachine(db, 2)
		if err != nil {
			t.Fatal(err)
		}
		newer := auditCoBatchDevice(2)
		if _, err := slot1.Apply(ctx, multiraft.Command{SlotID: 1, Index: 1, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(newer)}); err != nil {
			t.Fatal(err)
		}
		observer.Reset()
		type applyResult struct {
			slot   int
			result []byte
			err    error
		}
		results := make(chan applyResult, 2)
		start := make(chan struct{})
		go func() {
			<-start
			result, err := slot1.Apply(ctx, multiraft.Command{SlotID: 1, Index: 2, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(1))})
			results <- applyResult{1, result, err}
		}()
		go func() {
			<-start
			result, err := slot2.Apply(ctx, multiraft.Command{SlotID: 2, Index: 1, Term: 1, Data: fsm.EncodeCreateUserCommand(metadb.User{UID: "audit-valid-user"})})
			results <- applyResult{2, result, err}
		}()
		close(start)
		for range 2 {
			select {
			case result := <-results:
				want := fsm.ApplyResultOK
				if result.slot == 1 {
					want = fsm.ApplyResultStaleMeta
				}
				if result.err != nil || string(result.result) != want {
					t.Errorf("groupSize=%d slot=%d result=%q err=%v want=%q", groupSize, result.slot, result.result, result.err, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("FSM physical group did not terminate")
			}
		}
		groups := observer.Groups()
		if groupSize == 2 {
			if len(groups) == 0 || groups[0].Requests != 2 {
				t.Fatalf("co-batch precondition not proved: %+v", groups)
			}
			t.Logf("shared physical group requests=%d err=%v", groups[0].Requests, groups[0].Err)
		} else {
			for _, group := range groups {
				if group.Requests != 1 {
					t.Fatalf("separate replica unexpectedly grouped %+v", groups)
				}
			}
		}
		stored, err := db.ForSlot(1).GetDevice(ctx, newer.UID, newer.DeviceFlag, newer.DeviceID, newer.AppInstanceID)
		if err != nil {
			t.Fatal(err)
		}
		user, userErr := db.ForSlot(2).GetUser(ctx, "audit-valid-user")
		if userErr != nil {
			t.Errorf("groupSize=%d valid Slot write lost: %v", groupSize, userErr)
		}
		index1, err := db.SlotAppliedIndex(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		index2, err := db.SlotAppliedIndex(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if index1 != 2 || index2 != 1 {
			t.Errorf("groupSize=%d applied indexes=%d,%d", groupSize, index1, index2)
		}
		replicas = append(replicas, replicaState{stored, user, userErr == nil, index1, index2})
		// A resolved negative must not poison the next genuine write on either FSM.
		if result, err := slot1.Apply(ctx, multiraft.Command{SlotID: 1, Index: 3, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(3))}); err != nil || string(result) != fsm.ApplyResultOK {
			t.Fatalf("next credential write: %q %v", result, err)
		}
		if result, err := slot2.Apply(ctx, multiraft.Command{SlotID: 2, Index: 2, Term: 1, Data: fsm.EncodeCreateUserCommand(metadb.User{UID: "audit-next-user"})}); err != nil || string(result) != fsm.ApplyResultOK {
			t.Fatalf("next unrelated write: %q %v", result, err)
		}
		if _, err := db.ForSlot(2).GetUser(ctx, "audit-next-user"); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(replicas[0], replicas[1]) {
		t.Error("identical Raft commands produced different replica states depending on physical commit grouping")
	}
}

func auditCoBatchDevice(generation uint64) metadb.Device {
	return metadb.Device{UID: "audit-user", DeviceFlag: 1, DeviceID: "audit-device", AppInstanceID: "audit-app", DeviceSessionID: "audit-ds", IMSessionID: "audit-im",
		InstallationGeneration: 1, SessionGeneration: generation, AuthorizationFence: generation, Token: "local-fiction-credential"}
}

func TestAuditCredentialCommitFailureDoesNotPublishNoopOrAdvanceWatermarks(t *testing.T) {
	ctx := context.Background()
	db, observer := metadb.AuditDeviceCoBatchDB(t, 2)
	slot1, err := fsm.NewStateMachine(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	slot2, err := fsm.NewStateMachine(db, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slot1.Apply(ctx, multiraft.Command{SlotID: 1, Index: 1, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(2))}); err != nil {
		t.Fatal(err)
	}
	observer.Reset()
	physicalErr := errors.New("audit: physical metadata commit unavailable")
	observer.SetCommitError(physicalErr)
	results := make(chan error, 2)
	start := make(chan struct{})
	go func() {
		<-start
		_, err := slot1.Apply(ctx, multiraft.Command{SlotID: 1, Index: 2, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(1))})
		results <- err
	}()
	go func() {
		<-start
		_, err := slot2.Apply(ctx, multiraft.Command{SlotID: 2, Index: 1, Term: 1, Data: fsm.EncodeCreateUserCommand(metadb.User{UID: "audit-valid-user"})})
		results <- err
	}()
	close(start)
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, physicalErr) {
				t.Errorf("physical failure became a successful/no-op result: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("physical failure did not terminate")
		}
	}
	groups := observer.Groups()
	if len(groups) != 1 || groups[0].Requests != 2 || !errors.Is(groups[0].Err, physicalErr) {
		t.Fatalf("physical error group=%+v", groups)
	}
	if index, err := db.SlotAppliedIndex(ctx, 1); err != nil || index != 1 {
		t.Fatalf("stale command advanced without durability: index=%d err=%v", index, err)
	}
	if index, err := db.SlotAppliedIndex(ctx, 2); err != nil || index != 0 {
		t.Fatalf("valid command advanced without durability: index=%d err=%v", index, err)
	}
	if _, err := db.ForSlot(2).GetUser(ctx, "audit-valid-user"); !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("uncommitted valid user appeared: %v", err)
	}
	observer.SetCommitError(nil)
	if result, err := slot2.Apply(ctx, multiraft.Command{SlotID: 2, Index: 1, Term: 1, Data: fsm.EncodeCreateUserCommand(metadb.User{UID: "audit-valid-user"})}); err != nil || string(result) != fsm.ApplyResultOK {
		t.Fatalf("caller retry after infrastructure recovery: %q %v", result, err)
	}
}

func TestAuditCredentialHashSlotLockPreventsStaleCrossRequestReads(t *testing.T) {
	ctx := context.Background()
	db, observer := metadb.AuditDeviceCoBatchDB(t, 2)
	sm, err := fsm.NewStateMachine(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply(ctx, multiraft.Command{SlotID: 1, Index: 1, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(2))}); err != nil {
		t.Fatal(err)
	}
	observer.Reset()
	entered, release := observer.BlockNextCommit()
	var releaseOnce sync.Once
	releaseCommit := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCommit)
	type applyResult struct {
		result []byte
		err    error
	}
	newDone, oldDone := make(chan applyResult, 1), make(chan applyResult, 1)
	go func() {
		result, err := sm.Apply(ctx, multiraft.Command{SlotID: 1, Index: 2, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(3))})
		newDone <- applyResult{result, err}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("new credential never reached physical commit")
	}
	go func() {
		result, err := sm.Apply(ctx, multiraft.Command{SlotID: 1, Index: 3, Term: 1, Data: fsm.EncodeUpsertDeviceCommand(auditCoBatchDevice(2))})
		oldDone <- applyResult{result, err}
	}()
	select {
	case result := <-oldDone:
		t.Fatalf("second same-hash-slot request escaped before first fsync: %q %v", result.result, result.err)
	case <-time.After(25 * time.Millisecond):
	}
	releaseCommit()
	for index, done := range []<-chan applyResult{newDone, oldDone} {
		select {
		case result := <-done:
			want := fsm.ApplyResultOK
			if index == 1 {
				want = fsm.ApplyResultStaleMeta
			}
			if result.err != nil || string(result.result) != want {
				t.Fatalf("same-hash-slot ordered result=%q err=%v want=%q", result.result, result.err, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("ordered same-hash-slot request did not finish")
		}
	}
	for _, group := range observer.Groups() {
		if group.Requests != 1 {
			t.Fatalf("same hashslot co-batched while previous held commit lock: %+v", group)
		}
	}
	want := auditCoBatchDevice(3)
	stored, err := db.ForSlot(1).GetDevice(ctx, want.UID, want.DeviceFlag, want.DeviceID, want.AppInstanceID)
	if err != nil || stored != want {
		t.Fatal("second request failed to observe the first durable credential")
	}
	if index, err := db.SlotAppliedIndex(ctx, 1); err != nil || index != 3 {
		t.Fatalf("ordered watermarks: %d %v", index, err)
	}
}
