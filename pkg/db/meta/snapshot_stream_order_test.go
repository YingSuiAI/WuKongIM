package meta

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
)

func TestVerifyBackupSnapshotRejectsRuntimeOnlyRegisteredSpan(t *testing.T) {
	payload, _ := encodeSlotSnapshotPayload([]uint16{5}, []snapshotEntry{{
		Key:   encodeChannelRuntimeMetaRowKey(5, "room", 1, channelRuntimeMetaPrimaryFamilyID),
		Value: []byte("runtime ownership must remain local"),
	}})

	_, err := VerifyBackupHashSlotSnapshotReader(
		context.Background(), []uint16{5}, bytes.NewReader(payload), int64(len(payload)),
	)
	if !errors.Is(err, dberrors.ErrInvalidArgument) {
		t.Fatalf("VerifyBackupHashSlotSnapshotReader() error = %v, want semantic-span rejection", err)
	}
}

func TestVerifyBackupSnapshotRejectsNonCanonicalRegisteredSpanOrder(t *testing.T) {
	payload, _ := encodeSlotSnapshotPayload([]uint16{5}, []snapshotEntry{
		{Key: encodeChannelRowKey(5, "room", 1, channelPrimaryFamilyID), Value: []byte("channel")},
		{Key: encodeUserRowKey(5, "user", userPrimaryFamilyID), Value: []byte("user")},
	})

	_, err := VerifyBackupHashSlotSnapshotReader(
		context.Background(), []uint16{5}, bytes.NewReader(payload), int64(len(payload)),
	)
	if !errors.Is(err, dberrors.ErrCorruptValue) {
		t.Fatalf("VerifyBackupHashSlotSnapshotReader() error = %v, want non-canonical order rejection", err)
	}
}

func TestVerifyBackupSnapshotRejectsDuplicateKey(t *testing.T) {
	key := encodeUserRowKey(5, "user", userPrimaryFamilyID)
	payload, _ := encodeSlotSnapshotPayload([]uint16{5}, []snapshotEntry{
		{Key: key, Value: encodeUserValue(User{UID: "user", Token: "first"})},
		{Key: key, Value: encodeUserValue(User{UID: "user", Token: "second"})},
	})

	_, err := VerifyBackupHashSlotSnapshotReader(
		context.Background(), []uint16{5}, bytes.NewReader(payload), int64(len(payload)),
	)
	if !errors.Is(err, dberrors.ErrCorruptValue) {
		t.Fatalf("VerifyBackupHashSlotSnapshotReader() error = %v, want duplicate-key rejection", err)
	}
}
