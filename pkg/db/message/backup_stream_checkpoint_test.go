package message

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
)

func TestBackupStreamParserRejectsLogStartBeyondHW(t *testing.T) {
	var payload bytes.Buffer
	payload.Write(messageBackupSnapshotMagic[:])
	if err := binary.Write(&payload, binary.BigEndian, messageBackupSnapshotVersion); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(&payload, binary.BigEndian, uint16(1)); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(&payload, binary.BigEndian, uint32(1)); err != nil {
		t.Fatal(err)
	}
	if err := writeBackupString(&payload, "room:1"); err != nil {
		t.Fatal(err)
	}
	if err := writeBackupString(&payload, "room"); err != nil {
		t.Fatal(err)
	}
	if err := payload.WriteByte(1); err != nil {
		t.Fatal(err)
	}
	payload.Write(encodeCheckpoint(Checkpoint{LogStartOffset: 2, HW: 1}))
	if err := writeBackupUvarint(&payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := writeBackupUvarint(&payload, 0); err != nil {
		t.Fatal(err)
	}
	checksum := crc32.ChecksumIEEE(payload.Bytes())
	if err := binary.Write(&payload, binary.BigEndian, checksum); err != nil {
		t.Fatal(err)
	}

	body := payload.Bytes()
	_, err := ReplayBackupSnapshotReader(
		context.Background(), bytes.NewReader(body), int64(len(body)),
		func(BackupSnapshotBoundary) error { return nil },
		func(BackupSnapshotRecord) error { return nil },
	)
	if !errors.Is(err, dberrors.ErrCorruptState) {
		t.Fatalf("ReplayBackupSnapshotReader() error = %v, want corrupt state", err)
	}
}
