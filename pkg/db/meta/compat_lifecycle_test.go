package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
)

func TestClosedCompatibilityShardMigrationTaskGCFailsClosed(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	shard := db.ForHashSlot(1)

	if _, err := shard.PlanTerminalChannelMigrationTaskGC(context.Background(), 1, 1); !errors.Is(err, dberrors.ErrClosed) {
		t.Fatalf("PlanTerminalChannelMigrationTaskGC() error = %v, want ErrClosed", err)
	}
	if _, err := shard.DeleteTerminalChannelMigrationTasksBefore(context.Background(), 1, 1); !errors.Is(err, dberrors.ErrClosed) {
		t.Fatalf("DeleteTerminalChannelMigrationTasksBefore() error = %v, want ErrClosed", err)
	}
}
