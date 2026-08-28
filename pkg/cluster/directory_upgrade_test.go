package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestDirectoryUpgradePendingFencesSlotStartup(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, metadb.DirectoryProjectionUpgradePendingFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	node := &Node{cfg: Config{DataDir: dir}}
	if err := node.ensureDefaultSlots(); err == nil || !strings.Contains(err.Error(), "upgrade incomplete") {
		t.Fatalf("startup error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "slotmeta")); !os.IsNotExist(err) {
		t.Fatalf("started metadata before upgrade completed: %v", err)
	}
}
