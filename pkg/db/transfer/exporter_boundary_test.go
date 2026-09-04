package transfer

import (
	"errors"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestRowInt64RejectsUint64Overflow(t *testing.T) {
	if _, err := rowInt64(map[string]any{"value": ^uint64(0)}, "value"); !errors.Is(err, ErrValidation) {
		t.Fatalf("rowInt64() error = %v, want ErrValidation", err)
	}
}

func TestExportChannelRecordClassifiesInvalidDirectoryProjection(t *testing.T) {
	_, err := exportChannelRecord(1, metadb.InspectRow{
		"channel_id":                      "g1",
		"channel_type":                    int64(2),
		"ban":                             int64(0),
		"disband":                         int64(0),
		"send_ban":                        int64(0),
		"allow_stranger":                  int64(1),
		"large":                           int64(0),
		"subscriber_mutation_version":     uint64(1),
		"directory_projection_state":      uint64(255),
		"directory_projection_generation": uint64(1),
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("exportChannelRecord() error = %v, want ErrValidation", err)
	}
}
