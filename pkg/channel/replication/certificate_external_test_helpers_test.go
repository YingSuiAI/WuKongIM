package replication_test

import (
	"context"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replication"
)

func syncCertifiedTestMutations(t testing.TB, store replication.ReplicaStore, mutations ...replication.Mutation) {
	t.Helper()
	for _, mutation := range mutations {
		if mutation.Committed == mutation.Manifest.LastOffset && mutation.Committed > mutation.Manifest.BaseOffset {
			first := mutation
			first.Committed = mutation.Manifest.BaseOffset
			assertCertifiedTestSync(t, store, first)
			assertCertifiedTestSync(t, store, mutation)
		} else {
			assertCertifiedTestSync(t, store, mutation)
		}
	}
}

func assertCertifiedTestSync(t testing.TB, store replication.ReplicaStore, mutation replication.Mutation) {
	t.Helper()
	results := store.Sync(context.Background(), []replication.Mutation{mutation})
	if len(results) != 1 || !results[0].Outcome.Durable() || results[0].Err != nil || results[0].Outcome == ch.AppendOutcomeUnknown {
		t.Fatalf("Sync(certificate phase committed=%d) = %+v, want durable", mutation.Committed, results)
	}
}
