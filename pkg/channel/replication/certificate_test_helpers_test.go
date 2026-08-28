package replication

import (
	"context"
	"testing"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
)

func syncCertifiedTestMutations(t testing.TB, store ReplicaStore, mutations ...Mutation) {
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

func assertCertifiedTestSync(t testing.TB, store ReplicaStore, mutation Mutation) {
	t.Helper()
	results := store.Sync(context.Background(), []Mutation{mutation})
	if len(results) != 1 || !results[0].Outcome.Durable() || results[0].Err != nil {
		t.Fatalf("Sync(certificate phase committed=%d) = %+v, want durable", mutation.Committed, results)
	}
	if results[0].LastOffset != mutation.Manifest.LastOffset || results[0].Outcome == ch.AppendOutcomeUnknown {
		t.Fatalf("Sync(certificate phase committed=%d) = %+v, want exact last %d", mutation.Committed, results, mutation.Manifest.LastOffset)
	}
}
