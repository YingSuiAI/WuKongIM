package fsm

import (
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestSubscriberMutationResultVersionCodec(t *testing.T) {
	want := metadb.SubscriberMutationResult{RequestedCount: 2, ChangedCount: 1, Version: 42}
	got, err := DecodeSubscriberMutationResult(EncodeSubscriberMutationResult(&want))
	if err != nil || got != want {
		t.Fatalf("versioned result = %+v, err=%v, want %+v", got, err, want)
	}
	legacy, err := DecodeSubscriberMutationResult([]byte{'W', 'K', 'S', 'M', 1, 2, 1})
	if err != nil || legacy != (metadb.SubscriberMutationResult{RequestedCount: 2, ChangedCount: 1}) {
		t.Fatalf("legacy result = %+v, err=%v", legacy, err)
	}
	for _, data := range [][]byte{
		{'W', 'K', 'S', 'M', 1, 2, 1, 9},
		{'W', 'K', 'S', 'M', 2, 2, 1},
		{'W', 'K', 'S', 'M', 2, 2, 1, 42, 9},
	} {
		if _, err := DecodeSubscriberMutationResult(data); err == nil {
			t.Fatalf("accepted malformed result %v", data)
		}
	}
}
