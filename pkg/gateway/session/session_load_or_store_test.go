package session

import (
	"sync"
	"testing"
)

func TestSessionLoadOrStoreValuePreservesInitializedNilHotState(t *testing.T) {
	sess := newSession(21, "listener-a", "remote-a", "local-a", nil)

	actual, loaded := sess.LoadOrStoreValue(hotSessionValueCrypto, nil)
	if loaded || actual != nil {
		t.Fatalf("first LoadOrStoreValue() = (%#v, %v), want (nil, false)", actual, loaded)
	}

	actual, loaded = sess.LoadOrStoreValue(hotSessionValueCrypto, "replacement")
	if !loaded || actual != nil {
		t.Fatalf("second LoadOrStoreValue() = (%#v, %v), want (nil, true)", actual, loaded)
	}
}

func TestSessionLoadOrStoreValueInitializesHotStateOnce(t *testing.T) {
	sess := newSession(22, "listener-a", "remote-a", "local-a", nil)
	const contenders = 32
	type result struct {
		actual any
		loaded bool
	}

	start := make(chan struct{})
	results := make(chan result, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for value := 0; value < contenders; value++ {
		go func(value int) {
			ready.Done()
			<-start
			actual, loaded := sess.LoadOrStoreValue(hotSessionValueAuthorizationFence, value)
			results <- result{actual: actual, loaded: loaded}
		}(value)
	}
	ready.Wait()
	close(start)

	var initialized any
	initializers := 0
	allResults := make([]result, 0, contenders)
	for i := 0; i < contenders; i++ {
		got := <-results
		allResults = append(allResults, got)
		if !got.loaded {
			initialized = got.actual
			initializers++
		}
	}
	if initializers != 1 {
		t.Fatalf("LoadOrStoreValue() initializers = %d, want exactly 1", initializers)
	}
	for _, got := range allResults {
		if got.actual != initialized {
			t.Fatalf("LoadOrStoreValue() actual = %#v, want initialized value %#v", got.actual, initialized)
		}
	}
}
