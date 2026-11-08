package keyedmutex

import (
	"fmt"
	"sync"
	"testing"
)

// lucky-v2: after heavy concurrent use of many keys the lock table must be empty again.
func TestKeyedMutex_NoLeakAfter10kKeys(t *testing.T) {
	km := New()
	const keys = 10000
	var wg sync.WaitGroup
	for i := 0; i < keys; i++ {
		for rep := 0; rep < 3; rep++ { // contended: same key from 3 goroutines
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				unlock := km.Lock(fmt.Sprintf("k%d", i))
				unlock()
			}(i)
		}
	}
	wg.Wait()
	km.mu.Lock()
	n := len(km.locks)
	km.mu.Unlock()
	if n != 0 {
		t.Fatalf("lock table leaked %d entries", n)
	}
}
