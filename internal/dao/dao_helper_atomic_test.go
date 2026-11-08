package dao

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// A reader must never observe a truncated / half-written content.txt while it is rewritten:
// old content stays complete until the new one is renamed into place.
func TestSaveContentToFileIsAtomic(t *testing.T) {
	d := &Dao{}
	dir := filepath.Join(t.TempDir(), "n_1")
	a := strings.Repeat("A", 4<<20)
	b := strings.Repeat("B", 4<<20)
	if err := d.SaveContentToFile(dir, "content.txt", a); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var bad atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			got, ok, err := d.LoadContentFromFile(dir, "content.txt")
			if err != nil || !ok || (got != a && got != b) {
				bad.Add(1)
			}
		}
	}()
	for i := 0; i < 60; i++ {
		c := a
		if i%2 == 0 {
			c = b
		}
		if err := d.SaveContentToFile(dir, "content.txt", c); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("reader saw %d torn/missing reads", bad.Load())
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Fatalf("temp files left behind: %v", ents)
	}
}
