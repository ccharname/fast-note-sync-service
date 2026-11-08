package service

import (
	"fmt"
	"sort"

	"github.com/haierkeys/fast-note-sync-service/pkg/keyedmutex"
)

// lucky-v2: one process-wide lock per note (uid+vaultID+pathHash). Every code path that
// writes a note body / deletes / renames takes it, so server-side merge sees a stable "S"
// and REST / MCP / background writers cannot interleave with it. Entries are ref-counted
// and removed when idle (see pkg/keyedmutex), so the table does not leak.
// lucky-v2：进程内每篇笔记一把锁，所有写正文/删除/重命名的入口都取它。

var noteLocks = keyedmutex.New()

func noteLockKey(uid, vaultID int64, pathHash string) string {
	return fmt.Sprintf("note|%d|%d|%s", uid, vaultID, pathHash)
}

// lockNotes locks all keys in lexicographic order (deadlock-free for multi-key callers such
// as rename) and returns a single unlock func.
func lockNotes(keys ...string) func() {
	ks := append([]string(nil), keys...)
	sort.Strings(ks)
	uniq := ks[:0]
	for i, k := range ks {
		if i == 0 || k != ks[i-1] {
			uniq = append(uniq, k)
		}
	}
	unlocks := make([]func(), 0, len(uniq))
	for _, k := range uniq {
		unlocks = append(unlocks, noteLocks.Lock(k))
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}
