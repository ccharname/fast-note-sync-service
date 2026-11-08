package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/haierkeys/fast-note-sync-service/pkg/keyedmutex"
)

// noteWriteLocks serializes every write path of one note (uid, vault, pathHash) inside this process.
// FNS runs as a single instance, so an in-process lock is enough; no distributed lock is needed.
// noteWriteLocks 在进程内串行化同一笔记 (uid, vault, pathHash) 的所有写路径。
// FNS 为单实例部署，进程内锁足够，不引入分布式锁。
var noteWriteLocks = keyedmutex.New()

type noteLocksHeldKey struct{}

// LockNote locks the given note paths (same uid and vault) and returns a ctx that remembers the held keys,
// so nested LockNote calls on the same keys (e.g. NoteModify handler -> ModifyOrCreate) are no-ops
// instead of deadlocking. Keys are always taken in sorted order so multi-path callers (rename) cannot deadlock.
// LockNote 锁定同一 uid/vault 下的若干笔记路径，返回记住已持有 key 的 ctx；对同一 key 的嵌套调用是空操作，
// 避免死锁（例如 NoteModify 处理器 -> ModifyOrCreate）。多路径调用方（重命名）按排序顺序加锁，避免互相死锁。
func LockNote(ctx context.Context, uid int64, vault string, pathHashes ...string) (context.Context, func()) {
	held, _ := ctx.Value(noteLocksHeldKey{}).(map[string]struct{})

	keys := make([]string, 0, len(pathHashes))
	seen := make(map[string]struct{}, len(pathHashes))
	for _, ph := range pathHashes {
		k := fmt.Sprintf("note_write_%d_%s_%s", uid, vault, ph)
		if _, ok := held[k]; ok {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ctx, func() {}
	}
	sort.Strings(keys)

	next := make(map[string]struct{}, len(held)+len(keys))
	for k := range held {
		next[k] = struct{}{}
	}
	unlocks := make([]func(), 0, len(keys))
	for _, k := range keys {
		unlocks = append(unlocks, noteWriteLocks.Lock(k))
		next[k] = struct{}{}
	}
	return context.WithValue(ctx, noteLocksHeldKey{}, next), func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}
