package dao

import (
	"sync"
	"sync/atomic"
	"time"
)

var lastRev atomic.Int64

// Writers hold RLock inside the actual ExecuteWrite callback, before NextRev
// until SQL/content completion. Holding it around the queue wait would release
// too early on caller timeout and can invert the queue's lock order.
var noteCommitBarrier sync.RWMutex

// WithNoteSyncSnapshot waits for revision-bearing note writes to finish, then
// reads metadata while excluding new revision allocation. The cursor remains the
// query-before wall clock, not a revision. Future writes must exceed that cursor,
// including writes in the same millisecond. Only the metadata query belongs here;
// do not call note writers or wait for the write queue from query.
func WithNoteSyncSnapshot(query func() error) (int64, error) {
	return withNoteSyncSnapshot(func() int64 { return time.Now().UnixMilli() }, query)
}

func withNoteSyncSnapshot(now func() int64, query func() error) (int64, error) {
	noteCommitBarrier.Lock()
	defer noteCommitBarrier.Unlock()
	started := now()
	SeedRev(started)
	if err := query(); err != nil {
		return 0, err
	}
	return started, nil
}

// NextRev returns a process-wide strictly increasing millisecond revision that is
// also greater than atLeast (the previous revision of the record being written).
// It is used as note.UpdatedTimestamp so clients can order messages per path (lucky-v2).
// NextRev 返回进程内全局严格递增的毫秒修订号，且大于 atLeast（被写记录的上一个修订号）。
func NextRev(atLeast int64) int64 {
	for {
		last := lastRev.Load()
		next := time.Now().UnixMilli()
		if next <= last {
			next = last + 1
		}
		if next <= atLeast {
			next = atLeast + 1
		}
		if lastRev.CompareAndSwap(last, next) {
			return next
		}
	}
}

// SeedRev raises the process-wide floor to at least v. Called once per user DB (first access)
// with that DB's max(updated_timestamp), so after a restart / clock rollback / restored DB new
// revs still exceed every stored one. Cost: one covering-index scan per user DB per process.
// SeedRev 把全局下限抬到至少 v；每个用户库首次访问时用其 max(updated_timestamp) 调用一次。
func SeedRev(v int64) {
	for {
		last := lastRev.Load()
		if v <= last || lastRev.CompareAndSwap(last, v) {
			return
		}
	}
}

// NoteLock is installed by the service layer: it takes the per-note lock used by all note
// writers. Physical deletes in this package call it (outside ExecuteWrite, to avoid
// lock-vs-write-queue deadlocks) so they serialize with in-flight writes. (lucky-v2)
// NoteLock 由 service 层注入；物理删除在 ExecuteWrite 之外取笔记锁。
var NoteLock = func(uid, vaultID int64, pathHash string) (unlock func()) { return func() {} }
