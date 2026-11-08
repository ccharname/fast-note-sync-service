package service

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// lucky-v2 observability: one structured log line per serverMerge op, plus counters that the
// health endpoint exposes.

type mergeStats struct {
	mu          sync.Mutex
	reasons     map[string]int64
	ops         atomic.Int64
	maxLockWait atomic.Int64 // ns
	maxTotal    atomic.Int64 // ns
}

var opLogger atomic.Pointer[zap.Logger]

// SetOpLogger installs the application logger (zap.L() is never replaced in this server, so
// without this the serverMerge logs would go to a no-op logger).
func SetOpLogger(l *zap.Logger) { opLogger.Store(l) }

func opLog() *zap.Logger {
	if l := opLogger.Load(); l != nil {
		return l
	}
	return zap.L()
}

var stats = &mergeStats{reasons: map[string]int64{}}

func (m *mergeStats) inc(reason string) {
	m.mu.Lock()
	m.reasons[reason]++
	m.mu.Unlock()
}

func storeMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

// MergeStats returns the counters for the health endpoint.
func MergeStats() map[string]any {
	stats.mu.Lock()
	rs := make(map[string]int64, len(stats.reasons))
	for k, v := range stats.reasons {
		rs[k] = v
	}
	stats.mu.Unlock()
	return map[string]any{
		"ops":           stats.ops.Load(),
		"reasons":       rs,
		"maxLockWaitMs": float64(stats.maxLockWait.Load()) / 1e6,
		"maxOpMs":       float64(stats.maxTotal.Load()) / 1e6,
		"pendingIntents": pendingN.Load(),
	}
}

// logOp records one finished op. reason is one of: fastforward, unchanged, merged,
// conflict_overlap, conflict_nobase, conflict_budget, rejected_*, replay, rollforward.
func (s *noteService) logOp(kind, reason string, uid, vid int64, path, opID string, rev int64, lockWait time.Duration, t0 time.Time) {
	total := time.Since(t0)
	stats.ops.Add(1)
	stats.inc(kind + ":" + reason)
	storeMax(&stats.maxLockWait, int64(lockWait))
	storeMax(&stats.maxTotal, int64(total))
	opLog().Info("serverMerge op",
		zap.String("kind", kind), zap.String("reason", reason),
		zap.Int64("uid", uid), zap.Int64("vault", vid), zap.String("path", path), zap.String("opId", opID),
		zap.Int64("rev", rev), zap.Duration("lockWait", lockWait), zap.Duration("total", total))
}
