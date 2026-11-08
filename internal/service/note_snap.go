package service

import (
	"sync"
	"sync/atomic"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// lucky-v2: immutable content snapshots and the idempotency (opId) table, stored next to
// the note body: <note folder>/snap/<sha256>.txt and <note folder>/snap/ops.json.
// lucky-v2：不可变正文快照与 opId 幂等表，存放在笔记目录下 snap/。

const (
	snapKeepRecent  = 16               // newest snapshots always kept // 最近 N 个必留
	snapKeepMax     = 64               // hard cap per note // 单篇上限
	snapKeepAge     = 7 * 24 * time.Hour // older ones kept only while referenced and this young // 其余仅保留 7 天内被引用的
	snapPruneSlack  = 8                // prune only after this many new snapshots // 新增超过此数才触发清理
)

var shaRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// ContentSha returns the sha256 hex identity of a note body (lucky-v2 protocol `sha`).
func ContentSha(s string) string { return sha256Hex(s) }

// noteFolderPath mirrors dao.GetNoteFolderPath (relative to the data directory / cwd).
func noteFolderPath(uid, noteID int64) string {
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "note", fmt.Sprintf("n_%d", noteID))
}

func snapDir(uid, noteID int64) string { return filepath.Join(noteFolderPath(uid, noteID), "snap") }

// atomicWrite: temp + fsync + rename + fsync(dir), shared with the note body writer.
func atomicWrite(dir, name string, data []byte) error { return dao.AtomicWriteFile(dir, name, data) }

// writeSnap stores content as snap/<sha>.txt (never overwrites an existing file) and returns
// its sha. Existing snapshots are left untouched: retention is journal-reference based, so no
// per-write Chtimes / metadata churn. Durable (fsync file + dir) before returning.
func writeSnap(dir, content string) (string, error) {
	sha := sha256Hex(content)
	p := filepath.Join(dir, sha+".txt")
	if _, err := os.Stat(p); err == nil {
		return sha, nil
	}
	if err := atomicWrite(dir, sha+".txt", []byte(content)); err != nil {
		return sha, err
	}
	noteSnapWritten(dir)
	return sha, nil
}

// loadSnap returns the snapshot for sha, verifying its content really hashes to sha.
func loadSnap(dir, sha string) (string, bool) {
	if !shaRe.MatchString(sha) {
		return "", false
	}
	b, err := os.ReadFile(filepath.Join(dir, sha+".txt"))
	if err != nil {
		return "", false
	}
	if sha256Hex(string(b)) != sha {
		return "", false
	}
	return string(b), true
}

var snapCounters sync.Map // dir -> *atomic.Int32 (snapshots written since last prune)
var snapPruning sync.Map  // dir -> struct{} (prune in flight)

// noteSnapWritten counts a new snapshot and, once snapPruneSlack of them accumulated, schedules
// a background prune: cleanup never runs inside the note lock.
func noteSnapWritten(dir string) {
	v, _ := snapCounters.LoadOrStore(dir, new(atomic.Int32))
	if v.(*atomic.Int32).Add(1) < snapPruneSlack {
		return
	}
	if _, busy := snapPruning.LoadOrStore(dir, struct{}{}); busy {
		return
	}
	go func() {
		defer snapPruning.Delete(dir)
		pruneSnaps(dir)
		if c, ok := snapCounters.Load(dir); ok {
			c.(*atomic.Int32).Store(0)
		}
	}()
}

// pruneSnaps keeps the newest snapKeepRecent snapshots; older ones only while younger than
// snapKeepAge and referenced by the journal, never more than snapKeepMax in total.
func pruneSnaps(dir string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type fi struct {
		name string
		mod  time.Time
	}
	var fs []fi
	for _, e := range ents {
		n := e.Name()
		if len(n) != 68 || n[64:] != ".txt" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		fs = append(fs, fi{n, info.ModTime()})
	}
	if len(fs) <= snapKeepRecent {
		return
	}
	refs := journalReferencedShas(dir)
	sort.Slice(fs, func(i, j int) bool { return fs[i].mod.After(fs[j].mod) })
	cutoff := time.Now().Add(-snapKeepAge)
	kept := 0
	for i, f := range fs {
		keep := i < snapKeepRecent || (f.mod.After(cutoff) && refs[f.name[:64]])
		if keep && kept >= snapKeepMax {
			keep = false
		}
		if keep {
			kept++
			continue
		}
		_ = os.Remove(filepath.Join(dir, f.name))
	}
}

// moveSnapDir moves the snapshot dir (journal included) of a renamed/created note to the new
// note folder: O(1) rename when the destination does not exist, per-file moves otherwise.
// Source and destination parent directories are fsynced; journal merge errors are returned.
func moveSnapDir(from, to string) error {
	if _, err := os.Stat(from); err != nil {
		return nil
	}
	if err := fp("adopt_before"); err != nil {
		return err
	}
	if _, err := os.Stat(to); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(to), 0755); err != nil {
			return err
		}
		if err := os.Rename(from, to); err != nil {
			return err
		}
		syncDir(filepath.Dir(to))
		syncDir(filepath.Dir(from))
		return fp("adopt_after")
	}
	ents, err := os.ReadDir(from)
	if err != nil {
		return err
	}
	for _, e := range ents {
		n := e.Name()
		dst := filepath.Join(to, n)
		if n == journalName {
			b, err := os.ReadFile(filepath.Join(from, n))
			if err != nil {
				return err
			}
			if err := healJournalTail(dst); err != nil {
				return err
			}
			f, err := os.OpenFile(dst, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			if _, err := f.Write(b); err != nil {
				f.Close()
				return err
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return err
			}
			f.Close()
			continue
		}
		if _, err := os.Stat(dst); err == nil {
			continue
		}
		if err := os.Rename(filepath.Join(from, n), dst); err != nil {
			return err
		}
	}
	syncDir(to)
	if err := os.RemoveAll(from); err != nil {
		return err
	}
	syncDir(filepath.Dir(from))
	return fp("adopt_after")
}

func syncDir(d string) {
	if df, err := os.Open(d); err == nil {
		_ = df.Sync()
		df.Close()
	}
}
