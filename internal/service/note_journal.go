package service

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

// lucky-v2: per-note operation journal snap/ops.log (JSON lines, append-only, compacted by
// atomic rewrite). It is both the idempotency table (content-addressed keys) and the
// roll-forward journal for crash consistency:
//   1. snapshot(s)  2. intent line (fsync)  3. content.txt  4. DB row  5. done line
// Any intent without a done line is completed (or aborted) the next time the note is locked.

const (
	journalName       = "ops.log"
	journalCompactAt  = 192 << 10 // bytes // 超过则原子重写压缩
	journalKeepOps    = 256
	journalKeepAge    = 7 * 24 * time.Hour
	journalHardMaxOps = 2048
)

type jrec struct {
	T      string `json:"t"`            // intent | done | op
	ID     string `json:"id,omitempty"` // links intent <-> done
	K      string `json:"k,omitempty"`  // content-addressed key
	Kind   string `json:"kind,omitempty"` // m modify | c conflict-copy | d delete | r rename
	Op     string `json:"op,omitempty"` // client opId (log correlation / ack matching)
	UID    int64  `json:"uid,omitempty"`
	VID    int64  `json:"vid,omitempty"`
	Vault  string `json:"vault,omitempty"`
	Path   string `json:"path,omitempty"`
	PH     string `json:"ph,omitempty"`
	Path2  string `json:"path2,omitempty"` // rename target
	PH2    string `json:"ph2,omitempty"`
	Cl     string `json:"cl,omitempty"` // client name (conflict copy naming)
	Pre    string `json:"pre,omitempty"`  // sha of server body before the op
	PreRev int64  `json:"prv,omitempty"` // rev of the server row before the op (0 = no row)
	Base   string `json:"base,omitempty"`
	Csha   string `json:"csha,omitempty"` // sha of submitted body
	Fsha   string `json:"fsha,omitempty"` // sha of final body
	Ctime  int64  `json:"ctime,omitempty"`
	Mtime  int64  `json:"mtime,omitempty"`
	Rev    int64  `json:"rev,omitempty"`
	Merged bool   `json:"merged,omitempty"`
	Copy   string `json:"copy,omitempty"`
	Reason string `json:"reason,omitempty"`
	Abort  bool   `json:"abort,omitempty"`
	Ts     int64  `json:"ts,omitempty"`
}

func newJournalID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// healJournalTail drops a torn (newline-less) last line before anything is appended: a
// non-fsynced done/op line can be cut by power loss, and appending after the remnant would
// glue two records into one unreadable line.
// healJournalTail 追加前先截掉残缺的末行，避免新记录与残尾粘成一行。
func healJournalTail(p string) error {
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return err
	}
	last := make([]byte, 1)
	if _, err := f.ReadAt(last, st.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	keep := bytes.LastIndexByte(data, '\n') + 1
	if err := f.Truncate(int64(keep)); err != nil {
		return err
	}
	return f.Sync()
}

// journalAppend appends one line. sync=true fsyncs (intents must be durable before step 3).
func journalAppend(dir string, r *jrec, sync bool) error {
	if r.Ts == 0 {
		r.Ts = time.Now().UnixMilli()
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	p := filepath.Join(dir, journalName)
	_, statErr := os.Stat(p)
	if statErr == nil {
		if err := healJournalTail(p); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	info, _ := f.Stat()
	f.Close()
	if sync && statErr != nil { // new file: make the directory entry durable too
		if df, err := os.Open(dir); err == nil {
			_ = df.Sync()
			df.Close()
		}
	}
	if info != nil && info.Size() > journalCompactAt {
		journalCompact(dir)
	}
	return nil
}

type journalState struct {
	done    map[string]*jrec // key -> latest completed op (intent fields + result)
	pending []*jrec          // intents without done, in order
	all     []*jrec          // completed ops in file order
}

func journalRead(dir string) *journalState {
	st := &journalState{done: map[string]*jrec{}}
	if dir == "" {
		return st
	}
	data, err := os.ReadFile(filepath.Join(dir, journalName))
	if err != nil {
		return st
	}
	intents := map[string]*jrec{}
	var order []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		var r jrec
		if json.Unmarshal(sc.Bytes(), &r) != nil {
			continue // torn tail line
		}
		switch r.T {
		case "intent":
			rr := r
			intents[r.ID] = &rr
			order = append(order, r.ID)
		case "done":
			in := intents[r.ID]
			if in == nil {
				continue
			}
			m := *in
			m.T = "op"
			m.Rev, m.Merged, m.Copy, m.Reason, m.Abort, m.Fsha = r.Rev, r.Merged, r.Copy, r.Reason, r.Abort, firstNonEmpty(r.Fsha, in.Fsha)
			m.Ts = r.Ts
			delete(intents, r.ID)
			if !m.Abort && m.K != "" {
				st.done[m.K] = &m
			}
			st.all = append(st.all, &m)
		case "op":
			rr := r
			if rr.K != "" {
				st.done[rr.K] = &rr
			}
			st.all = append(st.all, &rr)
		}
	}
	for _, id := range order {
		if in := intents[id]; in != nil {
			st.pending = append(st.pending, in)
		}
	}
	return st
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// journalCompact rewrites the log atomically: pending intents + completed ops (newest
// journalKeepOps, or younger than journalKeepAge, whichever keeps more; hard capped).
func journalCompact(dir string) {
	st := journalRead(dir)
	ops := st.all
	cutoff := time.Now().Add(-journalKeepAge).UnixMilli()
	start := 0
	if len(ops) > journalKeepOps {
		start = len(ops) - journalKeepOps
		for start > 0 && ops[start-1].Ts > cutoff {
			start--
		}
	}
	if len(ops)-start > journalHardMaxOps {
		start = len(ops) - journalHardMaxOps
	}
	var buf bytes.Buffer
	for _, r := range ops[start:] {
		r.T = "op"
		b, _ := json.Marshal(r)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	for _, r := range st.pending {
		b, _ := json.Marshal(r)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	_ = atomicWrite(dir, journalName, buf.Bytes())
}

// journalReferencedShas: every sha mentioned by journal records still retained.
func journalReferencedShas(dir string) map[string]bool {
	st := journalRead(dir)
	refs := map[string]bool{}
	add := func(r *jrec) {
		for _, s := range []string{r.Pre, r.Base, r.Csha, r.Fsha} {
			if s != "" {
				refs[s] = true
			}
		}
	}
	for _, r := range st.all {
		add(r)
	}
	for _, r := range st.pending {
		add(r)
	}
	return refs
}

// ---- pending (unfinished intents) registry ----

var (
	pendingMu   sync.Mutex
	pendingKeys = map[string]string{} // lock key -> journal dir holding the unfinished intent
	pendingN    atomic.Int64
	scanOnce    sync.Once
)

func markPending(dir string, keys ...string) {
	pendingMu.Lock()
	for _, k := range keys {
		pendingKeys[k] = dir
	}
	pendingN.Store(int64(len(pendingKeys)))
	pendingMu.Unlock()
}

func clearPending(keys ...string) {
	pendingMu.Lock()
	for _, k := range keys {
		delete(pendingKeys, k)
	}
	pendingN.Store(int64(len(pendingKeys)))
	pendingMu.Unlock()
}

func pendingDir(key string) (string, bool) {
	if pendingN.Load() == 0 {
		return "", false
	}
	pendingMu.Lock()
	defer pendingMu.Unlock()
	d, ok := pendingKeys[key]
	return d, ok
}

// scanPending runs once per process, before the first locked write: it registers every
// unfinished intent found under storage/vault/u_*/note/*/snap and storage/vault/u_*/note/_new/*.
func scanPending() {
	var dirs []string
	m1, _ := filepath.Glob(filepath.Join("storage", "vault", "u_*", "note", "n_*", "snap", journalName))
	m2, _ := filepath.Glob(filepath.Join("storage", "vault", "u_*", "note", "_new", "*", journalName))
	for _, f := range append(m1, m2...) {
		dirs = append(dirs, filepath.Dir(f))
	}
	sort.Strings(dirs)
	for _, d := range dirs {
		// cheap pre-filter: only parse logs whose last bytes contain an intent
		if st := journalRead(d); len(st.pending) > 0 {
			for _, in := range st.pending {
				markPending(d, intentKeys(in)...)
			}
		}
	}
}

// resetRecoveryForTest simulates a process restart for the recovery registry.
func resetRecoveryForTest() {
	pendingMu.Lock()
	pendingKeys = map[string]string{}
	pendingN.Store(0)
	pendingMu.Unlock()
	scanOnce = sync.Once{}
}

// intentKeys: every lock key an unfinished intent touches.
func intentKeys(r *jrec) []string {
	keys := []string{noteLockKey(r.UID, r.VID, r.PH)}
	if r.PH2 != "" {
		keys = append(keys, noteLockKey(r.UID, r.VID, r.PH2))
	}
	if r.Kind == "c" {
		for n := 1; n <= conflictCopyMax; n++ {
			keys = append(keys, noteLockKey(r.UID, r.VID, util.EncodeHash32(conflictCopyPath(r.Path, r.Cl, r.Csha, n))))
		}
	}
	return keys
}
