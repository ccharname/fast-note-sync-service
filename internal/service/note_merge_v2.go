package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// lucky-v2: server-side three-way merge for NoteModify / NoteDelete / NoteRename requests
// carrying serverMerge=true (spec R1 + R2).
//
// Consistency: every write goes snapshot -> intent -> content/DB -> done (note_journal.go);
// unfinished intents are rolled forward under the note lock before anything else touches the
// note. Idempotency is content-addressed (key = kind|base|sha(C)), opId is only for matching
// Acks and log correlation. The emit callback runs INSIDE the lock so Ack/broadcast enter the
// per-connection ordered queues in write order.

// MergeResult is what a serverMerge operation produced; emit turns it into messages.
type MergeResult struct {
	UID      int64  // set on recovery events (broadcast target user)
	Vault    string // set on recovery events
	Path     string
	PathHash string
	LastTime int64
	Ack      dto.NoteAckExt

	consumed   bool         // recovery event already attached to a request result
	Written    *dto.NoteDTO // main note was written: broadcast to other clients
	Copy       *dto.NoteDTO // conflict copy newly written: broadcast to all clients
	Deleted    *dto.NoteDTO // note deleted: broadcast to other clients
	RenamedOld *dto.NoteDTO // rename: old/new records, broadcast to other clients
	RenamedNew *dto.NoteDTO
}

// NoteServerMerge is implemented by *noteService; ws handlers type-assert to it so the
// NoteService interface (and its generated mock) stays untouched.
type NoteServerMerge interface {
	ServerMergeModify(ctx context.Context, uid int64, p *dto.NoteModifyOrCreateRequest, emit func(*MergeResult)) error
	ServerMergeDelete(ctx context.Context, uid int64, p *dto.NoteDeleteRequest, emit func(*MergeResult)) error
	ServerMergeRename(ctx context.Context, uid int64, p *dto.NoteRenameRequest, emit func(*MergeResult)) error
}

var _ NoteServerMerge = (*noteService)(nil)

// failpoint is a test seam: returning errCrash simulates a process kill at that stage (the
// caller returns immediately, no cleanup); any other error simulates a failing disk/DB.
var failpoint func(stage string) error
var errCrash = errors.New("simulated crash")

func fp(stage string) error {
	if failpoint != nil {
		return failpoint(stage)
	}
	return nil
}

func (s *noteService) renameLockKeys(uid, vaultID int64, p *dto.NoteRenameRequest) []string {
	return []string{noteLockKey(uid, vaultID, renameNewHash(p)), noteLockKey(uid, vaultID, renameOldHash(p))}
}

func renameNewHash(p *dto.NoteRenameRequest) string {
	if p.PathHash != "" {
		return p.PathHash
	}
	return util.EncodeHash32(strings.Trim(p.Path, "/"))
}

func renameOldHash(p *dto.NoteRenameRequest) string {
	if p.OldPathHash != "" {
		return p.OldPathHash
	}
	return util.EncodeHash32(strings.Trim(p.OldPath, "/"))
}

func (s *noteService) getAny(ctx context.Context, pathHash string, vaultID, uid int64) (*domain.Note, error) {
	n, err := s.noteRepo.GetAllByPathHash(ctx, pathHash, vaultID, uid)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	}
	return n, nil
}

func sanitizeClient(name string) string {
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		}
		if b.Len() >= 32 {
			break
		}
	}
	if b.Len() == 0 {
		return "device"
	}
	return b.String()
}

const conflictCopyMax = 50

func conflictCopyPath(path, client, sha string, n int) string {
	base := strings.TrimSuffix(path, filepath.Ext(path))
	name := base + ".conflict-" + sanitizeClient(client) + "-" + sha[:8]
	if n > 1 {
		name += fmt.Sprintf("-%d", n)
	}
	return name + ".md"
}

func newAck(opID string, rev int64, sha string) dto.NoteAckExt {
	return dto.NoteAckExt{OpID: opID, Rev: rev, Sha: sha}
}

func baseKey(baseSha, baseHash string) string {
	if baseSha != "" {
		return baseSha
	}
	if baseHash != "" {
		return "h:" + baseHash
	}
	return ""
}

// ---- locking + recovery ----

func init() {
	dao.NoteLock = func(uid, vaultID int64, pathHash string) func() {
		return lockNotes(noteLockKey(uid, vaultID, pathHash))
	}
}

// RecoveryBroadcast is installed by the websocket layer: it pushes a roll-forward result to
// every connection of the user (called inside the note lock, in commit order).
var RecoveryBroadcast func(uid int64, ev *MergeResult)

// recoverySvc is the service instance used by entries outside noteService (folder, conflict,
// history) to run roll-forward before they write.
var recoverySvc atomic.Pointer[noteService]

// lockRecover is the package-level entry for non-noteService writers.
func lockRecover(ctx context.Context, keys ...string) func() {
	if svc := recoverySvc.Load(); svc != nil {
		return svc.lockKeysAndRecover(ctx, keys)
	}
	return lockNotes(keys...)
}

func (s *noteService) broadcastRecovered(evs []*MergeResult) {
	if RecoveryBroadcast == nil {
		return
	}
	for _, ev := range evs {
		if ev.consumed {
			continue
		}
		RecoveryBroadcast(ev.UID, ev)
	}
}

// lockAndRecover takes the note locks for hashes (plus any extra keys an unfinished intent
// touches, in lexicographic order) and rolls unfinished intents forward before returning.
// Roll-forward results are broadcast to the user's connections.
func (s *noteService) lockAndRecover(ctx context.Context, uid, vid int64, hashes ...string) func() {
	keys := make([]string, 0, len(hashes))
	for _, h := range hashes {
		keys = append(keys, noteLockKey(uid, vid, h))
	}
	return s.lockKeysAndRecover(ctx, keys)
}

func (s *noteService) lockKeysAndRecover(ctx context.Context, keys []string) func() {
	unlock, evs := s.lockKeysAndRecoverEv(ctx, keys)
	s.broadcastRecovered(evs)
	return unlock
}

// lockKeysAndRecoverEv is lockKeysAndRecover but hands the roll-forward results to the caller.
func (s *noteService) lockKeysAndRecoverEv(ctx context.Context, keys []string) (func(), []*MergeResult) {
	scanOnce.Do(scanPending)
	for {
		unlock := lockNotes(keys...)
		if pendingN.Load() == 0 {
			return unlock, nil
		}
		dirs := map[string]bool{}
		for _, k := range keys {
			if d, ok := pendingDir(k); ok {
				dirs[d] = true
			}
		}
		if len(dirs) == 0 {
			return unlock, nil
		}
		held := map[string]bool{}
		for _, k := range keys {
			held[k] = true
		}
		var extra []string
		var ds []string
		for d := range dirs {
			ds = append(ds, d)
			for _, in := range journalRead(d).pending {
				for _, k := range intentKeys(in) {
					if !held[k] {
						held[k] = true
						extra = append(extra, k)
					}
				}
			}
		}
		if len(extra) > 0 {
			unlock()
			keys = append(append([]string(nil), keys...), extra...)
			continue
		}
		sort.Strings(ds)
		var evs []*MergeResult
		for _, d := range ds {
			evs = append(evs, s.rollForwardDir(ctx, d)...)
		}
		return unlock, evs
	}
}

// ---- modify ----

type modifyOutcome int

const (
	outDone modifyOutcome = iota
	outRetry
	outNeedCopyLock
)

// premerge: diff3 computed outside the lock from the current snapshot; the locked section
// only commits it if rev and body are unchanged.
type premerge struct {
	noteID int64
	rev    int64
	sSha   string
	merged string
	status MergeStatus
}

func (s *noteService) premergeModify(ctx context.Context, uid, vaultID int64, p *dto.NoteModifyOrCreateRequest) *premerge {
	if p.BaseSha == "" {
		return nil
	}
	S, err := s.getAny(ctx, p.PathHash, vaultID, uid)
	if err != nil || S == nil || S.Action == domain.NoteActionDelete || S.Content == p.Content {
		return nil
	}
	sSha := sha256Hex(S.Content)
	if sSha == p.BaseSha {
		return nil
	}
	base, ok := loadSnap(snapDir(uid, S.ID), p.BaseSha)
	if !ok {
		return nil
	}
	m, st := mergeBudgeted(base, S.Content, p.Content)
	return &premerge{noteID: S.ID, rev: S.UpdatedTimestamp, sSha: sSha, merged: m, status: st}
}

// ServerMergeModify implements spec §5 (R1/R2).
func (s *noteService) ServerMergeModify(ctx context.Context, uid int64, p *dto.NoteModifyOrCreateRequest, emit func(*MergeResult)) error {
	vaultID, err := s.vaultService.MustGetID(ctx, uid, p.Vault)
	if err != nil {
		return err
	}
	t0 := time.Now()
	var pm *premerge
	for attempt := 0; attempt <= 3; attempt++ {
		if attempt < 3 {
			pm = s.premergeModify(ctx, uid, vaultID, p)
		} else {
			pm = nil // 3 stale retries: compute inside the lock
		}
		tl := time.Now()
		unlock, evs := s.lockKeysAndRecoverEv(ctx, []string{noteLockKey(uid, vaultID, p.PathHash)})
		wait := time.Since(tl)
		held := s.broadcastExceptSameNote(evs, p.PathHash)
		out, err := s.modifyLocked(ctx, uid, vaultID, p, pm, false, emit, wait, t0, held)
		s.broadcastRecovered(held)
		unlock()
		if err != nil {
			return err
		}
		switch out {
		case outDone:
			return nil
		case outRetry:
			continue
		case outNeedCopyLock:
			// Rare: take main + every candidate copy key together (lexicographic order, the same
			// rule as rename), then redo the decision. Nothing has been written yet.
			keys := []string{noteLockKey(uid, vaultID, p.PathHash)}
			cSha := sha256Hex(p.Content)
			for n := 1; n <= conflictCopyMax; n++ {
				keys = append(keys, noteLockKey(uid, vaultID, util.EncodeHash32(conflictCopyPath(p.Path, s.clientName, cSha, n))))
			}
			tl := time.Now()
			unlock, evs := s.lockKeysAndRecoverEv(ctx, keys)
			held := s.broadcastExceptSameNote(evs, p.PathHash)
			_, err := s.modifyLocked(ctx, uid, vaultID, p, nil, true, emit, time.Since(tl), t0, held)
			s.broadcastRecovered(held)
			unlock()
			return err
		}
	}
	return nil
}

func stagingDir(uid, vid int64, pathHash string) string {
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "note", "_new", fmt.Sprintf("%d_%s", vid, pathHash))
}

// journalDirFor: the note's own snap dir, or a staging dir while the note does not exist yet.
func journalDirFor(uid, vid int64, pathHash string, S *domain.Note) string {
	if S != nil {
		return snapDir(uid, S.ID)
	}
	return stagingDir(uid, vid, pathHash)
}

// broadcastExceptSameNote broadcasts roll-forward results immediately, except a modify of the
// note the current request targets: that one is handed to the request (it rides on the replay
// result so the originator's other devices get it, in order, with the Ack).
func (s *noteService) broadcastExceptSameNote(evs []*MergeResult, pathHash string) (held []*MergeResult) {
	var now []*MergeResult
	for _, ev := range evs {
		if ev.Written != nil && ev.PathHash == pathHash {
			held = append(held, ev)
		} else {
			now = append(now, ev)
		}
	}
	s.broadcastRecovered(now)
	return held
}

func (s *noteService) modifyLocked(ctx context.Context, uid, vaultID int64, p *dto.NoteModifyOrCreateRequest, pm *premerge, copyHeld bool, emit func(*MergeResult), lockWait time.Duration, t0 time.Time, held []*MergeResult) (modifyOutcome, error) {
	S, err := s.getAny(ctx, p.PathHash, vaultID, uid)
	if err != nil {
		return outDone, err
	}
	exists := S != nil && S.Action != domain.NoteActionDelete
	jdir := journalDirFor(uid, vaultID, p.PathHash, S)
	C := p.Content
	cSha := sha256Hex(C)
	key := "m|" + baseKey(p.BaseSha, p.BaseHash) + "|" + cSha
	var sSha string
	if exists {
		sSha = sha256Hex(S.Content)
	}

	// content-addressed replay
	if rec := journalRead(jdir).done[key]; rec != nil {
		sameState := exists && sSha == rec.Fsha
		if sameState || (p.OpID != "" && rec.Op == p.OpID) {
			var replayContent string
			if rec.Merged {
				if sameState {
					replayContent = S.Content
				} else if content, ok := loadSnap(jdir, rec.Fsha); ok {
					replayContent = content
				} else {
					// the merged body is no longer retained: never mix an old sha with the
					// current body, tell the client to resync from the server
					var rev int64
					if S != nil {
						rev = S.UpdatedTimestamp
					}
					a := newAck(p.OpID, rev, sSha)
					a.Rejected, a.RejectReason = true, "needResync"
					if exists {
						a.Merged, a.Content = true, S.Content
					}
					emit(&MergeResult{Path: p.Path, PathHash: p.PathHash, LastTime: rev, Ack: a})
					s.logOp("modify", "rejected_needResync", uid, vaultID, p.Path, p.OpID, rev, lockWait, t0)
					return outDone, nil
				}
			}
			rev := rec.Rev
			if sameState {
				rev = S.UpdatedTimestamp
			}
			a := newAck(p.OpID, rev, rec.Fsha)
			a.Merged, a.ConflictCopy = rec.Merged, rec.Copy
			if rec.Merged {
				a.Content = replayContent
			}
			r := &MergeResult{Path: p.Path, PathHash: p.PathHash, LastTime: rev, Ack: a}
			for _, ev := range held { // just rolled forward: let the other devices see it
				if ev.Written != nil && ev.PathHash == p.PathHash && !ev.consumed {
					r.Written, ev.consumed = ev.Written, true
				}
			}
			emit(r)
			s.logOp("modify", "replay", uid, vaultID, p.Path, p.OpID, rev, lockWait, t0)
			return outDone, nil
		}
	}
	if exists && p.CreateOnly {
		return outDone, code.ErrorNoteExist
	}

	final := C
	needWrite := true
	copyNeeded := false
	reason := "fastforward"
	if exists {
		switch {
		case S.Content == C:
			needWrite, reason = false, "unchanged"
		case (p.BaseSha != "" && p.BaseSha == sSha) || (p.BaseSha == "" && p.BaseHash != "" && p.BaseHash == S.ContentHash):
			// fast-forward: client edited on top of the current server version
		default:
			merged, st, haveBase := "", MergeOverlap, false
			if p.BaseSha != "" {
				if pm != nil && pm.noteID == S.ID && pm.rev == S.UpdatedTimestamp && pm.sSha == sSha {
					merged, st, haveBase = pm.merged, pm.status, true
				} else if pm != nil {
					return outRetry, nil // stale premerge: recompute outside the lock
				} else if base, ok := loadSnap(snapDir(uid, S.ID), p.BaseSha); ok {
					merged, st = mergeBudgeted(base, S.Content, C)
					haveBase = true
				}
			}
			switch {
			case !haveBase:
				final, needWrite, copyNeeded, reason = S.Content, false, true, "conflict_nobase"
			case st == MergeBudget:
				final, needWrite, copyNeeded, reason = S.Content, false, true, "conflict_budget"
			case st != MergeOK:
				final, needWrite, copyNeeded, reason = S.Content, false, true, "conflict_overlap"
			case merged == S.Content:
				final, needWrite, reason = S.Content, false, "merged"
			default:
				final, reason = merged, "merged"
			}
		}
	}
	if copyNeeded && !copyHeld {
		return outNeedCopyLock, nil
	}
	finalSha := cSha
	if final != C {
		finalSha = sha256Hex(final)
	}

	res := &MergeResult{Path: p.Path, PathHash: p.PathHash}
	var rev int64
	if S != nil {
		rev = S.UpdatedTimestamp
	}
	stagedCreate := S == nil

	switch {
	case needWrite:
		// 1. snapshots (durable)
		if _, err := writeSnap(jdir, C); err != nil {
			return outDone, fmt.Errorf("snapshot: %w", err)
		}
		if final != C {
			if _, err := writeSnap(jdir, final); err != nil {
				return outDone, fmt.Errorf("snapshot: %w", err)
			}
		}
		if err := fp("after_snapshot"); err != nil {
			return outDone, err
		}
		mtime := p.Mtime
		if final != C {
			mtime = time.Now().UnixMilli()
		}
		ctime := p.Ctime
		if S != nil && S.Ctime > 0 {
			ctime = S.Ctime
		}
		// 2. intent (durable)
		in := &jrec{T: "intent", ID: newJournalID(), K: key, Kind: "m", Op: p.OpID, UID: uid, VID: vaultID, Vault: p.Vault,
			Path: p.Path, PH: p.PathHash, Cl: s.clientName, Pre: sSha, PreRev: rev, Base: baseKey(p.BaseSha, p.BaseHash), Csha: cSha, Fsha: finalSha,
			Ctime: ctime, Mtime: mtime, Reason: reason}
		if err := journalAppend(jdir, in, true); err != nil {
			return outDone, fmt.Errorf("journal: %w", err)
		}
		markPending(jdir, intentKeys(in)...)
		if err := fp("after_intent"); err != nil {
			return outDone, err
		}
		// 3+4. content.txt (atomic) + DB row
		wp := *p
		wp.Content, wp.ContentHash, wp.Mtime, wp.Ctime, wp.CreateOnly = final, util.EncodeHash32(final), mtime, ctime, false
		var pre []*domain.Note
		if S != nil {
			pre = append(pre, S)
		}
		var d *dto.NoteDTO
		var werr error
		if werr = fp("write"); werr == nil {
			_, d, werr = s.modifyOrCreateInner(ctx, uid, &wp, false, pre...)
		}
		if werr != nil {
			if errors.Is(werr, errCrash) {
				return outDone, werr
			}
			s.broadcastRecovered(s.rollForwardDir(ctx, jdir)) // complete or abort; client retry then replays or redoes
			return outDone, werr
		}
		if err := fp("after_write"); err != nil {
			return outDone, err
		}
		rev = d.UpdatedTimestamp
		res.Written = d
		if stagedCreate {
			dir := snapDir(uid, d.ID)
			if err := moveSnapDir(jdir, dir); err != nil {
				if errors.Is(err, errCrash) {
					return outDone, err
				}
				s.logWarn("adopt staging", err)
			} else {
				jdir = dir
			}
		}
		// 5. done (not fsynced: a lost done is re-derived by roll-forward)
		_ = journalAppend(jdir, &jrec{T: "done", ID: in.ID, Rev: rev, Merged: final != C, Fsha: finalSha, Reason: reason}, false)
		clearPending(intentKeys(in)...)
	case copyNeeded:
		// conflict copy: snapshot C, intent, write the copy note, done
		if _, err := writeSnap(jdir, C); err != nil {
			return outDone, fmt.Errorf("snapshot: %w", err)
		}
		if err := fp("after_snapshot"); err != nil {
			return outDone, err
		}
		in := &jrec{T: "intent", ID: newJournalID(), K: key, Kind: "c", Op: p.OpID, UID: uid, VID: vaultID, Vault: p.Vault,
			Path: p.Path, PH: p.PathHash, Cl: s.clientName, Pre: sSha, Base: baseKey(p.BaseSha, p.BaseHash), Csha: cSha, Fsha: finalSha,
			Ctime: p.Ctime, Mtime: p.Mtime, Reason: reason}
		if err := journalAppend(jdir, in, true); err != nil {
			return outDone, fmt.Errorf("journal: %w", err)
		}
		markPending(jdir, intentKeys(in)...)
		if err := fp("after_intent"); err != nil {
			return outDone, err
		}
		cp, d, werr := s.writeConflictCopy(ctx, uid, vaultID, p.Vault, p.Path, s.clientName, C, cSha, p.Ctime, p.Mtime)
		if werr == nil {
			werr = fp("after_write")
		}
		if werr != nil {
			if errors.Is(werr, errCrash) {
				return outDone, werr
			}
			s.broadcastRecovered(s.rollForwardDir(ctx, jdir))
			return outDone, werr
		}
		res.Ack.ConflictCopy = cp
		res.Copy = d
		_ = journalAppend(jdir, &jrec{T: "done", ID: in.ID, Rev: rev, Merged: true, Copy: cp, Fsha: finalSha, Reason: reason}, false)
		clearPending(intentKeys(in)...)
	default:
		// nothing to write (unchanged / merge result equals server body): remember the result
		if S != nil {
			if _, err := writeSnap(jdir, C); err != nil {
				return outDone, fmt.Errorf("snapshot: %w", err)
			}
			_ = journalAppend(jdir, &jrec{T: "op", K: key, Kind: "m", Op: p.OpID, Base: baseKey(p.BaseSha, p.BaseHash), Csha: cSha, Fsha: finalSha, Rev: rev, Merged: final != C, Reason: reason}, false)
		}
	}

	cc := res.Ack.ConflictCopy
	res.LastTime = rev
	res.Ack = newAck(p.OpID, rev, finalSha)
	res.Ack.ConflictCopy = cc
	if final != C {
		res.Ack.Merged, res.Ack.Content = true, final
	}
	emit(res)
	s.logOp("modify", reason, uid, vaultID, p.Path, p.OpID, rev, lockWait, t0)
	return outDone, nil
}

// writeConflictCopy stores C as a regular note next to the original; idempotent (same body at
// a candidate name is reused, a different body moves on to the next -N suffix).
func (s *noteService) writeConflictCopy(ctx context.Context, uid, vaultID int64, vault, path, client, content, cSha string, ctime, mtime int64) (string, *dto.NoteDTO, error) {
	for n := 1; n <= conflictCopyMax; n++ {
		cpath := conflictCopyPath(path, client, cSha, n)
		hash := util.EncodeHash32(cpath)
		ex, err := s.getAny(ctx, hash, vaultID, uid)
		if err != nil {
			return "", nil, err
		}
		if ex != nil && ex.Action != domain.NoteActionDelete {
			if ex.Content == content {
				return cpath, nil, nil // already there: no re-write, no re-broadcast
			}
			continue
		}
		now := time.Now().UnixMilli()
		cp := &dto.NoteModifyOrCreateRequest{
			Vault: vault, Path: cpath, PathHash: hash,
			Content: content, ContentHash: util.EncodeHash32(content),
			Ctime: now, Mtime: now,
		}
		var pre []*domain.Note
		if ex != nil {
			pre = append(pre, ex)
		}
		_, d, err := s.modifyOrCreateInner(ctx, uid, cp, false, pre...)
		if err != nil {
			return "", nil, err
		}
		return cpath, d, nil
	}
	return "", nil, code.ErrorNoteExist.WithDetails("too many conflict copies")
}

func (s *noteService) logWarn(what string, err error) {
	opLog().Warn("serverMerge: "+what+" failed", zap.Error(err))
}

// ---- delete ----

// ServerMergeDelete implements spec §7 (R1).
func (s *noteService) ServerMergeDelete(ctx context.Context, uid int64, p *dto.NoteDeleteRequest, emit func(*MergeResult)) error {
	vaultID, err := s.vaultService.MustGetID(ctx, uid, p.Vault)
	if err != nil {
		return err
	}
	t0 := time.Now()
	tl := time.Now()
	defer s.lockAndRecover(ctx, uid, vaultID, p.PathHash)()
	wait := time.Since(tl)

	S, err := s.getAny(ctx, p.PathHash, vaultID, uid)
	if err != nil {
		return err
	}
	res := &MergeResult{Path: p.Path, PathHash: p.PathHash}
	if S == nil {
		res.LastTime = dao.NextRev(0)
		res.Ack = newAck(p.OpID, res.LastTime, "")
		emit(res)
		s.logOp("delete", "replay", uid, vaultID, p.Path, p.OpID, res.LastTime, wait, t0)
		return nil
	}
	if S.Action == domain.NoteActionDelete {
		res.LastTime = S.UpdatedTimestamp
		res.Ack = newAck(p.OpID, S.UpdatedTimestamp, "")
		emit(res)
		s.logOp("delete", "replay", uid, vaultID, p.Path, p.OpID, res.LastTime, wait, t0)
		return nil
	}
	sSha := sha256Hex(S.Content)
	if sSha != p.BaseSha && S.Content != "" {
		res.LastTime = S.UpdatedTimestamp
		res.Ack = newAck(p.OpID, S.UpdatedTimestamp, sSha)
		res.Ack.Rejected, res.Ack.RejectReason, res.Ack.Merged, res.Ack.Content = true, "contentChanged", true, S.Content
		emit(res)
		s.logOp("delete", "rejected_contentChanged", uid, vaultID, p.Path, p.OpID, res.LastTime, wait, t0)
		return nil
	}
	jdir := snapDir(uid, S.ID)
	key := "d|" + p.BaseSha + "|" + p.PathHash
	if _, err := writeSnap(jdir, S.Content); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := fp("after_snapshot"); err != nil {
		return err
	}
	in := &jrec{T: "intent", ID: newJournalID(), K: key, Kind: "d", Op: p.OpID, UID: uid, VID: vaultID, Vault: p.Vault, Path: p.Path, PH: p.PathHash, Pre: sSha, PreRev: S.UpdatedTimestamp, Base: p.BaseSha, Reason: "fastforward"}
	if err := journalAppend(jdir, in, true); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	markPending(jdir, intentKeys(in)...)
	if err := fp("after_intent"); err != nil {
		return err
	}
	var d *dto.NoteDTO
	werr := fp("write")
	if werr == nil {
		d, werr = s.deleteInner(ctx, uid, p)
	}
	if werr != nil {
		if errors.Is(werr, errCrash) {
			return werr
		}
		s.broadcastRecovered(s.rollForwardDir(ctx, jdir))
		return werr
	}
	if err := fp("after_write"); err != nil {
		return err
	}
	_ = journalAppend(jdir, &jrec{T: "done", ID: in.ID, Rev: d.UpdatedTimestamp, Fsha: sSha}, false)
	clearPending(intentKeys(in)...)
	res.LastTime = d.UpdatedTimestamp
	res.Ack = newAck(p.OpID, d.UpdatedTimestamp, sSha)
	res.Deleted = d
	emit(res)
	s.logOp("delete", "fastforward", uid, vaultID, p.Path, p.OpID, res.LastTime, wait, t0)
	return nil
}

// ---- rename ----

// ServerMergeRename implements spec §8 (R1/R2): move only, no body merge.
func (s *noteService) ServerMergeRename(ctx context.Context, uid int64, p *dto.NoteRenameRequest, emit func(*MergeResult)) error {
	vaultID, err := s.vaultService.MustGetID(ctx, uid, p.Vault)
	if err != nil {
		return err
	}
	t0 := time.Now()
	newPath := strings.Trim(p.Path, "/")
	newHash := renameNewHash(p)
	oldPath := strings.Trim(p.OldPath, "/")
	oldHash := renameOldHash(p)
	tl := time.Now()
	defer s.lockAndRecover(ctx, uid, vaultID, newHash, oldHash)()
	wait := time.Since(tl)
	res := &MergeResult{Path: newPath, PathHash: newHash}
	key := "r|" + p.BaseSha + "|" + newPath

	Src, err := s.getAny(ctx, oldHash, vaultID, uid)
	if err != nil {
		return err
	}
	T, err := s.getAny(ctx, newHash, vaultID, uid)
	if err != nil {
		return err
	}
	srcLive := Src != nil && Src.Action != domain.NoteActionDelete
	tLive := T != nil && T.Action != domain.NoteActionDelete

	// replay: the rename already happened (journal lives with the moved snap dir, or in the
	// source dir for the identical-target variant)
	if !srcLive && tLive {
		hit := false
		if journalRead(snapDir(uid, T.ID)).done[key] != nil {
			hit = true
		}
		if !hit && Src != nil && journalRead(snapDir(uid, Src.ID)).done[key] != nil {
			hit = true
		}
		if hit {
			tSha := sha256Hex(T.Content)
			res.LastTime = T.UpdatedTimestamp
			res.Ack = newAck(p.OpID, T.UpdatedTimestamp, tSha)
			if p.BaseSha != "" && p.BaseSha != tSha {
				res.Ack.Merged, res.Ack.Content = true, T.Content
			}
			emit(res)
			s.logOp("rename", "replay", uid, vaultID, newPath, p.OpID, res.LastTime, wait, t0)
			return nil
		}
	}
	if !srcLive {
		res.LastTime = dao.NextRev(0)
		res.Ack = newAck(p.OpID, res.LastTime, "")
		res.Ack.Rejected, res.Ack.RejectReason = true, "sourceMissing"
		emit(res)
		s.logOp("rename", "rejected_sourceMissing", uid, vaultID, newPath, p.OpID, res.LastTime, wait, t0)
		return nil
	}
	sd := snapDir(uid, Src.ID)
	sSha := sha256Hex(Src.Content)

	if tLive {
		tSha := sha256Hex(T.Content)
		if T.Content == Src.Content {
			// identical content already at target: just drop the source (journaled as a delete)
			if _, err := writeSnap(sd, Src.Content); err != nil {
				return fmt.Errorf("snapshot: %w", err)
			}
			in := &jrec{T: "intent", ID: newJournalID(), K: key, Kind: "d", Op: p.OpID, UID: uid, VID: vaultID, Vault: p.Vault, Path: oldPath, PH: oldHash, Pre: sSha, PreRev: Src.UpdatedTimestamp, Base: p.BaseSha, Reason: "fastforward"}
			if err := journalAppend(sd, in, true); err != nil {
				return fmt.Errorf("journal: %w", err)
			}
			markPending(sd, intentKeys(in)...)
			d, err := s.deleteInner(ctx, uid, &dto.NoteDeleteRequest{Vault: p.Vault, Path: oldPath, PathHash: oldHash})
			if err != nil {
				s.broadcastRecovered(s.rollForwardDir(ctx, sd))
				return err
			}
			_ = journalAppend(sd, &jrec{T: "done", ID: in.ID, Rev: d.UpdatedTimestamp, Fsha: sSha}, false)
			clearPending(intentKeys(in)...)
			res.LastTime = T.UpdatedTimestamp
			res.Ack = newAck(p.OpID, T.UpdatedTimestamp, tSha)
			res.Deleted = d
			emit(res)
			s.logOp("rename", "fastforward", uid, vaultID, newPath, p.OpID, res.LastTime, wait, t0)
			return nil
		}
		res.LastTime = T.UpdatedTimestamp
		res.Ack = newAck(p.OpID, T.UpdatedTimestamp, tSha)
		res.Ack.Rejected, res.Ack.RejectReason, res.Ack.Merged, res.Ack.Content = true, "targetExists", true, T.Content
		emit(res)
		s.logOp("rename", "rejected_targetExists", uid, vaultID, newPath, p.OpID, res.LastTime, wait, t0)
		return nil
	}

	// 1. snapshot  2. intent  3+4. rename  5. move snap dir + done
	if _, err := writeSnap(sd, Src.Content); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := fp("after_snapshot"); err != nil {
		return err
	}
	in := &jrec{T: "intent", ID: newJournalID(), K: key, Kind: "r", Op: p.OpID, UID: uid, VID: vaultID, Vault: p.Vault,
		Path: oldPath, PH: oldHash, Path2: newPath, PH2: newHash, Pre: sSha, PreRev: Src.UpdatedTimestamp, Base: p.BaseSha, Reason: "fastforward"}
	if err := journalAppend(sd, in, true); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	markPending(sd, intentKeys(in)...)
	if err := fp("after_intent"); err != nil {
		return err
	}
	var oldDTO, newDTO *dto.NoteDTO
	werr := fp("write")
	if werr == nil {
		oldDTO, newDTO, werr = s.renameInner(ctx, uid, p)
	}
	if werr != nil {
		if errors.Is(werr, errCrash) {
			return werr
		}
		s.broadcastRecovered(s.rollForwardDir(ctx, sd))
		return werr
	}
	if err := fp("after_write"); err != nil {
		return err
	}
	nd := snapDir(uid, newDTO.ID)
	if err := moveSnapDir(sd, nd); err != nil {
		if errors.Is(err, errCrash) {
			return err
		}
		s.logWarn("move snap dir", err)
		nd = sd
	}
	_ = journalAppend(nd, &jrec{T: "done", ID: in.ID, Rev: newDTO.UpdatedTimestamp, Fsha: sSha}, false)
	clearPending(intentKeys(in)...)
	merged := p.BaseSha != "" && p.BaseSha != sSha
	res.LastTime = newDTO.UpdatedTimestamp
	res.Ack = newAck(p.OpID, newDTO.UpdatedTimestamp, sSha)
	if merged {
		res.Ack.Merged, res.Ack.Content = true, Src.Content
	}
	res.RenamedOld, res.RenamedNew = oldDTO, newDTO
	emit(res)
	s.logOp("rename", "fastforward", uid, vaultID, newPath, p.OpID, res.LastTime, wait, t0)
	return nil
}

// ---- roll-forward ----

// rollForwardDir completes (or aborts) every unfinished intent in the journal at dir and
// returns the committed effects so the caller can broadcast them in commit order. A done line
// that cannot be written and read back leaves the intent pending (retried on the next lock).
func (s *noteService) rollForwardDir(ctx context.Context, dir string) []*MergeResult {
	st := journalRead(dir)
	var evs []*MergeResult
	for _, in := range st.pending {
		doneDir, done, ev := s.rollForwardIntent(ctx, dir, in)
		if done.Abort {
			done.Reason = firstNonEmpty(done.Reason, "rollforward_abort")
		} else {
			done.Reason = "rollforward"
		}
		done.T, done.ID = "done", in.ID
		if err := journalAppend(doneDir, &done, true); err != nil {
			s.logWarn("roll-forward done line", err)
			continue
		}
		unread := false
		for _, pi := range journalRead(doneDir).pending {
			if pi.ID == in.ID {
				unread = true
			}
		}
		if unread {
			s.logWarn("roll-forward done line unreadable", errors.New(in.ID))
			continue
		}
		clearPending(intentKeys(in)...)
		stats.inc("rollforward")
		opLog().Warn("serverMerge: rolled forward unfinished op", zap.String("kind", in.Kind), zap.String("path", in.Path), zap.String("op", in.Op), zap.Bool("abort", done.Abort), zap.String("reason", done.Reason))
		if ev != nil {
			ev.UID, ev.Vault = in.UID, in.Vault
			evs = append(evs, ev)
		}
	}
	return evs
}

// rollForwardIntent returns the dir the done line must go to, the result fields, and the
// effect to broadcast (nil when nothing changed). Redo is allowed only while the row is still
// in the state the intent was based on (rev + existence + sha); a later write wins.
func (s *noteService) rollForwardIntent(ctx context.Context, dir string, in *jrec) (string, jrec, *MergeResult) {
	abort := func(reason string) (string, jrec, *MergeResult) { return dir, jrec{Abort: true, Reason: reason}, nil }
	switch in.Kind {
	case "m":
		fin, ok := loadSnap(dir, in.Fsha)
		if !ok {
			return abort("rollforward_nosnap")
		}
		S, gerr := s.getAny(ctx, in.PH, in.VID, in.UID)
		if gerr != nil { // row exists but its body is unreadable (crash between DB row and content file)
			metas, _ := s.noteRepo.ListByPathHashesMeta(ctx, []string{in.PH}, in.VID, in.UID)
			if m := metas[in.PH]; m != nil && m.UpdatedTimestamp == in.PreRev {
				m.Content, m.ContentHash = fin, util.EncodeHash32(fin)
				if _, err := s.noteRepo.Update(ctx, m, in.UID); err == nil {
					d, r := s.finishModifyRoll(dir, in, m.ID)
					return d, r, &MergeResult{Path: in.Path, PathHash: in.PH, Written: s.domainToDTO(m)}
				}
			}
			return abort("rollforward_unreadable")
		}
		exists := S != nil && S.Action != domain.NoteActionDelete
		var sNow string
		if exists {
			sNow = sha256Hex(S.Content)
		}
		if exists && sNow == in.Fsha && S.ContentHash == util.EncodeHash32(fin) {
			d, r := s.finishModifyRoll(dir, in, S.ID)
			r.Rev = S.UpdatedTimestamp
			return d, r, &MergeResult{Path: in.Path, PathHash: in.PH, LastTime: S.UpdatedTimestamp, Written: s.domainToDTO(S)}
		}
		revOK := (S == nil && in.PreRev == 0) || (S != nil && S.UpdatedTimestamp == in.PreRev)
		redo := revOK && ((!exists && in.Pre == "") || (exists && (sNow == in.Pre || sNow == in.Fsha)))
		if !redo {
			return abort("rollforward_superseded")
		}
		wp := &dto.NoteModifyOrCreateRequest{Vault: in.Vault, Path: in.Path, PathHash: in.PH, Content: fin, ContentHash: util.EncodeHash32(fin), Ctime: in.Ctime, Mtime: in.Mtime}
		var pre []*domain.Note
		if S != nil {
			pre = append(pre, S)
		}
		_, d, err := s.modifyOrCreateInner(ctx, in.UID, wp, false, pre...)
		if err != nil {
			return abort("rollforward_failed")
		}
		doneDir, r := s.finishModifyRoll(dir, in, d.ID)
		r.Rev = d.UpdatedTimestamp
		return doneDir, r, &MergeResult{Path: in.Path, PathHash: in.PH, LastTime: d.UpdatedTimestamp, Written: d}
	case "c":
		c, ok := loadSnap(dir, in.Csha)
		if !ok {
			return abort("rollforward_nosnap")
		}
		cp, d, err := s.writeConflictCopy(ctx, in.UID, in.VID, in.Vault, in.Path, in.Cl, c, in.Csha, in.Ctime, in.Mtime)
		if err != nil {
			return abort("rollforward_failed")
		}
		if d == nil {
			// The first attempt may have committed the copy before failing to
			// emit it. Recovery must publish the reused copy as well.
			n, err := s.getAny(ctx, util.EncodeHash32(cp), in.VID, in.UID)
			if err != nil || n == nil || n.Action == domain.NoteActionDelete || sha256Hex(n.Content) != in.Csha {
				return abort("rollforward_unreadable")
			}
			d = s.domainToDTO(n)
		}
		ev := &MergeResult{Path: cp, PathHash: d.PathHash, LastTime: d.UpdatedTimestamp, Copy: d}
		return dir, jrec{Merged: true, Copy: cp, Fsha: in.Fsha}, ev
	case "d":
		S, err := s.getAny(ctx, in.PH, in.VID, in.UID)
		if err != nil {
			return abort("rollforward_unreadable")
		}
		if S != nil && S.Action != domain.NoteActionDelete && sha256Hex(S.Content) == in.Pre && S.UpdatedTimestamp == in.PreRev {
			d, err := s.deleteInner(ctx, in.UID, &dto.NoteDeleteRequest{Vault: in.Vault, Path: in.Path, PathHash: in.PH})
			if err != nil {
				return abort("rollforward_failed")
			}
			return dir, jrec{Rev: d.UpdatedTimestamp, Fsha: in.Pre}, &MergeResult{Path: in.Path, PathHash: in.PH, LastTime: d.UpdatedTimestamp, Deleted: d}
		}
		if S != nil && S.Action == domain.NoteActionDelete {
			return dir, jrec{Rev: S.UpdatedTimestamp, Fsha: in.Pre}, &MergeResult{Path: in.Path, PathHash: in.PH, LastTime: S.UpdatedTimestamp, Deleted: s.domainToDTO(S)}
		}
		return abort("rollforward_superseded") // live and changed since: a later write wins
	case "r":
		Src, e1 := s.getAny(ctx, in.PH, in.VID, in.UID)
		T, e2 := s.getAny(ctx, in.PH2, in.VID, in.UID)
		if e1 != nil || e2 != nil {
			return abort("rollforward_unreadable")
		}
		srcLive := Src != nil && Src.Action != domain.NoteActionDelete
		tLive := T != nil && T.Action != domain.NoteActionDelete
		var target *dto.NoteDTO
		switch {
		case tLive && !srcLive && sha256Hex(T.Content) == in.Pre:
			// already renamed; only the snapshot move may be missing
		case srcLive && !tLive && sha256Hex(Src.Content) == in.Pre && Src.UpdatedTimestamp == in.PreRev:
			rp := &dto.NoteRenameRequest{Vault: in.Vault, Path: in.Path2, PathHash: in.PH2, OldPath: in.Path, OldPathHash: in.PH}
			_, nd, err := s.renameInner(ctx, in.UID, rp)
			if err != nil {
				return abort("rollforward_failed")
			}
			target = nd
		case Src != nil && !srcLive && !tLive && sha256Hex(Src.Content) == in.Pre:
			// old side already marked deleted, new side never created: finish it
			wp := &dto.NoteModifyOrCreateRequest{Vault: in.Vault, Path: in.Path2, PathHash: in.PH2, Content: Src.Content, ContentHash: util.EncodeHash32(Src.Content), Ctime: Src.Ctime, Mtime: Src.Mtime}
			_, nd, err := s.modifyOrCreateInner(ctx, in.UID, wp, false, T)
			if err != nil {
				return abort("rollforward_failed")
			}
			target = nd
		default:
			return abort("rollforward_superseded")
		}
		if target == nil && T != nil {
			target = s.domainToDTO(T)
		}
		nd := dir
		if target != nil {
			nd = snapDir(in.UID, target.ID)
			if nd != dir {
				if err := moveSnapDir(dir, nd); err != nil {
					nd = dir
				}
			}
		}
		rev := int64(0)
		var ev *MergeResult
		if target != nil {
			rev = target.UpdatedTimestamp
			ev = &MergeResult{Path: in.Path2, PathHash: in.PH2, LastTime: rev, RenamedNew: target}
			if Src != nil {
				ev.RenamedOld = s.domainToDTO(Src)
			}
			if ev.RenamedOld == nil {
				ev = nil
			}
		}
		return nd, jrec{Rev: rev, Fsha: in.Pre}, ev
	}
	return abort("rollforward_unknown")
}

// finishModifyRoll adopts a staging dir (note created by the roll-forward) and returns the
// dir the done line belongs to.
func (s *noteService) finishModifyRoll(dir string, in *jrec, noteID int64) (string, jrec) {
	doneDir := dir
	if strings.Contains(dir, string(filepath.Separator)+"_new"+string(filepath.Separator)) {
		nd := snapDir(in.UID, noteID)
		if err := moveSnapDir(dir, nd); err == nil {
			doneDir = nd
		}
	}
	return doneDir, jrec{Fsha: in.Fsha, Merged: in.Fsha != in.Csha}
}
