package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func NoteDigest(s string) string  { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
func NoteRevision(n int64) string { return strconv.FormatInt(n, 10) }

func ReliableConflictPath(p *dto.NoteModifyOrCreateRequest) string {
	ext := filepath.Ext(p.Path)
	return strings.TrimSuffix(p.Path, ext) + ".conflict." + NoteDigest(p.RequestID)[:24] + ext
}

// ReliableNoteResult is a durable receipt for one immutable upload, not for a path's latest edit.
type ReliableNoteResult struct {
	Replay        bool   `json:"-"`
	MergeProtocol int    `json:"mergeProtocol"`
	RequestID     string `json:"requestId"`
	Path          string `json:"path"`
	PathHash      string `json:"pathHash"`
	Content       string `json:"content"`
	ContentHash   string `json:"contentHash"`
	ContentDigest string `json:"contentDigest"`
	Revision      string `json:"revision"`
	LastTime      int64  `json:"lastTime"`
	Mtime         int64  `json:"mtime"`
	Ctime         int64  `json:"ctime"`
	ConflictPath  string `json:"conflictPath,omitempty"`
	Deleted       bool   `json:"deleted,omitempty"`
}

type reliableBefore struct {
	Content          string `json:"content"`
	Action           string `json:"action"`
	UpdatedTimestamp int64  `json:"lastTime"`
}

type reliableIntent struct {
	Fingerprint string              `json:"fingerprint"`
	Before      *reliableBefore     `json:"before"`
	Desired     string              `json:"desired"`
	Conflict    bool                `json:"conflict"`
	Result      *ReliableNoteResult `json:"result,omitempty"`
}

func atomicMergeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func currentReliableNote(ctx context.Context, svc NoteService, uid int64, p *dto.NoteModifyOrCreateRequest) (*dto.NoteDTO, error) {
	n, err := svc.Get(ctx, uid, &dto.NoteGetRequest{Vault: p.Vault, Path: p.Path, PathHash: p.PathHash})
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, code.ErrorNoteNotFound) {
		return nil, err
	}
	n, err = svc.Get(ctx, uid, &dto.NoteGetRequest{Vault: p.Vault, Path: p.Path, PathHash: p.PathHash, IsRecycle: true})
	if errors.Is(err, code.ErrorNoteNotFound) {
		return nil, nil
	}
	return n, err
}

// ApplyReliableNote serializes the read/merge/write/receipt sequence. An fsynced intent preserves
// the complete incoming manuscript before touching the canonical note. A retry after a crash
// never overwrites an intervening writer: it saves the intended manuscript as a conflict instead.
func ApplyReliableNote(ctx context.Context, svc NoteService, uid int64, p *dto.NoteModifyOrCreateRequest) (*ReliableNoteResult, *dto.NoteDTO, error) {
	if p.MergeProtocol != 1 || len(p.RequestID) < 8 || len(p.RequestID) > 160 {
		return nil, nil, fmt.Errorf("invalid merge protocol or requestId")
	}
	if p.PathHash != util.EncodeHash32(p.Path) {
		return nil, nil, fmt.Errorf("pathHash mismatch")
	}
	ctx, unlock := LockNote(ctx, uid, p.Vault, p.PathHash, util.EncodeHash32(ReliableConflictPath(p)))
	defer unlock()
	// Do not allow a 32-bit path collision to overwrite another actual path.
	current, err := currentReliableNote(ctx, svc, uid, p)
	if err != nil {
		return nil, nil, err
	}
	if current != nil && current.Path != p.Path {
		return nil, nil, fmt.Errorf("path hash collision")
	}
	receiptPath := filepath.Join("storage", "merge-receipts", strconv.FormatInt(uid, 10), NoteDigest(p.Vault), NoteDigest(p.Path), NoteDigest(p.RequestID)+".json")
	// Context/mtime may legitimately change on reconnect; manuscript and ancestor may not.
	fingerprintPayload := fmt.Sprintf("%t\x00%s\x00%s\x00%s", p.HasBase, p.BaseContent, p.Content, p.Path)
	// Omitted recreation fields keep the original protocol-1 identity, including
	// already pending requests whose receipts predate explicit recreation.
	if p.Recreate || p.BaseRevision != "" {
		fingerprintPayload += fmt.Sprintf("\x00%t\x00%s", p.Recreate, p.BaseRevision)
	}
	fp := NoteDigest(fingerprintPayload)
	intent := &reliableIntent{}
	data, readErr := os.ReadFile(receiptPath)
	if readErr == nil {
		if err = json.Unmarshal(data, intent); err != nil {
			return nil, nil, err
		}
		if intent.Fingerprint != fp {
			return nil, nil, fmt.Errorf("requestId reused for another manuscript")
		}
		if intent.Result != nil {
			intent.Result.Replay = true
			return intent.Result, nil, nil
		}
	} else if !os.IsNotExist(readErr) {
		return nil, nil, readErr
	} else {
		intent.Fingerprint = fp
		if current != nil {
			intent.Before = &reliableBefore{Content: current.Content, Action: current.Action, UpdatedTimestamp: current.UpdatedTimestamp}
		}
		intent.Desired = p.Content
		switch {
		case current == nil:
			// A known base with no corresponding path is a stale rename/delete upload. Preserve it.
			intent.Conflict = p.HasBase
		case current.Action == "delete":
			// Recreating a path is a new user action against a confirmed tombstone,
			// never a late upload based on a manuscript that used to exist.
			intent.Conflict = p.HasBase || !p.Recreate || p.BaseRevision != NoteRevision(current.UpdatedTimestamp)
		case current.Content == p.Content && (!p.HasBase || p.BaseContent == current.Content):
			intent.Desired = current.Content
		case p.HasBase:
			merged, ok := MergeReliableTexts(p.BaseContent, current.Content, p.Content)
			intent.Conflict = !ok
			if ok {
				intent.Desired = merged
			}
		default:
			intent.Conflict = true
		}
		if err = atomicMergeJSON(receiptPath, intent); err != nil {
			return nil, nil, err
		}
	}
	// An interrupted intent may only be completed on the exact original state or its own result.
	sameBefore := current == nil && intent.Before == nil
	if current != nil && intent.Before != nil {
		sameBefore = current.Content == intent.Before.Content && current.Action == intent.Before.Action && current.UpdatedTimestamp == intent.Before.UpdatedTimestamp
	}
	ownResult := current != nil && current.Action != "delete" && current.Content == intent.Desired
	if !sameBefore && !ownResult {
		intent.Conflict = true
	}
	var conflictNote *dto.NoteDTO
	if intent.Conflict {
		conflictPath := ReliableConflictPath(p)
		cp := *p
		cp.Path = conflictPath
		cp.PathHash = util.EncodeHash32(conflictPath)
		cp.Content = p.Content
		cp.ContentHash = util.EncodeHash32(p.Content)
		cp.CreateOnly = true
		existing, e := currentReliableNote(ctx, svc, uid, &cp)
		if e != nil {
			return nil, nil, e
		}
		if existing != nil {
			if existing.Path != cp.Path || existing.Content != cp.Content || existing.Action == "delete" {
				return nil, nil, fmt.Errorf("conflict copy name occupied")
			}
			conflictNote = existing
		} else {
			_, conflictNote, err = svc.ModifyOrCreate(ctx, uid, &cp, false)
			if err != nil {
				return nil, nil, err
			}
		}
	} else if !ownResult {
		mp := *p
		mp.Content = intent.Desired
		mp.ContentHash = util.EncodeHash32(mp.Content)
		_, current, err = svc.ModifyOrCreate(ctx, uid, &mp, false)
		if err != nil {
			return nil, nil, err
		}
	}

	// A prior process may have committed SQLite metadata and crashed before projecting its
	// body file. Preserve this request first (above), then repair metadata/body from the
	// actually readable manuscript. If the projection vanished, the intent's original
	// manuscript is the recoverable fallback, never an invented empty ancestor.
	if current != nil && current.Action != "delete" && (current.ContentHash != util.EncodeHash32(current.Content) || current.Size != int64(len(current.Content))) {
		repairContent := current.Content
		if repairContent == "" && current.Size > 0 && intent.Before != nil {
			repairContent = intent.Before.Content
		}
		repair := *p
		repair.Content = repairContent
		repair.ContentHash = util.EncodeHash32(repairContent)
		_, current, err = svc.ModifyOrCreate(ctx, uid, &repair, false)
		if err != nil {
			return nil, nil, err
		}
	}
	// Read back the persisted body, not the in-memory result returned by the write.
	if current != nil && current.Action != "delete" {
		verified, verifyErr := currentReliableNote(ctx, svc, uid, p)
		if verifyErr != nil {
			return nil, nil, verifyErr
		}
		if verified == nil || verified.Content != current.Content || verified.ContentHash != util.EncodeHash32(verified.Content) || verified.Size != int64(len(verified.Content)) {
			return nil, nil, fmt.Errorf("canonical body/metadata verification failed")
		}
		current = verified
	}
	result := &ReliableNoteResult{MergeProtocol: 1, RequestID: p.RequestID, Path: p.Path, PathHash: p.PathHash}
	if current != nil {
		result.Content = current.Content
		result.ContentHash = util.EncodeHash32(current.Content)
		result.ContentDigest = NoteDigest(current.Content)
		result.Revision = NoteRevision(current.UpdatedTimestamp)
		result.LastTime = current.UpdatedTimestamp
		result.Ctime = current.Ctime
		result.Mtime = current.Mtime
		result.Deleted = current.Action == "delete"
	} else {
		result.Deleted = true
		result.Revision = "0"
		result.ContentDigest = NoteDigest("")
	}
	if conflictNote != nil {
		cp := *p
		cp.Path = conflictNote.Path
		cp.PathHash = util.EncodeHash32(cp.Path)
		verified, verifyErr := currentReliableNote(ctx, svc, uid, &cp)
		if verifyErr != nil {
			return nil, nil, verifyErr
		}
		if verified == nil || verified.Action == "delete" || verified.Content != p.Content || verified.ContentHash != util.EncodeHash32(verified.Content) || verified.Size != int64(len(verified.Content)) {
			return nil, nil, fmt.Errorf("conflict body/metadata verification failed")
		}
		conflictNote = verified
		result.ConflictPath = conflictNote.Path
	}
	intent.Result = result
	if err = atomicMergeJSON(receiptPath, intent); err != nil {
		return nil, nil, err
	}
	return result, conflictNote, nil
}

type mergeHunk struct {
	start, end int
	text       []string
}

func lineHunks(base, other string) []mergeHunk {
	dmp := diffmatchpatch.New()
	a, b, table := dmp.DiffLinesToChars(base, other)
	diffs := dmp.DiffCharsToLines(dmp.DiffMain(a, b, false), table)
	var hunks []mergeHunk
	pos := 0
	var cur *mergeHunk
	flush := func() {
		if cur != nil {
			hunks = append(hunks, *cur)
			cur = nil
		}
	}
	for _, d := range diffs {
		lines := strings.SplitAfter(d.Text, "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
		switch d.Type {
		case diffmatchpatch.DiffEqual:
			flush()
			pos += len(lines)
		case diffmatchpatch.DiffDelete:
			if cur == nil {
				cur = &mergeHunk{start: pos, end: pos}
			}
			pos += len(lines)
			cur.end = pos
		case diffmatchpatch.DiffInsert:
			if cur == nil {
				cur = &mergeHunk{start: pos, end: pos}
			}
			cur.text = append(cur.text, lines...)
		}
	}
	flush()
	return hunks
}

// MergeReliableTexts applies only non-overlapping line edits to the exact ancestor. It never
// fuzzy-applies patches or forces conflicting text into the canonical note.
func MergeReliableTexts(base, current, incoming string) (string, bool) {

	if current == base {
		return incoming, true
	}
	if incoming == base {
		return current, true
	}
	if len(base)+len(current)+len(incoming) > 6*1024*1024 || strings.Count(base, "\n") > 20000 {
		return "", false
	}
	left, right := lineHunks(base, current), lineHunks(base, incoming)
	all := append([]mergeHunk{}, left...)
	for _, r := range right {
		duplicate := false
		for _, l := range left {
			if l.start == r.start && l.end == r.end && l.start != l.end && strings.Join(l.text, "") == strings.Join(r.text, "") {
				duplicate = true
				break
			}
			// Concurrent insertions at the same position have an explicit deterministic order.
			if l.start == l.end && r.start == r.end && l.start == r.start {
				continue
			}
			overlap := l.start < r.end && r.start < l.end
			if l.start == l.end {
				overlap = l.start > r.start && l.start < r.end
			}
			if r.start == r.end {
				overlap = r.start > l.start && r.start < l.end
			}
			if overlap {
				return "", false
			}
		}
		if !duplicate {
			all = append(all, r)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].start == all[j].start {
			return all[i].end < all[j].end
		}
		return all[i].start < all[j].start
	})
	lines := strings.SplitAfter(base, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	var out strings.Builder
	pos := 0
	for _, h := range all {
		if h.start < pos {
			return "", false
		}
		for _, l := range lines[pos:h.start] {
			out.WriteString(l)
		}
		for _, l := range h.text {
			out.WriteString(l)
		}
		pos = h.end
	}
	for _, l := range lines[pos:] {
		out.WriteString(l)
	}
	return out.String(), true
}
