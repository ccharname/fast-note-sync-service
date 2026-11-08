package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

func TestReliableThreeWayMerge(t *testing.T) {
	cases := []struct {
		name, base, current, incoming, want string
		ok                                  bool
	}{
		{"different lines", "one\ntwo\n", "ONE\ntwo\n", "one\nTWO\n", "ONE\nTWO\n", true},
		{"same line conflict", "one\n", "ONE\n", "OTHER\n", "", false},
		{"delete versus change", "one\ntwo\n", "two\n", "ONE\ntwo\n", "", false},
		{"empty ancestor", "", "A\n", "B\n", "A\nB\n", true},
		{"concurrent append", "base\n", "base\nA\n", "base\nB\n", "base\nA\nB\n", true},
		{"independent identical appends", "base\n", "base\nrecord\n", "base\nrecord\n", "base\nrecord\nrecord\n", true},
		{"same result", "base\n", "changed\n", "changed\n", "changed\n", true},
		{"empty incoming", "base\n", "base\n", "", "", true},
		{"newline preservation", "a\nb", "A\nb", "a\nB", "A\nB", true},
		{"disjoint identical insertions", "a\nb\n", "x\na\nb\n", "a\nb\nx\n", "x\na\nb\nx\n", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := MergeReliableTexts(c.base, c.current, c.incoming)
			if ok != c.ok || got != c.want {
				t.Fatalf("got %q,%v want %q,%v", got, ok, c.want, c.ok)
			}
		})
	}
}

type reliableFake struct {
	NoteService
	notes map[string]*dto.NoteDTO
	fail  bool
	tick  int64
}

func (f *reliableFake) Get(_ context.Context, _ int64, p *dto.NoteGetRequest) (*dto.NoteDTO, error) {
	n := f.notes[p.Path]
	if n == nil {
		return nil, code.ErrorNoteNotFound
	}
	cp := *n
	return &cp, nil
}
func (f *reliableFake) ModifyOrCreate(_ context.Context, _ int64, p *dto.NoteModifyOrCreateRequest, _ bool, _ ...*domain.Note) (bool, *dto.NoteDTO, error) {
	if f.fail {
		return false, nil, errors.New("disk failure")
	}
	f.tick++
	n := &dto.NoteDTO{Path: p.Path, PathHash: p.PathHash, Content: p.Content, Size: int64(len(p.Content)), ContentHash: util.EncodeHash32(p.Content), Action: "modify", UpdatedTimestamp: f.tick, Ctime: p.Ctime, Mtime: p.Mtime}
	f.notes[p.Path] = n
	cp := *n
	return false, &cp, nil
}
func reliableTestEnv(t *testing.T) *reliableFake {
	t.Helper()
	old, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chdir(t.TempDir()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	return &reliableFake{notes: map[string]*dto.NoteDTO{}, tick: 100}
}
func reliableReq(id, base, content string, hasBase bool) *dto.NoteModifyOrCreateRequest {
	return &dto.NoteModifyOrCreateRequest{MergeProtocol: 1, RequestID: id, Vault: "v", Path: "note.md", PathHash: util.EncodeHash32("note.md"), HasBase: hasBase, BaseContent: base, Content: content, ContentHash: util.EncodeHash32(content), Ctime: 1, Mtime: 2}
}
func applyTest(t *testing.T, f *reliableFake, p *dto.NoteModifyOrCreateRequest) *ReliableNoteResult {
	t.Helper()
	r, _, e := ApplyReliableNote(context.Background(), f, 1, p)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestReliableReceiptSurvivesRetryAndInterveningWrite(t *testing.T) {
	f := reliableTestEnv(t)
	first := reliableReq("request-one", "", "Aa", false)
	r := applyTest(t, f, first)
	if r.Content != "Aa" {
		t.Fatal(r)
	}
	applyTest(t, f, reliableReq("request-two", "Aa", "BB", true))
	retry := applyTest(t, f, first)
	if !retry.Replay || retry.Content != "Aa" || f.notes["note.md"].Content != "BB" {
		t.Fatal("retry overwrote newer content", retry)
	}
	changed := *first
	changed.Content = "OTHER"
	if _, _, e := ApplyReliableNote(context.Background(), f, 1, &changed); e == nil {
		t.Fatal("reused request accepted")
	}
}
func TestReliableHashCollisionAndEmptyContent(t *testing.T) {
	f := reliableTestEnv(t)
	applyTest(t, f, reliableReq("request-one", "", "Aa", false))
	r := applyTest(t, f, reliableReq("request-two", "Aa", "BB", true))
	if r.Content != "BB" || r.ContentDigest == NoteDigest("Aa") {
		t.Fatal("32 bit collision dropped edit", r)
	}
	r = applyTest(t, f, reliableReq("request-empty", "BB", "", true))
	if r.Content != "" || r.Deleted {
		t.Fatal("empty body treated as deletion")
	}
}
func TestReliableConflictCopyIsDurableAndIdempotent(t *testing.T) {
	f := reliableTestEnv(t)
	applyTest(t, f, reliableReq("request-one", "", "original", false))
	p := reliableReq("request-two", "ancestor", "draft", true)
	r := applyTest(t, f, p)
	if r.Content != "original" || r.ConflictPath == "" || f.notes[r.ConflictPath].Content != "draft" {
		t.Fatal(r)
	}
	applyTest(t, f, p)
	if len(f.notes) != 2 {
		t.Fatal("duplicate conflict copy")
	}
}
func TestReliableInterruptedIntentPreservesDraft(t *testing.T) {
	f := reliableTestEnv(t)
	applyTest(t, f, reliableReq("request-one", "", "original", false))
	p := reliableReq("request-two", "original", "draft", true)
	f.fail = true
	if _, _, e := ApplyReliableNote(context.Background(), f, 1, p); e == nil {
		t.Fatal("write failure acknowledged")
	}
	files, _ := filepath.Glob("storage/merge-receipts/*/*/*/*.json")
	if len(files) != 2 {
		t.Fatal("draft intent not persisted")
	}
	f.fail = false
	// Another writer commits before the interrupted upload retries.
	f.notes["note.md"].Content = "other writer"
	f.notes["note.md"].UpdatedTimestamp++
	r := applyTest(t, f, p)
	if r.Content != "other writer" || r.ConflictPath == "" || f.notes[r.ConflictPath].Content != "draft" {
		t.Fatal("recovery overwrote intervening writer", r)
	}
}
func TestReliableUnknownBaseAndDeletedPathPreserveCopies(t *testing.T) {
	f := reliableTestEnv(t)
	applyTest(t, f, reliableReq("request-one", "", "original", false))
	r := applyTest(t, f, reliableReq("request-two", "", "unknown base draft", false))
	if r.ConflictPath == "" {
		t.Fatal(r)
	}
	f.notes["note.md"].Action = "delete"
	r = applyTest(t, f, reliableReq("request-three", "original", "offline draft", true))
	if !r.Deleted || r.ConflictPath == "" {
		t.Fatal(r)
	}
}
func TestReliableConcurrentDisjointWrites(t *testing.T) {
	f := reliableTestEnv(t)
	base := "one\ntwo\n"
	applyTest(t, f, reliableReq("request-initial", "", base, false))
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, body := range []string{"ONE\ntwo\n", "one\nTWO\n"} {
		wg.Add(1)
		go func(i int, body string) {
			defer wg.Done()
			_, _, e := ApplyReliableNote(context.Background(), f, 1, reliableReq([]string{"request-left", "request-right"}[i], base, body, true))
			errs <- e
		}(i, body)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if f.notes["note.md"].Content != "ONE\nTWO\n" {
		t.Fatal(f.notes["note.md"].Content)
	}
}
func TestReliableLargeConflictBounds(t *testing.T) {
	base := strings.Repeat("a\n", 21000)
	if _, ok := MergeReliableTexts(base, "x"+base, "y"+base); ok {
		t.Fatal("oversized merge accepted")
	}
}

func TestReliableExplicitRecreateRequiresConfirmedTombstone(t *testing.T) {
	f := reliableTestEnv(t)
	applyTest(t, f, reliableReq("recreate-initial", "", "old", false))
	f.notes["note.md"].Action = "delete"
	f.notes["note.md"].UpdatedTimestamp++
	tombstone := NoteRevision(f.notes["note.md"].UpdatedTimestamp)
	stale := reliableReq("recreate-stale", "", "unconfirmed new", false)
	stale.Recreate = true
	stale.BaseRevision = "0"
	if r := applyTest(t, f, stale); !r.Deleted || r.ConflictPath == "" {
		t.Fatal("stale confirmation resurrected path", r)
	}
	late := reliableReq("recreate-old-branch", "old", "late old edit", true)
	late.Recreate = true
	late.BaseRevision = tombstone
	if r := applyTest(t, f, late); !r.Deleted || r.ConflictPath == "" {
		t.Fatal("old manuscript resurrected path", r)
	}
	fresh := reliableReq("recreate-explicit", "", "intentional new", false)
	fresh.Recreate = true
	fresh.BaseRevision = tombstone
	r := applyTest(t, f, fresh)
	if r.Deleted || r.ConflictPath != "" || r.Content != "intentional new" {
		t.Fatal("confirmed recreation rejected", r)
	}
	retry := applyTest(t, f, fresh)
	if !retry.Replay || retry.Revision != r.Revision {
		t.Fatal("recreation retry changed commit", retry)
	}
	mutated := *fresh
	mutated.BaseRevision = "other"
	if _, _, e := ApplyReliableNote(context.Background(), f, 1, &mutated); e == nil {
		t.Fatal("recreation identity drift accepted")
	}
}

func TestReliablePendingWithoutRecreateKeepsProtocolOneIdentity(t *testing.T) {
	f := reliableTestEnv(t)
	p := reliableReq("legacy-pending-request", "", "Aa", false)
	applyTest(t, f, p)
	receipt := filepath.Join("storage", "merge-receipts", "1", NoteDigest("v"), NoteDigest("note.md"), NoteDigest(p.RequestID)+".json")
	raw, err := os.ReadFile(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), NoteDigest("false\x00\x00Aa\x00note.md")) {
		t.Fatal("default recreation fields changed pending request identity")
	}
}
