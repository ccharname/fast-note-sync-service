package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func crashAt(stage string) {
	failpoint = func(s string) error {
		if s == stage {
			return errCrash
		}
		return nil
	}
}

func restart(t *testing.T) {
	t.Helper()
	failpoint = nil
	resetRecoveryForTest()
}

func body(t *testing.T, s *noteService, path string) string {
	n := current(t, s, path)
	require.NotNil(t, n)
	return n.Content
}

// R2-1 (review item 10): retry of the same content with a NEW opId must not insert twice.
func TestR2_ReplayWithNewOpIdNoDuplicateInsert(t *testing.T) {
	s := newMergeEnv(t)
	base := "a\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	modify(t, s, mreq("n.md", "s\na\n", base, "srv"))
	r1 := modify(t, s, mreq("n.md", "c\na\n", base, "op1"))
	first := body(t, s, "n.md")
	assert.True(t, r1.Ack.Merged)
	r2 := modify(t, s, mreq("n.md", "c\na\n", base, "op2-retry"))
	assert.Equal(t, first, body(t, s, "n.md"))
	assert.Equal(t, 1, strings.Count(first, "c\n"))
	assert.Equal(t, r1.Ack.Sha, r2.Ack.Sha)
	assert.Equal(t, "op2-retry", r2.Ack.OpID)
}

// A legitimate repeat edit (state moved on since) must not be swallowed by content-addressed replay.
func TestR2_RepeatedEditAfterServerMovedIsNotSwallowed(t *testing.T) {
	s := newMergeEnv(t)
	base := "a\nb\nc\nd\ne\nf\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	modify(t, s, mreq("n.md", "X\nb\nc\nd\ne\nf\n", base, "o1"))
	modify(t, s, mreq("n.md", "a\nb\nc\nd\ne\nY\n", base, "o2"))
	// server edited by someone else, then the same (base, C) as o1 arrives again as a new edit
	modify(t, s, mreq("n.md", "X\nb\nc\nd\ne\nZ\n", "X\nb\nc\nd\ne\nY\n", "o3"))
	assert.Equal(t, "X\nb\nc\nd\ne\nZ\n", body(t, s, "n.md"))
}

func TestR2_CrashPointsModify(t *testing.T) {
	for _, stage := range []string{"after_snapshot", "after_intent", "write", "after_write"} {
		for _, create := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/create=%v", stage, create), func(t *testing.T) {
				defer restart(t)
				s := newMergeEnv(t)
				base := "l1\nl2\nl3\nl4\nl5\nl6\nl7\n"
				var req *dto.NoteModifyOrCreateRequest
				want := ""
				if create {
					req = mreq("n.md", base, "<none>", "op1")
					want = base
				} else {
					modify(t, s, mreq("n.md", base, "<none>", "seed"))
					modify(t, s, mreq("n.md", "SRV\nl2\nl3\nl4\nl5\nl6\nl7\n", base, "srv"))
					req = mreq("n.md", "l1\nl2\nl3\nl4\nl5\nl6\nCLI\n", base, "op1")
					want = "SRV\nl2\nl3\nl4\nl5\nl6\nCLI\n"
				}
				crashAt(stage)
				err := s.ServerMergeModify(context.Background(), tuid, req, func(*MergeResult) { t.Fatal("no ack may be emitted on crash") })
				require.True(t, errors.Is(err, errCrash))
				restart(t)
				// roll-forward happens on the next lock of the note, before anything else
				unlock := s.lockAndRecover(context.Background(), tuid, 1, req.PathHash)
				unlock()
				n, _ := s.getAny(context.Background(), req.PathHash, 1, tuid)
				// after_snapshot: no intent yet, nothing to roll forward; the client retry redoes it
				if stage != "after_snapshot" {
					require.NotNil(t, n, "rolled forward")
					assert.Equal(t, want, n.Content, "rolled forward")
				}
				// client retry (new opId): exactly the expected body, no duplicate
				r := modify(t, s, mreq("n.md", req.Content, baseOf(req, create), "op-retry"))
				assert.Equal(t, want, body(t, s, "n.md"))
				assert.Equal(t, sha256Hex(want), r.Ack.Sha)
				assert.Equal(t, 0, pendingCount())
			})
		}
	}
}

func baseOf(req *dto.NoteModifyOrCreateRequest, create bool) string {
	if create {
		return "<none>"
	}
	return "l1\nl2\nl3\nl4\nl5\nl6\nl7\n"
}

func pendingCount() int { return int(pendingN.Load()) }

// crash-injection: the DB row is written but content.txt is the old body, or the reverse.
func TestR2_PartialWriteContentVsDB(t *testing.T) {
	s := newMergeEnv(t)
	base := "l1\nl2\nl3\nl4\nl5\nl6\nl7\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	modify(t, s, mreq("n.md", "SRV\nl2\nl3\nl4\nl5\nl6\nl7\n", base, "srv"))
	req := mreq("n.md", "l1\nl2\nl3\nl4\nl5\nl6\nCLI\n", base, "op1")
	want := "SRV\nl2\nl3\nl4\nl5\nl6\nCLI\n"
	crashAt("after_intent")
	require.True(t, errors.Is(s.ServerMergeModify(context.Background(), tuid, req, func(*MergeResult) {}), errCrash))
	// simulate content.txt already new but DB row old: apply the content file by hand
	n := current(t, s, "n.md")
	require.NotNil(t, n)
	require.NoError(t, dao.AtomicWriteFile(filepath.Dir(snapDir(tuid, n.ID)), "content.txt", []byte(want)))
	restart(t)
	unlock := s.lockAndRecover(context.Background(), tuid, 1, req.PathHash)
	unlock()
	assert.Equal(t, want, body(t, s, "n.md"))
	assert.Equal(t, 0, pendingCount())
}

// A critical persistence failure (snapshot cannot be written) means error and no ACK, no write.
func TestR2_SnapshotFailureNoAckNoWrite(t *testing.T) {
	s := newMergeEnv(t)
	base := "a\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	n := current(t, s, "n.md")
	sd := snapDir(tuid, n.ID)
	require.NoError(t, os.RemoveAll(sd))
	require.NoError(t, os.WriteFile(sd, []byte("x"), 0644)) // a file where the dir must be
	err := s.ServerMergeModify(context.Background(), tuid, mreq("n.md", "b\n", base, "op"), func(*MergeResult) { t.Fatal("ack on snapshot failure") })
	require.Error(t, err)
	assert.Equal(t, base, body(t, s, "n.md"))
}

func TestR2_CrashPointsDelete(t *testing.T) {
	for _, stage := range []string{"after_snapshot", "after_intent", "write", "after_write"} {
		t.Run(stage, func(t *testing.T) {
			defer restart(t)
			s := newMergeEnv(t)
			base := "a\n"
			modify(t, s, mreq("n.md", base, "<none>", "seed"))
			req := &dto.NoteDeleteRequest{Vault: "v", Path: "n.md", PathHash: mreq("n.md", "", "<none>", "").PathHash, BaseSha: sha256Hex(base), OpID: "d1"}
			crashAt(stage)
			err := s.ServerMergeDelete(context.Background(), tuid, req, func(*MergeResult) { t.Fatal("ack on crash") })
			require.True(t, errors.Is(err, errCrash))
			restart(t)
			var got *MergeResult
			require.NoError(t, s.ServerMergeDelete(context.Background(), tuid, req, func(m *MergeResult) { got = m }))
			require.NotNil(t, got)
			assert.False(t, got.Ack.Rejected)
			assert.Equal(t, 0, pendingCount())
			n := current(t, s, "n.md")
			assert.True(t, n == nil || n.Action == "delete")
		})
	}
}

func TestR2_CrashPointsRename(t *testing.T) {
	for _, stage := range []string{"after_snapshot", "after_intent", "write", "after_write"} {
		t.Run(stage, func(t *testing.T) {
			defer restart(t)
			s := newMergeEnv(t)
			base := "hello\nworld\n"
			modify(t, s, mreq("old.md", base, "<none>", "seed"))
			req := &dto.NoteRenameRequest{Vault: "v", Path: "new.md", OldPath: "old.md", BaseSha: sha256Hex(base), OpID: "r1"}
			crashAt(stage)
			err := s.ServerMergeRename(context.Background(), tuid, req, func(*MergeResult) { t.Fatal("ack on crash") })
			require.True(t, errors.Is(err, errCrash))
			restart(t)
			var got *MergeResult
			require.NoError(t, s.ServerMergeRename(context.Background(), tuid, req, func(m *MergeResult) { got = m }))
			require.NotNil(t, got)
			assert.False(t, got.Ack.Rejected, got.Ack.RejectReason)
			assert.Equal(t, base, body(t, s, "new.md"))
			o := current(t, s, "old.md")
			assert.True(t, o == nil || o.Action == "delete")
			// snapshots travelled with the note (moved, not copied)
			n := current(t, s, "new.md")
			_, ok := loadSnap(snapDir(tuid, n.ID), sha256Hex(base))
			assert.True(t, ok)
			assert.Equal(t, 0, pendingCount())
		})
	}
}

func TestR2_ConflictBudgetMakesCopy(t *testing.T) {
	s := newMergeEnv(t)
	base := "a\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	modify(t, s, mreq("n.md", "s\na\n", base, "srv"))
	big := "c\n" + strings.Repeat("x", diff3MaxBytes+10)
	r := modify(t, s, mreq("n.md", big, base, "op"))
	assert.NotEmpty(t, r.Ack.ConflictCopy)
	assert.Equal(t, "s\na\n", body(t, s, "n.md"))
	assert.Equal(t, big, body(t, s, r.Ack.ConflictCopy))
}

func TestR2_Diff3BudgetStatuses(t *testing.T) {
	_, st := Merge3Status("a\n", "s\na\n", "c\n"+strings.Repeat("y", diff3MaxBytes))
	assert.Equal(t, MergeBudget, st)
	var sb strings.Builder
	for i := 0; i < diff3MaxLines; i++ {
		sb.WriteString("l\n")
	}
	_, st = Merge3Status("a\n", sb.String(), sb.String()+"x\n")
	assert.Equal(t, MergeBudget, st)
	_, st = Merge3Status("a\nb\nc\nd\ne\nf\ng\n", "a\nb\nc\nd\ne\nf\nS\n", "C\nb\nc\nd\ne\nf\ng\n")
	assert.Equal(t, MergeOK, st)
}

// Physical delete / vault delete / recycle clear take the same note lock as writers.
func TestR2_PhysicalDeleteTakesNoteLock(t *testing.T) {
	s := newMergeEnv(t)
	modify(t, s, mreq("n.md", "a\n", "<none>", "seed"))
	n := current(t, s, "n.md")
	unlock := lockNotes(noteLockKey(tuid, 1, n.PathHash))
	done := make(chan error, 1)
	go func() {
		done <- s.noteRepo.Delete(context.Background(), n.ID, 1, tuid)
	}()
	select {
	case <-done:
		t.Fatal("repo.Delete ran while the note lock was held")
	case <-time.After(150 * time.Millisecond):
	}
	unlock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("repo.Delete did not proceed after unlock")
	}
}

func TestR2_ModifyWaitsForNoteLock(t *testing.T) {
	s := newMergeEnv(t)
	modify(t, s, mreq("n.md", "a\n", "<none>", "seed"))
	h := mreq("n.md", "", "<none>", "").PathHash
	unlock := lockNotes(noteLockKey(tuid, 1, h))
	var wg sync.WaitGroup
	wg.Add(1)
	fin := make(chan struct{})
	go func() {
		defer wg.Done()
		p := mreq("n.md", "b\n", "a\n", "op")
		_ = s.ServerMergeModify(context.Background(), tuid, p, func(*MergeResult) {})
		close(fin)
	}()
	select {
	case <-fin:
		t.Fatal("modify did not wait")
	case <-time.After(100 * time.Millisecond):
	}
	unlock()
	wg.Wait()
}

func TestR2_MergeStatsCountReasons(t *testing.T) {
	s := newMergeEnv(t)
	modify(t, s, mreq("n.md", "a\n", "<none>", "seed"))
	m := MergeStats()
	assert.GreaterOrEqual(t, m["ops"].(int64), int64(1))
	assert.Contains(t, m["reasons"], "modify:fastforward")
}

// A full history queue must never block a writer that holds the note lock.
func TestR2_HistoryPushNonBlocking(t *testing.T) {
	for len(NoteHistoryChannel) < cap(NoteHistoryChannel) {
		NoteHistoryChannel <- NoteHistoryMsg{}
	}
	defer func() {
		for len(NoteHistoryChannel) > 0 {
			<-NoteHistoryChannel
		}
	}()
	before := HistoryDropped.Load()
	done := make(chan struct{})
	go func() { NoteHistoryDelayPush(1, 1); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("NoteHistoryDelayPush blocked on a full queue")
	}
	assert.Equal(t, before+1, HistoryDropped.Load())
}
