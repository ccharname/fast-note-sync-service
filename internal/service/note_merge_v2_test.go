package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// ---- fakes: embed the interface, override only what the code under test touches ----

type fakeVault struct{ VaultService }

func (fakeVault) MustGetID(ctx context.Context, uid int64, name string) (int64, error) { return 1, nil }

func (fakeVault) UpdateNoteStats(context.Context, int64, int64, int64, int64) error { return nil }

type fakeFolder struct{ FolderService }

func (fakeFolder) SyncResourceFID(context.Context, int64, int64, []int64, []int64) error { return nil }
func (fakeFolder) EnsurePathFID(context.Context, int64, int64, string) (int64, error)    { return 0, nil }
func (fakeFolder) CleanupEmptyAncestors(context.Context, int64, int64, string) error     { return nil }

type fakeShare struct{ domain.UserShareRepository }

func (fakeShare) UpdateStatusByRes(context.Context, int64, string, int64, int64) error { return nil }
func (fakeShare) MigrateResID(context.Context, int64, int64, int64) error              { return nil }

const tuid = int64(7)

func newMergeEnv(t *testing.T) *noteService {
	t.Helper()
	orig, _ := os.Getwd()
	dir := t.TempDir()
	require.NoError(t, os.Chdir(dir))
	require.NoError(t, os.MkdirAll(filepath.Join("storage", "database"), 0755))
	dbPath := filepath.Join("storage", "database", "db.sqlite3")
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	require.NoError(t, err)
	cfg := &config.DatabaseConfig{Type: "sqlite", Path: dbPath, EnableWriteQueue: util.Ptr(false)}
	d := dao.New(db, context.Background(), dao.WithConfig(cfg), dao.WithUserDatabaseConfig(cfg), dao.WithLogger(zap.NewNop()), dao.WithBleveManager(dao.NewBleveManager(util.Ptr(false), util.Ptr(false), zap.NewNop())))
	userDB := d.ResolveDB(fmt.Sprintf("user_%d", tuid))
	require.NoError(t, userDB.AutoMigrate(&model.Note{}))
	_ = model.CreateNoteFTSTable(userDB)

	stop := make(chan struct{})
	go func() { // drain the history channel so writers never block
		for {
			select {
			case <-NoteHistoryChannel:
			case <-stop:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		if sdb, err := db.DB(); err == nil {
			_ = sdb.Close()
		}
		_ = os.Chdir(orig)
	})
	svc := NewNoteService(nil, dao.NewNoteRepository(d), nil, nil, fakeShare{}, nil, fakeVault{}, fakeFolder{}, nil, nil, nil, nil)
	return svc.WithClient("desktop", "Mac Book", "1").(*noteService)
}

func mreq(path, content, baseContent, op string) *dto.NoteModifyOrCreateRequest {
	r := &dto.NoteModifyOrCreateRequest{
		Vault: "v", Path: path, PathHash: util.EncodeHash32(path),
		Content: content, ContentHash: util.EncodeHash32(content),
		Ctime: 1, Mtime: 2, ServerMerge: true, OpID: op,
	}
	if baseContent != "<none>" {
		r.BaseSha = sha256Hex(baseContent)
		r.BaseHash = util.EncodeHash32(baseContent)
	}
	return r
}

func modify(t *testing.T, s *noteService, r *dto.NoteModifyOrCreateRequest) *MergeResult {
	t.Helper()
	var got *MergeResult
	require.NoError(t, s.ServerMergeModify(context.Background(), tuid, r, func(m *MergeResult) { got = m }))
	require.NotNil(t, got)
	return got
}

func current(t *testing.T, s *noteService, path string) *domain.Note {
	n, err := s.noteRepo.GetAllByPathHash(context.Background(), util.EncodeHash32(path), 1, tuid)
	require.NoError(t, err)
	return n
}

func TestServerMerge_CreateFastForwardAndMerge(t *testing.T) {
	s := newMergeEnv(t)
	base := "a\nb\nc\nd\n"
	r1 := modify(t, s, mreq("n.md", base, "<none>", "op1"))
	assert.False(t, r1.Ack.Merged)
	assert.Equal(t, sha256Hex(base), r1.Ack.Sha)
	assert.NotNil(t, r1.Written)

	// device A edits line a (fast-forward from base)
	A := "A\nb\nc\nd\n"
	r2 := modify(t, s, mreq("n.md", A, base, "op2"))
	assert.False(t, r2.Ack.Merged)
	assert.Greater(t, r2.Ack.Rev, r1.Ack.Rev)

	// device B edits line d on top of base: server != base -> three-way merge
	B := "a\nb\nc\nD\n"
	r3 := modify(t, s, mreq("n.md", B, base, "op3"))
	assert.True(t, r3.Ack.Merged)
	assert.Equal(t, "A\nb\nc\nD\n", r3.Ack.Content)
	assert.Equal(t, "A\nb\nc\nD\n", current(t, s, "n.md").Content)
	assert.Greater(t, r3.Ack.Rev, r2.Ack.Rev)

	// same content again: no write, same rev
	r4 := modify(t, s, mreq("n.md", "A\nb\nc\nD\n", "A\nb\nc\nD\n", "op4"))
	assert.Nil(t, r4.Written)
	assert.Equal(t, r3.Ack.Rev, r4.Ack.Rev)
}

func TestServerMerge_ConflictCopyAndReplayIdempotent(t *testing.T) {
	s := newMergeEnv(t)
	base := "line1\nline2\n"
	modify(t, s, mreq("c.md", base, "<none>", "o0"))
	modify(t, s, mreq("c.md", "line1\nSERVER\n", base, "o1"))

	req := mreq("c.md", "line1\nCLIENT\n", base, "o2")
	r := modify(t, s, req)
	require.NotEmpty(t, r.Ack.ConflictCopy)
	assert.True(t, r.Ack.Merged)
	assert.Equal(t, "line1\nSERVER\n", r.Ack.Content) // server version kept
	assert.NotNil(t, r.Copy)
	assert.Nil(t, r.Written)
	assert.Contains(t, r.Ack.ConflictCopy, ".conflict-MacBook-")
	cp := current(t, s, r.Ack.ConflictCopy)
	assert.Equal(t, "line1\nCLIENT\n", cp.Content)

	// replay the same opId: same Ack, no second copy, no broadcast
	r2 := modify(t, s, req)
	assert.Equal(t, r.Ack, r2.Ack)
	assert.Nil(t, r2.Copy)
	assert.Nil(t, r2.Written)

	// replay with a NEW opId but same content (client lost the ack): copy not duplicated
	r3 := modify(t, s, mreq("c.md", "line1\nCLIENT\n", base, "o3"))
	assert.Equal(t, r.Ack.ConflictCopy, r3.Ack.ConflictCopy)
	assert.Nil(t, r3.Copy)

	// different content landing on the same copy name gets a -2 suffix (forced by pre-existing file)
	other := "line1\nOTHER\n"
	name := conflictCopyPath("c.md", s.clientName, sha256Hex(other), 1)
	_, _, err := s.modifyOrCreateInner(context.Background(), tuid, &dto.NoteModifyOrCreateRequest{
		Vault: "v", Path: name, PathHash: util.EncodeHash32(name), Content: "x", ContentHash: util.EncodeHash32("x"), Ctime: 1, Mtime: 1}, false)
	require.NoError(t, err)
	r4 := modify(t, s, mreq("c.md", other, base, "o4"))
	assert.Equal(t, strings.TrimSuffix(name, ".md")+"-2.md", r4.Ack.ConflictCopy)
}

func TestServerMerge_NoBaseGoesToConflictCopy(t *testing.T) {
	s := newMergeEnv(t)
	modify(t, s, mreq("m.md", "server\n", "<none>", "a"))
	r := modify(t, s, mreq("m.md", "client\n", "<none>", "b"))
	assert.NotEmpty(t, r.Ack.ConflictCopy)
	assert.Equal(t, "server\n", current(t, s, "m.md").Content)
	// base given but unknown snapshot
	r = modify(t, s, mreq("m.md", "client2\n", "never seen\n", "c"))
	assert.NotEmpty(t, r.Ack.ConflictCopy)
}

func TestServerMerge_HashOnlyTransition(t *testing.T) {
	s := newMergeEnv(t)
	modify(t, s, mreq("h.md", "one\n", "<none>", "a"))
	// legacy client: only hash32 base, equals server -> fast-forward
	q := mreq("h.md", "two\n", "one\n", "b")
	q.BaseSha = ""
	r := modify(t, s, q)
	assert.Nil(t, nilIf(r.Ack.ConflictCopy))
	assert.Equal(t, "two\n", current(t, s, "h.md").Content)
	// hash32 base that is not the server's -> conflict copy
	q = mreq("h.md", "three\n", "zzz\n", "c")
	q.BaseSha = ""
	r = modify(t, s, q)
	assert.NotEmpty(t, r.Ack.ConflictCopy)
}

func nilIf(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func TestServerMerge_ConcurrentSameBaseSerialized(t *testing.T) {
	for round := 0; round < 5; round++ {
		s := newMergeEnv(t)
		base := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n"
		modify(t, s, mreq("p.md", base, "<none>", "seed"))
		const n = 6
		contents := make([]string, n)
		for i := range contents {
			ls := strings.Split(base, "\n")
			ls[2*i] = fmt.Sprintf("edit%d", i)
			contents[i] = strings.Join(ls, "\n")
		}
		var wg sync.WaitGroup
		revs := make([]int64, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var got *MergeResult
				require.NoError(t, s.ServerMergeModify(context.Background(), tuid, mreq("p.md", contents[i], base, fmt.Sprintf("op%d", i)), func(m *MergeResult) { got = m }))
				revs[i] = got.Ack.Rev
			}(i)
		}
		wg.Wait()
		final := current(t, s, "p.md").Content
		assert.Equal(t, "edit0\n2\nedit1\n4\nedit2\n6\nedit3\n8\nedit4\n10\nedit5\n12\n", final, "all disjoint edits survive in any arrival order")
		seen := map[int64]bool{}
		for _, r := range revs {
			assert.False(t, seen[r], "revs unique")
			seen[r] = true
		}
	}
}

func TestServerMerge_DeleteAndRename(t *testing.T) {
	s := newMergeEnv(t)
	ctx := context.Background()
	modify(t, s, mreq("d.md", "x\n", "<none>", "a"))
	modify(t, s, mreq("d.md", "x\ny\n", "x\n", "b"))

	del := func(base, op string) *MergeResult {
		var got *MergeResult
		require.NoError(t, s.ServerMergeDelete(ctx, tuid, &dto.NoteDeleteRequest{Vault: "v", Path: "d.md", PathHash: util.EncodeHash32("d.md"), ServerMerge: true, OpID: op, BaseSha: sha256Hex(base)}, func(m *MergeResult) { got = m }))
		return got
	}
	r := del("x\n", "d1") // stale base -> rejected
	assert.True(t, r.Ack.Rejected)
	assert.Equal(t, "contentChanged", r.Ack.RejectReason)
	assert.Equal(t, "x\ny\n", r.Ack.Content)
	assert.Nil(t, r.Deleted)
	assert.Equal(t, domain.NoteActionModify, current(t, s, "d.md").Action)

	r = del("x\ny\n", "d2")
	assert.False(t, r.Ack.Rejected)
	assert.NotNil(t, r.Deleted)
	assert.Equal(t, domain.NoteActionDelete, current(t, s, "d.md").Action)
	rev := r.Ack.Rev
	r = del("x\ny\n", "d2") // replay
	assert.Equal(t, rev, r.Ack.Rev)
	assert.Nil(t, r.Deleted)
	r = del("whatever", "d3") // already deleted: idempotent success
	assert.False(t, r.Ack.Rejected)

	// recreate after delete keeps rev increasing
	r5 := modify(t, s, mreq("d.md", "again\n", "<none>", "e"))
	assert.Greater(t, r5.Ack.Rev, rev)

	// rename
	ren := func(from, to, base, op string) *MergeResult {
		var got *MergeResult
		require.NoError(t, s.ServerMergeRename(ctx, tuid, &dto.NoteRenameRequest{Vault: "v", Path: to, PathHash: util.EncodeHash32(to), OldPath: from, OldPathHash: util.EncodeHash32(from), ServerMerge: true, OpID: op, BaseSha: sha256Hex(base)}, func(m *MergeResult) { got = m }))
		return got
	}
	rr := ren("d.md", "e.md", "again\n", "r1")
	assert.False(t, rr.Ack.Rejected)
	assert.NotNil(t, rr.RenamedNew)
	assert.Equal(t, "again\n", current(t, s, "e.md").Content)
	assert.Equal(t, domain.NoteActionDelete, current(t, s, "d.md").Action)
	assert.Greater(t, rr.Ack.Rev, r5.Ack.Rev)
	// replay
	rr2 := ren("d.md", "e.md", "again\n", "r1")
	assert.Equal(t, rr.Ack.Rev, rr2.Ack.Rev)
	assert.Nil(t, rr2.RenamedNew)
	// snapshots followed the rename: a client with base "again\n" can still merge at the new path
	modify(t, s, mreq("e.md", "again\nplus\n", "again\n", "w1"))
	m := modify(t, s, mreq("e.md", "ZZ\nagain\n", "again\n", "w2"))
	assert.False(t, m.Ack.ConflictCopy != "")
	assert.Equal(t, "ZZ\nagain\nplus\n", current(t, s, "e.md").Content)

	// target exists with different content -> rejected, nothing moved
	modify(t, s, mreq("f.md", "ff\n", "<none>", "f1"))
	rj := ren("f.md", "e.md", "ff\n", "r2")
	assert.True(t, rj.Ack.Rejected)
	assert.Equal(t, "targetExists", rj.Ack.RejectReason)
	assert.Equal(t, "ZZ\nagain\nplus\n", rj.Ack.Content)
	assert.Equal(t, domain.NoteActionCreate, current(t, s, "f.md").Action)
	// source missing
	rm := ren("nope.md", "g.md", "", "r3")
	assert.True(t, rm.Ack.Rejected)
	assert.Equal(t, "sourceMissing", rm.Ack.RejectReason)
}

// Every write entry shares one lock and one monotone rev, serverMerge or not.
func TestRevMonotoneAcrossEntries(t *testing.T) {
	s := newMergeEnv(t)
	ctx := context.Background()
	var last int64
	check := func(rev int64) {
		assert.Greater(t, rev, last)
		last = rev
	}
	r := modify(t, s, mreq("r.md", "v1\n", "<none>", "1"))
	check(r.Ack.Rev)
	// upstream path (no serverMerge) via ModifyOrCreate
	_, d, err := s.ModifyOrCreate(ctx, tuid, &dto.NoteModifyOrCreateRequest{Vault: "v", Path: "r.md", PathHash: util.EncodeHash32("r.md"), Content: "v2\n", ContentHash: util.EncodeHash32("v2\n"), Ctime: 1, Mtime: 3}, true)
	require.NoError(t, err)
	check(d.UpdatedTimestamp)
	// REST-style append
	ap, err := s.AppendContent(ctx, tuid, &dto.NoteAppendRequest{Vault: "v", Path: "r.md", PathHash: util.EncodeHash32("r.md"), Content: "tail\n"})
	require.NoError(t, err)
	check(ap.UpdatedTimestamp)
	// upstream delete then restore
	dd, err := s.Delete(ctx, tuid, &dto.NoteDeleteRequest{Vault: "v", Path: "r.md", PathHash: util.EncodeHash32("r.md")})
	require.NoError(t, err)
	check(dd.UpdatedTimestamp)
	rs, err := s.Restore(ctx, tuid, &dto.NoteRestoreRequest{Vault: "v", Path: "r.md", PathHash: util.EncodeHash32("r.md")})
	require.NoError(t, err)
	check(rs.UpdatedTimestamp)
}

// Without serverMerge the handler never reaches ServerMerge*; ModifyOrCreate itself behaves as upstream.
func TestUpstreamModifyOrCreateUnchanged(t *testing.T) {
	s := newMergeEnv(t)
	ctx := context.Background()
	p := &dto.NoteModifyOrCreateRequest{Vault: "v", Path: "u.md", PathHash: util.EncodeHash32("u.md"), Content: "c1", ContentHash: util.EncodeHash32("c1"), Ctime: 1, Mtime: 2}
	isNew, d, err := s.ModifyOrCreate(ctx, tuid, p, true)
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, "c1", d.Content)
	// identical content+mtime => upstream returns nil dto (no write)
	_, d2, err := s.ModifyOrCreate(ctx, tuid, p, true)
	require.NoError(t, err)
	assert.Nil(t, d2)
	// createOnly on existing => error
	p2 := *p
	p2.CreateOnly = true
	p2.Content, p2.ContentHash = "c2", util.EncodeHash32("c2")
	_, _, err = s.ModifyOrCreate(ctx, tuid, &p2, true)
	assert.Error(t, err)
	// plain overwrite is last-writer-wins (no merge)
	p3 := *p
	p3.Content, p3.ContentHash, p3.Mtime = "c3", util.EncodeHash32("c3"), 5
	_, d3, err := s.ModifyOrCreate(ctx, tuid, &p3, true)
	require.NoError(t, err)
	assert.Equal(t, "c3", d3.Content)
}

func TestSnapRetention(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 40; i++ {
		_, err := writeSnap(dir, fmt.Sprintf("content %d", i))
		require.NoError(t, err)
		time.Sleep(2 * time.Millisecond)
	}
	pruneSnaps(dir) // unreferenced: only the newest snapKeepRecent survive
	ents, _ := os.ReadDir(dir)
	assert.Equal(t, snapKeepRecent, len(ents))
	_, ok := loadSnap(dir, "../../etc/passwd")
	assert.False(t, ok)
	_, ok = loadSnap(dir, sha256Hex("content 39"))
	assert.True(t, ok)
	_, ok = loadSnap(dir, sha256Hex("content 0"))
	assert.False(t, ok)
}

// Conflict copies are written under their own key, taken together with the main key in
// lexicographic order: concurrent conflicts + a writer hitting copy paths must not deadlock
// and every client version must survive.
func TestServerMerge_ConcurrentConflictCopiesNoDeadlock(t *testing.T) {
	s := newMergeEnv(t)
	base := "l1\nl2\nl3\n"
	modify(t, s, mreq("k.md", base, "<none>", "seed"))
	modify(t, s, mreq("k.md", "l1\nSERVER\nl3\n", base, "srv"))
	const n = 8
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c := fmt.Sprintf("l1\nCLIENT%d\nl3\n", i)
				modify(t, s, mreq("k.md", c, base, fmt.Sprintf("c%d", i)))
			}(i)
			wg.Add(1)
			go func(i int) { // plain writer on a (likely) copy path
				defer wg.Done()
				c := fmt.Sprintf("l1\nCLIENT%d\nl3\n", i)
				p := conflictCopyPath("k.md", s.clientName, sha256Hex(c), 1)
				_, _, _ = s.ModifyOrCreate(context.Background(), tuid, &dto.NoteModifyOrCreateRequest{Vault: "v", Path: p, PathHash: util.EncodeHash32(p), Content: c, ContentHash: util.EncodeHash32(c), Ctime: 1, Mtime: 2}, false)
			}(i)
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock")
	}
	assert.Equal(t, "l1\nSERVER\nl3\n", current(t, s, "k.md").Content)
	for i := 0; i < n; i++ {
		c := fmt.Sprintf("l1\nCLIENT%d\nl3\n", i)
		cp := current(t, s, conflictCopyPath("k.md", s.clientName, sha256Hex(c), 1))
		assert.Equal(t, c, cp.Content)
	}
}
