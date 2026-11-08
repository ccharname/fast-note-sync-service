package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/haierkeys/fast-note-sync-service/pkg/writequeue"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Debounced statistics must not reopen a test DB after its temporary cwd is gone.
// Actual note writes, metadata reads, snapshot migration and tombstones stay real.
type syncBarrierStatsRepo struct{ domain.NoteRepository }

func (syncBarrierStatsRepo) CountSizeSum(context.Context, int64, int64) (*domain.CountSizeResult, error) {
	return &domain.CountSizeResult{}, nil
}

type syncBarrierShare struct {
	fakeShare
	migrated chan struct{}
}

func (s syncBarrierShare) MigrateResID(context.Context, int64, int64, int64) error {
	s.migrated <- struct{}{}
	return nil
}

func newSyncBarrierService(t *testing.T, queued bool) (*noteService, *gorm.DB, <-chan struct{}) {
	t.Helper()
	orig, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, os.Chdir(orig)) })
	require.NoError(t, os.MkdirAll(filepath.Join("storage", "database"), 0755))
	dbPath := filepath.Join("storage", "database", "db.sqlite3")
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	require.NoError(t, err)
	cfg := &config.DatabaseConfig{Type: "sqlite", Path: dbPath, EnableWriteQueue: util.Ptr(queued)}
	queue := writequeue.New(&writequeue.Config{WriteTimeout: 5 * time.Second}, zap.NewNop())
	d := dao.New(db, context.Background(), dao.WithConfig(cfg), dao.WithUserDatabaseConfig(cfg),
		dao.WithLogger(zap.NewNop()), dao.WithWriteQueueManager(queue),
		dao.WithBleveManager(dao.NewBleveManager(util.Ptr(false), util.Ptr(false), zap.NewNop())))
	r := dao.NewNoteRepository(d)
	_, err = r.ListByUpdatedTimestampMeta(context.Background(), 0, 1, tuid)
	require.NoError(t, err)
	userDB := d.ResolveDB(fmt.Sprintf("user_%d", tuid))
	migrated := make(chan struct{}, 1)
	s := NewNoteService(nil, syncBarrierStatsRepo{r}, nil, nil, syncBarrierShare{migrated: migrated},
		nil, fakeVault{}, fakeFolder{}, nil, nil, nil, nil).WithClient("desktop", "Mac Book", "1").(*noteService)
	t.Cleanup(func() {
		require.NoError(t, queue.Shutdown(context.Background()))
		for _, opened := range []*gorm.DB{userDB, db} {
			sqlDB, err := opened.DB()
			if err == nil {
				require.NoError(t, sqlDB.Close())
			}
		}
	})
	return s, userDB, migrated
}

func TestNoteSyncBarrierServiceEntrypoints(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, kind := range []string{"merge-create", "merge-update", "merge-delete", "merge-rename", "legacy-rename", "legacy-restore", "patch-frontmatter"} {
			t.Run(fmt.Sprintf("queue=%v/%s", queued, kind), func(t *testing.T) {
				s, db, migrated := newSyncBarrierService(t, queued)
				restart(t)
				defer restart(t)
				ctx := context.Background()
				n, err := s.noteRepo.Create(ctx, &domain.Note{VaultID: 1, Path: "n.md", PathHash: util.EncodeHash32("n.md"),
					Content: "base\n", ContentHash: util.EncodeHash32("base\n"), Action: domain.NoteActionCreate}, tuid)
				require.NoError(t, err)
				if kind == "legacy-restore" {
					n.Action = domain.NoteActionDelete
					require.NoError(t, s.noteRepo.UpdateDelete(ctx, n, tuid))
				}
				entered, release := make(chan struct{}), make(chan struct{})
				var once, releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				pause := func(tx *gorm.DB) {
					if tx.Statement.Schema != nil && tx.Statement.Schema.Table == "note" {
						once.Do(func() { close(entered); <-release })
					}
				}
				require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:sync_barrier", pause))
				require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:sync_barrier", pause))
				writeDone := make(chan error, 1)
				go func() {
					var err error
					switch kind {
					case "merge-create":
						err = s.ServerMergeModify(ctx, tuid, mreq("new.md", "new\n", "<none>", "sync-create"), func(*MergeResult) {})
					case "merge-update":
						err = s.ServerMergeModify(ctx, tuid, mreq("n.md", "edit\n", "base\n", "sync-update"), func(*MergeResult) {})
					case "merge-delete":
						err = s.ServerMergeDelete(ctx, tuid, &dto.NoteDeleteRequest{Vault: "v", Path: "n.md", PathHash: n.PathHash,
							ServerMerge: true, OpID: "sync-delete", BaseSha: sha256Hex("base\n")}, func(*MergeResult) {})
					case "merge-rename", "legacy-rename":
						req := &dto.NoteRenameRequest{Vault: "v", Path: "renamed.md", PathHash: util.EncodeHash32("renamed.md"),
							OldPath: "n.md", OldPathHash: n.PathHash, ServerMerge: true, OpID: "sync-rename", BaseSha: sha256Hex("base\n")}
						if kind == "merge-rename" {
							err = s.ServerMergeRename(ctx, tuid, req, func(*MergeResult) {})
						} else {
							_, _, err = s.Rename(ctx, tuid, req)
						}
					case "legacy-restore":
						_, err = s.Restore(ctx, tuid, &dto.NoteRestoreRequest{Vault: "v", Path: "n.md", PathHash: n.PathHash})
					case "patch-frontmatter":
						_, err = s.PatchFrontmatter(ctx, tuid, &dto.NotePatchFrontmatterRequest{Vault: "v", Path: "n.md", PathHash: n.PathHash,
							Updates: map[string]interface{}{"updated": true}})
					}
					writeDone <- err
				}()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("entrypoint never reached SQL")
				}
				type snapshot struct {
					cursor int64
					rows   []*dto.NoteDTO
					err    error
				}
				syncDone := make(chan snapshot, 1)
				go func() {
					var rows []*dto.NoteDTO
					cursor, err := dao.WithNoteSyncSnapshot(func() error {
						var err error
						rows, err = s.ListByLastTime(ctx, tuid, &dto.NoteSyncRequest{Vault: "v"})
						return err
					})
					syncDone <- snapshot{cursor, rows, err}
				}()
				var early *snapshot
				select {
				case snap := <-syncDone:
					early = &snap
					t.Error("service sync passed a paused revision-bearing SQL write")
				case <-time.After(40 * time.Millisecond):
				}
				unblock()
				require.NoError(t, <-writeDone)
				if kind == "merge-rename" || kind == "legacy-rename" {
					select {
					case <-migrated:
					case <-time.After(3 * time.Second):
						t.Fatal("background rename migration did not finish")
					}
				}
				var snap snapshot
				if early != nil {
					snap = *early
				} else {
					snap = <-syncDone
				}
				require.NoError(t, snap.err)
				later, err := s.ListByLastTime(ctx, tuid, &dto.NoteSyncRequest{Vault: "v", LastTime: snap.cursor})
				require.NoError(t, err)
				seen := map[int64]int64{}
				for _, row := range append(snap.rows, later...) {
					seen[row.ID] = max(seen[row.ID], row.UpdatedTimestamp)
				}
				all, err := s.ListByLastTime(ctx, tuid, &dto.NoteSyncRequest{Vault: "v"})
				require.NoError(t, err)
				for _, row := range all {
					require.Equal(t, row.UpdatedTimestamp, seen[row.ID], "entrypoint left an unreachable committed path %s", row.Path)
				}
			})
		}
	}
}
