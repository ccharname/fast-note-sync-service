package dao

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/haierkeys/fast-note-sync-service/pkg/writequeue"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const barrierUID int64 = 42

func newBarrierRepo(t *testing.T, queued bool, timeout time.Duration) (*noteRepository, *gorm.DB) {
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
	queue := writequeue.New(&writequeue.Config{WriteTimeout: timeout}, zap.NewNop())
	d := New(db, context.Background(), WithConfig(cfg), WithUserDatabaseConfig(cfg),
		WithLogger(zap.NewNop()), WithWriteQueueManager(queue),
		WithBleveManager(NewBleveManager(util.Ptr(false), util.Ptr(false), zap.NewNop())))
	r := NewNoteRepository(d).(*noteRepository)
	r.note(barrierUID)
	userDB := d.ResolveDB(r.GetKey(barrierUID))
	t.Cleanup(func() {
		require.NoError(t, queue.Shutdown(context.Background()))
		for _, entry := range d.KeyDb {
			sqlDB, err := entry.db.DB()
			if err == nil {
				require.NoError(t, sqlDB.Close())
			}
		}
		sqlDB, err := db.DB()
		if err == nil {
			require.NoError(t, sqlDB.Close())
		}
	})
	return r, userDB
}

func barrierNote(path string) *domain.Note {
	return &domain.Note{VaultID: 1, Path: path, PathHash: util.EncodeHash32(path),
		Content: "body", ContentHash: util.EncodeHash32("body"), Action: domain.NoteActionCreate}
}

// Pause after revision allocation but before the actual note SQL statement.
func pauseNoteSQL(t *testing.T, db *gorm.DB) (<-chan struct{}, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var once, released sync.Once
	callback := func(tx *gorm.DB) {
		if tx.Statement.Schema == nil || tx.Statement.Schema.Table != "note" {
			return
		}
		once.Do(func() { close(entered); <-release })
	}
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:pause_note", callback))
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:pause_note", callback))
	unblock := func() { released.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return entered, unblock
}

func awaitBarrierSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("SQL callback did not start")
	}
}

func barrierTime() int64 {
	return max(time.Now().UnixMilli(), lastRev.Load()) + 10_000
}

func TestNoteSyncBarrierWaitsForSQL(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, kind := range []string{"create", "update", "delete", "restore", "mtime", "action-mtime", "recycle-clear", "rename"} {
			t.Run(fmt.Sprintf("queue=%v/%s", queued, kind), func(t *testing.T) {
				r, db := newBarrierRepo(t, queued, 5*time.Second)
				ctx := context.Background()
				n, err := r.Create(ctx, barrierNote("old.md"), barrierUID)
				require.NoError(t, err)
				if kind == "restore" || kind == "recycle-clear" {
					n.Action = domain.NoteActionDelete
					require.NoError(t, r.UpdateDelete(ctx, n, barrierUID))
				}
				entered, release := pauseNoteSQL(t, db)
				writeDone := make(chan error, 1)
				go func() {
					var err error
					switch kind {
					case "create":
						_, err = r.Create(ctx, barrierNote("new.md"), barrierUID)
					case "update":
						n.Content = "edited"
						_, err = r.Update(ctx, n, barrierUID)
					case "delete":
						n.Action = domain.NoteActionDelete
						err = r.UpdateDelete(ctx, n, barrierUID)
					case "restore":
						n.Action = domain.NoteActionModify
						err = r.UpdateDelete(ctx, n, barrierUID)
					case "mtime":
						err = r.UpdateMtime(ctx, 900, n.ID, barrierUID)
					case "action-mtime":
						err = r.UpdateActionMtime(ctx, domain.NoteActionModify, 901, n.ID, barrierUID)
					case "recycle-clear":
						err = r.RecycleClear(ctx, "", n.PathHash, 1, barrierUID)
					case "rename":
						n.Action, n.Rename = domain.NoteActionDelete, 1
						_, err = r.Update(ctx, n, barrierUID)
						if err == nil {
							_, err = r.Create(ctx, barrierNote("renamed.md"), barrierUID)
						}
					}
					writeDone <- err
				}()
				awaitBarrierSignal(t, entered)
				stamp := barrierTime()
				type snapshot struct {
					cursor int64
					rows   []*domain.Note
					err    error
				}
				syncDone := make(chan snapshot, 1)
				go func() {
					var rows []*domain.Note
					cursor, err := withNoteSyncSnapshot(func() int64 { return stamp }, func() error {
						var err error
						rows, err = r.ListByUpdatedTimestampMeta(ctx, 0, 1, barrierUID)
						return err
					})
					syncDone <- snapshot{cursor, rows, err}
				}()
				var early *snapshot
				select {
				case result := <-syncDone:
					early = &result
					t.Error("sync completed while a revision-bearing SQL write was still uncommitted")
				case <-time.After(40 * time.Millisecond):
				}
				release()
				require.NoError(t, <-writeDone)
				var snap snapshot
				if early != nil {
					snap = *early
				} else {
					snap = <-syncDone
				}
				require.NoError(t, snap.err)
				require.Equal(t, stamp, snap.cursor, "cursor must remain the query-before wall clock")
				// Any row absent/stale in the snapshot must be returned by strict >cursor incremental sync.
				later, err := r.ListByUpdatedTimestampMeta(ctx, snap.cursor, 1, barrierUID)
				require.NoError(t, err)
				seen := map[int64]int64{}
				for _, row := range append(snap.rows, later...) {
					seen[row.ID] = max(seen[row.ID], row.UpdatedTimestamp)
				}
				all, err := r.ListByUpdatedTimestampMeta(ctx, 0, 1, barrierUID)
				require.NoError(t, err)
				for _, row := range all {
					require.Equal(t, row.UpdatedTimestamp, seen[row.ID], "committed row %s was permanently skipped", row.Path)
				}
			})
		}
	}
}

func TestNoteSyncBarrierSameMillisecondAndQueryFailure(t *testing.T) {
	stamp := barrierTime()
	wantErr := errors.New("query failed")
	cursor, err := withNoteSyncSnapshot(func() int64 { return stamp }, func() error { return wantErr })
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, cursor, "a failed query cannot publish a completion cursor")
	done := make(chan int64, 1)
	go func() {
		cursor, err := withNoteSyncSnapshot(func() int64 { return stamp }, func() error { return nil })
		if err != nil {
			done <- 0
			return
		}
		done <- cursor
	}()
	select {
	case cursor = <-done:
		require.Equal(t, stamp, cursor)
	case <-time.After(time.Second):
		t.Fatal("failed query leaked the exclusive lock")
	}
	require.Greater(t, NextRev(0), stamp, "a write in the same wall-clock millisecond must exceed the completed cursor")
}

func TestNoteSyncBarrierLateWriteAppearsInNextIncrement(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue=%v", queued), func(t *testing.T) {
			r, db := newBarrierRepo(t, queued, 5*time.Second)
			enteredSQL, releaseSQL := pauseNoteSQL(t, db)
			querying, releaseQuery := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblockQuery := func() { releaseOnce.Do(func() { close(releaseQuery) }) }
			t.Cleanup(unblockQuery)
			stamp := barrierTime()
			syncDone := make(chan error, 1)
			go func() {
				_, err := withNoteSyncSnapshot(func() int64 { return stamp }, func() error {
					close(querying)
					<-releaseQuery
					rows, err := r.ListByUpdatedTimestampMeta(context.Background(), 0, 1, barrierUID)
					if err == nil && len(rows) != 0 {
						return fmt.Errorf("late write entered the snapshot: %d rows", len(rows))
					}
					return err
				})
				syncDone <- err
			}()
			awaitBarrierSignal(t, querying)
			writeDone := make(chan error, 1)
			go func() { _, err := r.Create(context.Background(), barrierNote("late.md"), barrierUID); writeDone <- err }()
			select {
			case <-enteredSQL:
				t.Error("late writer allocated a revision during the metadata snapshot")
			case <-time.After(40 * time.Millisecond):
			}
			unblockQuery()
			require.NoError(t, <-syncDone)
			awaitBarrierSignal(t, enteredSQL)
			releaseSQL()
			require.NoError(t, <-writeDone)
			rows, err := r.ListByUpdatedTimestampMeta(context.Background(), stamp, 1, barrierUID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "late.md", rows[0].Path)
			require.Greater(t, rows[0].UpdatedTimestamp, stamp)
		})
	}
}

func TestNoteSyncBarrierWaitsForTimedOutWorker(t *testing.T) {
	r, db := newBarrierRepo(t, true, 50*time.Millisecond)
	entered, release := pauseNoteSQL(t, db)
	writeDone := make(chan error, 1)
	go func() { _, err := r.Create(context.Background(), barrierNote("late.md"), barrierUID); writeDone <- err }()
	awaitBarrierSignal(t, entered)
	require.ErrorIs(t, <-writeDone, writequeue.ErrWriteTimeout)
	done := make(chan error, 1)
	go func() {
		_, err := WithNoteSyncSnapshot(func() error {
			rows, err := r.ListByUpdatedTimestampMeta(context.Background(), 0, 1, barrierUID)
			if err == nil && len(rows) != 1 {
				return fmt.Errorf("late commit absent: %d rows", len(rows))
			}
			return err
		})
		done <- err
	}()
	var early *error
	select {
	case err := <-done:
		early = &err
		t.Error("caller timeout released protection while worker still owns an uncommitted rev")
	case <-time.After(40 * time.Millisecond):
	}
	release()
	if early != nil {
		require.NoError(t, *early)
	} else {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("sync did not resume after worker commit")
		}
	}
}
