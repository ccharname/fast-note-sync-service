package dao

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextRevStrictlyIncreasing(t *testing.T) {
	const n = 20000
	out := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); out[i] = NextRev(0) }(i)
	}
	wg.Wait()
	seen := make(map[int64]bool, n)
	for _, v := range out {
		if seen[v] {
			t.Fatalf("duplicate rev %d", v)
		}
		seen[v] = true
	}
	if r := NextRev(out[0] + 1_000_000); r <= out[0]+1_000_000 {
		t.Fatalf("atLeast not honoured: %d", r)
	}
}

// A fresh process must start numbering above every rev already stored in a user's DB
// (clock rolled back / restored DB), seeded once per user DB on first access.
func TestRevSeededFromDBMax(t *testing.T) {
	orig, _ := os.Getwd()
	require.NoError(t, os.Chdir(t.TempDir()))
	defer os.Chdir(orig)
	require.NoError(t, os.MkdirAll(filepath.Join("storage", "database"), 0755))
	dbPath := filepath.Join("storage", "database", "db.sqlite3")
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	require.NoError(t, err)
	defer func() { sqlDB, _ := db.DB(); sqlDB.Close() }()
	cfg := &config.DatabaseConfig{Type: "sqlite", Path: dbPath, EnableWriteQueue: util.Ptr(false)}
	daoInst := New(db, context.Background(), WithConfig(cfg), WithUserDatabaseConfig(cfg), WithLogger(zap.NewNop()),
		WithBleveManager(NewBleveManager(util.Ptr(false), util.Ptr(false), zap.NewNop())))
	noteRepo := NewNoteRepository(daoInst)
	uid := int64(5)
	future := time.Now().Add(48*time.Hour).UnixMilli()

	userDb := daoInst.ResolveDB("user_5")
	require.NoError(t, userDb.AutoMigrate(&model.Note{}))
	require.NoError(t, userDb.Create(&model.Note{ID: 1, VaultID: 1, Path: "old.md", PathHash: "h1", Action: "create", UpdatedTimestamp: future}).Error)

	n, err := noteRepo.Create(context.Background(), &domain.Note{VaultID: 1, Path: "new.md", PathHash: "h2", Content: "x", ContentHash: "1", Action: domain.NoteActionCreate}, uid)
	require.NoError(t, err)
	require.Greater(t, n.UpdatedTimestamp, future)
}
