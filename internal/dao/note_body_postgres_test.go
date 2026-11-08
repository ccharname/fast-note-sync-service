package dao

import (
	"context"
	"fmt"
	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	glog "gorm.io/gorm/logger"
	"os"
	"path/filepath"
	"testing"
)

// Explicit opt-in only: use an isolated disposable PostgreSQL database, never production.
func TestNoteBodyPostgresDurability(t *testing.T) {
	host := os.Getenv("FNS_REVIEW_PG_HOST")
	if host == "" {
		t.Skip("isolated PostgreSQL fixture not configured")
	}
	cfg := &config.DatabaseConfig{Type: "postgres", Host: host, Port: 5432, UserName: "fns_reviewer", Password: os.Getenv("FNS_REVIEW_PG_PASSWORD"), Name: "fns_reviewer", SSLMode: "disable", EnableWriteQueue: util.Ptr(false), AutoMigrate: util.Ptr(true)}
	if cfg.Password == "" {
		t.Fatal("isolated PostgreSQL password missing")
	}
	dsn := fmt.Sprintf("host=%s port=5432 user=fns_reviewer password=%s dbname=fns_reviewer sslmode=disable", host, cfg.Password)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: glog.Default.LogMode(glog.Silent)})
	if err != nil {
		t.Fatal("isolated PostgreSQL connection failed")
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer sqlDB.Close()
	require.NoError(t, db.AutoMigrate(&model.Vault{}))
	old, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(t.TempDir()))
	defer os.Chdir(old)
	mgr := NewBleveManager(util.Ptr(false), util.Ptr(false), zap.NewNop())
	d := New(db, context.Background(), WithConfig(cfg), WithUserDatabaseConfig(cfg), WithLogger(zap.NewNop()), WithBleveManager(mgr))
	repo := NewNoteRepository(d)
	ctx := context.Background()
	const uid int64 = 919
	// Force the real DAO to commit a PostgreSQL body reference, then fail content.txt.
	require.NoError(t, os.MkdirAll(filepath.Join(d.GetNoteFolderPath(uid, 1), "content.txt"), 0755))
	_, err = repo.Create(ctx, &domain.Note{VaultID: 1, Path: "postgres.md", PathHash: util.EncodeHash32("postgres.md"), Content: "Aa", ContentHash: util.EncodeHash32("Aa"), Size: 2, Action: domain.NoteActionCreate}, uid)
	require.Error(t, err)
	n, err := repo.GetByPathHash(ctx, util.EncodeHash32("postgres.md"), 1, uid)
	require.NoError(t, err)
	require.Equal(t, "Aa", n.Content)
	prior := n.UpdatedTimestamp
	n.Content = "BB"
	n.ContentHash = util.EncodeHash32("BB")
	n.Size = 2
	_, err = repo.Update(ctx, n, uid)
	require.Error(t, err)
	n, err = repo.GetByID(ctx, n.ID, uid)
	require.NoError(t, err)
	require.Equal(t, "BB", n.Content)
	require.Greater(t, n.UpdatedTimestamp, prior)
	n.Content = ""
	n.ContentHash = util.EncodeHash32("")
	n.Size = 0
	_, err = repo.Update(ctx, n, uid)
	require.Error(t, err)
	n, err = repo.GetByID(ctx, n.ID, uid)
	require.NoError(t, err)
	require.Empty(t, n.Content)
	// Unicode confirms the cross-platform UTF-16 hash metadata is preserved too.
	n.Content = "中文😀\n"
	n.ContentHash = util.EncodeHash32(n.Content)
	n.Size = int64(len(n.Content))
	_, err = repo.Update(ctx, n, uid)
	require.Error(t, err)
	n, err = repo.GetByID(ctx, n.ID, uid)
	require.NoError(t, err)
	require.Equal(t, "中文😀\n", n.Content)
	r := repo.(*noteRepository)
	require.NoError(t, os.WriteFile(filepath.Join(r.noteBlobFolder(uid), noteBodyDigest(n.Content)+".txt"), []byte("tampered"), 0644))
	_, err = repo.GetByID(ctx, n.ID, uid)
	require.ErrorContains(t, err, "verification failed")
	t.Log("real PostgreSQL Create/Update committed references; failed projection, collision, empty body, Unicode and tampered blob verified")
}
