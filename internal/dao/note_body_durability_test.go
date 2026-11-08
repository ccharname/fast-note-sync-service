package dao

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
)

// Exercise real SQLite commits and real failed filesystem renames, not a mocked repository.
func TestNoteBodyProjectionFailureKeepsCommittedBody(t *testing.T) {
	d, repo, _, cleanup := setupFTSTestEnv(t, false)
	defer cleanup()
	d.config.EnableWriteQueue = util.Ptr(false)
	d.userConfig.EnableWriteQueue = util.Ptr(false)
	ctx := context.Background()
	const uid int64 = 913
	makeNote := func(path, body string) *domain.Note {
		return &domain.Note{VaultID: 1, Path: path, PathHash: util.EncodeHash32(path), Content: body, ContentHash: util.EncodeHash32(body), Size: int64(len(body)), Action: domain.NoteActionCreate}
	}
	// Force Create to commit metadata and then fail its compatibility projection.
	folder := d.GetNoteFolderPath(uid, 1)
	require.NoError(t, os.MkdirAll(filepath.Join(folder, "content.txt"), 0755))
	_, err := repo.Create(ctx, makeNote("new.md", "Aa"), uid)
	require.Error(t, err)
	got, err := repo.GetByPathHash(ctx, util.EncodeHash32("new.md"), 1, uid)
	require.NoError(t, err)
	require.Equal(t, "Aa", got.Content)
	firstRevision := got.UpdatedTimestamp
	// Keep the obstructed projection: Update must still recover the newly committed
	// body, including a known hash32 collision, from its immutable strong reference.
	got.Content = "BB"
	got.ContentHash = util.EncodeHash32(got.Content)
	got.Size = 2
	_, err = repo.Update(ctx, got, uid)
	require.Error(t, err)
	got, err = repo.GetByID(ctx, got.ID, uid)
	require.NoError(t, err)
	require.Equal(t, "BB", got.Content)
	require.Greater(t, got.UpdatedTimestamp, firstRevision)
	// Empty bodies also have a real immutable blob; an absent projection is not used
	// as an invented empty value.
	got.Content = ""
	got.ContentHash = util.EncodeHash32("")
	got.Size = 0
	_, err = repo.Update(ctx, got, uid)
	require.Error(t, err)
	got, err = repo.GetByID(ctx, got.ID, uid)
	require.NoError(t, err)
	require.Empty(t, got.Content)
	// A body that disagrees with its strong reference must never become a read/ACK.
	concrete := repo.(*noteRepository)
	require.NoError(t, os.WriteFile(filepath.Join(concrete.noteBlobFolder(uid), noteBodyDigest("")+".txt"), []byte("corrupt"), 0644))
	_, err = repo.GetByID(ctx, got.ID, uid)
	require.ErrorContains(t, err, "verification failed")
}

func TestNoteBodyOldProjectionCannotOverrideNewMetadata(t *testing.T) {
	d, repo, _, cleanup := setupFTSTestEnv(t, false)
	defer cleanup()
	d.config.EnableWriteQueue = util.Ptr(false)
	d.userConfig.EnableWriteQueue = util.Ptr(false)
	ctx := context.Background()
	const uid int64 = 914
	n, err := repo.Create(ctx, &domain.Note{VaultID: 1, Path: "old.md", PathHash: util.EncodeHash32("old.md"), Content: "old", ContentHash: util.EncodeHash32("old"), Size: 3, Action: domain.NoteActionCreate}, uid)
	require.NoError(t, err)
	projection := filepath.Join(d.GetNoteFolderPath(uid, n.ID), "content.txt")
	require.NoError(t, os.Rename(projection, projection+".saved"))
	require.NoError(t, os.Mkdir(projection, 0755))
	n.Content = "new manuscript"
	n.ContentHash = util.EncodeHash32(n.Content)
	n.Size = int64(len(n.Content))
	_, err = repo.Update(ctx, n, uid)
	require.Error(t, err)
	// Restore the old body exactly as if the process stopped between metadata and rename.
	require.NoError(t, os.Remove(projection))
	require.NoError(t, os.Rename(projection+".saved", projection))
	n, err = repo.GetByID(ctx, n.ID, uid)
	require.NoError(t, err)
	require.Equal(t, "new manuscript", n.Content)
	require.Equal(t, util.EncodeHash32(n.Content), n.ContentHash)
	require.Equal(t, int64(len(n.Content)), n.Size)
}
