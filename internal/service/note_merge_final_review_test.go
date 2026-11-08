package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
)

// Fail once before the repository commit, then let the real repository perform
// the immediate roll-forward. This exercises ordinary errors rather than errCrash.
type finalReviewTransientRepo struct {
	domain.NoteRepository
	createErr error
	deleteErr error
}

func (r *finalReviewTransientRepo) Create(ctx context.Context, note *domain.Note, uid int64) (*domain.Note, error) {
	if err := r.createErr; err != nil {
		r.createErr = nil
		return nil, err
	}
	return r.NoteRepository.Create(ctx, note, uid)
}

func (r *finalReviewTransientRepo) UpdateDelete(ctx context.Context, note *domain.Note, uid int64) error {
	if err := r.deleteErr; err != nil {
		r.deleteErr = nil
		return err
	}
	return r.NoteRepository.UpdateDelete(ctx, note, uid)
}

func TestFinalReview_InlineRecoveryBroadcasts(t *testing.T) {
	for _, kind := range []string{"modify", "create", "conflict-before-write", "conflict-after-write", "delete", "rename", "rename-identical-target"} {
		t.Run(kind, func(t *testing.T) {
			s := newMergeEnv(t)
			restart(t)
			defer restart(t)
			ctx := context.Background()
			if kind != "create" {
				modify(t, s, mreq("n.md", "server\n", "<none>", "seed"))
			}
			if kind == "rename-identical-target" {
				modify(t, s, mreq("moved.md", "server\n", "<none>", "target-seed"))
			}
			transient := errors.New("transient write failure")
			stage := "write"
			switch kind {
			case "conflict-before-write":
				s.noteRepo = &finalReviewTransientRepo{NoteRepository: s.noteRepo, createErr: transient}
				stage = ""
			case "conflict-after-write":
				stage = "after_write"
			case "rename-identical-target":
				s.noteRepo = &finalReviewTransientRepo{NoteRepository: s.noteRepo, deleteErr: transient}
				stage = ""
			}
			failpoint = func(at string) error {
				if at == stage {
					return transient
				}
				return nil
			}
			var recovered []*MergeResult
			previousBroadcast := RecoveryBroadcast
			RecoveryBroadcast = func(uid int64, ev *MergeResult) {
				require.Equal(t, tuid, uid)
				require.Equal(t, "v", ev.Vault)
				recovered = append(recovered, ev)
			}
			defer func() { RecoveryBroadcast = previousBroadcast }()
			var invoke func(func(*MergeResult)) error
			switch kind {
			case "delete":
				req := &dto.NoteDeleteRequest{Vault: "v", Path: "n.md", PathHash: util.EncodeHash32("n.md"), BaseSha: sha256Hex("server\n"), OpID: "inline-delete", ServerMerge: true}
				invoke = func(emit func(*MergeResult)) error { return s.ServerMergeDelete(ctx, tuid, req, emit) }
			case "rename", "rename-identical-target":
				req := &dto.NoteRenameRequest{Vault: "v", OldPath: "n.md", OldPathHash: util.EncodeHash32("n.md"), Path: "moved.md", PathHash: util.EncodeHash32("moved.md"), BaseSha: sha256Hex("server\n"), OpID: "inline-rename", ServerMerge: true}
				invoke = func(emit func(*MergeResult)) error { return s.ServerMergeRename(ctx, tuid, req, emit) }
			default:
				req := mreq("n.md", "client\n", "server\n", "inline-modify")
				if kind == "create" || kind == "conflict-before-write" || kind == "conflict-after-write" {
					req.BaseSha, req.BaseHash = "", ""
				}
				invoke = func(emit func(*MergeResult)) error { return s.ServerMergeModify(ctx, tuid, req, emit) }
			}
			require.Error(t, invoke(func(*MergeResult) { t.Fatal("error path must not emit a successful ACK") }))
			failpoint = nil
			require.Zero(t, pendingCount(), "inline recovery must finish the durable intent")
			// Recovery committed before the request returned its error. Online peers
			// need the event now; no pending intent remains to broadcast on retry.
			require.Len(t, recovered, 1, "committed inline recovery must notify online peers")
			ev := recovered[0]
			require.Equal(t, tuid, ev.UID)
			require.Equal(t, util.EncodeHash32(ev.Path), ev.PathHash)
			var pushed *dto.NoteDTO
			switch kind {
			case "modify", "create":
				require.NotNil(t, ev.Written)
				pushed = ev.Written
				require.Equal(t, "client\n", ev.Written.Content)
				require.Equal(t, "client\n", current(t, s, "n.md").Content)
			case "conflict-before-write", "conflict-after-write":
				require.NotNil(t, ev.Copy)
				pushed = ev.Copy
				require.Equal(t, "client\n", ev.Copy.Content)
				require.Equal(t, "client\n", current(t, s, ev.Copy.Path).Content)
				require.Equal(t, "server\n", current(t, s, "n.md").Content)
			case "delete", "rename-identical-target":
				require.NotNil(t, ev.Deleted)
				pushed = ev.Deleted
				require.Equal(t, domain.NoteActionDelete, current(t, s, "n.md").Action)
			case "rename":
				require.NotNil(t, ev.RenamedOld)
				require.NotNil(t, ev.RenamedNew)
				pushed = ev.RenamedNew
				require.Equal(t, "n.md", ev.RenamedOld.Path)
				require.Equal(t, util.EncodeHash32("n.md"), ev.RenamedOld.PathHash)
				require.Equal(t, "moved.md", ev.RenamedNew.Path)
				require.Equal(t, "server\n", current(t, s, "moved.md").Content)
			}
			persisted := current(t, s, pushed.Path)
			require.Equal(t, pushed.Path, ev.Path)
			require.Equal(t, pushed.PathHash, ev.PathHash)
			require.Positive(t, ev.LastTime)
			require.Equal(t, persisted.UpdatedTimestamp, ev.LastTime)
			require.Equal(t, persisted.UpdatedTimestamp, pushed.UpdatedTimestamp)
			require.Equal(t, persisted.ContentHash, pushed.ContentHash)
			require.Equal(t, sha256Hex(persisted.Content), ContentSha(pushed.Content), "broadcast body must identify the committed bytes")
			require.NoError(t, invoke(func(r *MergeResult) {
				require.Nil(t, r.Written)
				require.Nil(t, r.Copy)
				require.Nil(t, r.Deleted)
				require.Nil(t, r.RenamedNew)
			}))
			require.Len(t, recovered, 1, "retry must not rebroadcast a completed recovery")
		})
	}
}

func TestFinalReview_ConflictReplayBodyMatchesSHA(t *testing.T) {
	for _, snapshot := range []string{"retained", "missing", "corrupt", "retained-empty", "missing-empty"} {
		t.Run(snapshot, func(t *testing.T) {
			s := newMergeEnv(t)
			restart(t)
			defer restart(t)
			base, historical, latest := "base\n", "server\n", "new server\n"
			if snapshot == "retained-empty" || snapshot == "missing-empty" {
				historical = ""
			}
			modify(t, s, mreq("n.md", base, "<none>", "seed"))
			modify(t, s, mreq("n.md", historical, base, "server-edit"))
			req := mreq("n.md", "client\n", base, "lost-conflict-ack")
			first := modify(t, s, req)
			require.NotEmpty(t, first.Ack.ConflictCopy)
			require.True(t, first.Ack.Merged)
			modify(t, s, mreq("n.md", latest, historical, "later-server-edit"))
			now := current(t, s, "n.md")
			path := filepath.Join(snapDir(tuid, now.ID), first.Ack.Sha+".txt")
			switch snapshot {
			case "missing", "missing-empty":
				require.NoError(t, os.Remove(path))
			case "corrupt":
				require.NoError(t, os.WriteFile(path, []byte("wrong snapshot bytes"), 0644))
			}
			replay := modify(t, s, req)
			require.True(t, replay.Ack.Merged)
			require.Equal(t, replay.Ack.Sha, sha256Hex(replay.Ack.Content), "never pair a historical nonempty SHA with omitted/empty content")
			if snapshot == "retained" || snapshot == "retained-empty" {
				require.False(t, replay.Ack.Rejected)
				require.Equal(t, first.Ack, replay.Ack)
			} else {
				require.True(t, replay.Ack.Rejected)
				require.Equal(t, "needResync", replay.Ack.RejectReason)
				require.Equal(t, latest, replay.Ack.Content)
				require.Equal(t, now.UpdatedTimestamp, replay.Ack.Rev)
			}
			require.Nil(t, replay.Copy)
			require.Nil(t, replay.Written)
			require.Equal(t, latest, current(t, s, "n.md").Content)
			require.Equal(t, "client\n", current(t, s, first.Ack.ConflictCopy).Content)
		})
	}
}
