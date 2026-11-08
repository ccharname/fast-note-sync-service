package service

import (
	"context"
	"errors"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"testing"
)

type mutationFake struct {
	*reliableFake
	deletes, renames         int
	crashDelete, crashRename bool
}

func (f *mutationFake) Delete(_ context.Context, _ int64, p *dto.NoteDeleteRequest) (*dto.NoteDTO, error) {
	f.deletes++
	f.tick++
	n := *f.notes[p.Path]
	n.Action = "delete"
	n.UpdatedTimestamp = f.tick
	f.notes[p.Path] = &n
	if f.crashDelete {
		f.crashDelete = false
		return nil, errors.New("crash after delete commit")
	}
	return &n, nil
}
func (f *mutationFake) Rename(ctx context.Context, uid int64, p *dto.NoteRenameRequest) (*dto.NoteDTO, *dto.NoteDTO, error) {
	f.renames++
	f.tick++
	old := *f.notes[p.OldPath]
	old.Action = "delete"
	old.UpdatedTimestamp = f.tick
	f.notes[p.OldPath] = &old
	if f.crashRename {
		f.crashRename = false
		return nil, nil, errors.New("crash between rename commits")
	}
	_, next, e := f.ModifyOrCreate(ctx, uid, &dto.NoteModifyOrCreateRequest{Path: p.Path, PathHash: p.PathHash, Content: old.Content}, false)
	return &old, next, e
}
func mutationRequest(kind, id string) *ReliableMutationRequest {
	p := &ReliableMutationRequest{MergeProtocol: 1, RequestID: id, Kind: kind, Vault: "v", Path: "note.md", PathHash: util.EncodeHash32("note.md"), BaseRevision: "101"}
	if kind == "rename" {
		p.OldPath = "note.md"
		p.OldPathHash = p.PathHash
		p.Path = "renamed.md"
		p.PathHash = util.EncodeHash32(p.Path)
	}
	return p
}
func mutationEnv(t *testing.T) *mutationFake {
	f := &mutationFake{reliableFake: reliableTestEnv(t)}
	applyTest(t, f.reliableFake, reliableReq("mutation-seed", "", "manuscript", false))
	return f
}
func TestReliableDeleteReplayNeverDeletesRecreation(t *testing.T) {
	f := mutationEnv(t)
	p := mutationRequest("delete", "mutation-delete")
	r, e := ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || r.Replay {
		t.Fatal(r, e)
	}
	f.notes["note.md"].Action = "create"
	f.notes["note.md"].Content = "recreated"
	f.notes["note.md"].UpdatedTimestamp++
	r, e = ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || !r.Replay || f.deletes != 1 || f.notes["note.md"].Content != "recreated" {
		t.Fatal("replay deleted new path", r, e)
	}
}
func TestReliableDeleteInterruptedThenNewWriterIsSuperseded(t *testing.T) {
	f := mutationEnv(t)
	f.crashDelete = true
	p := mutationRequest("delete", "mutation-delete-crash")
	if _, e := ApplyReliableMutation(context.Background(), f, 1, p); e == nil {
		t.Fatal("crash acknowledged")
	}
	f.notes["note.md"].Action = "create"
	f.notes["note.md"].Content = "recreated"
	f.notes["note.md"].UpdatedTimestamp++
	r, e := ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || !r.Superseded || f.deletes != 1 || f.notes["note.md"].Content != "recreated" {
		t.Fatal("interrupted delete touched new writer", r, e)
	}
}
func TestReliableRenameInterruptedBetweenPathsRecoversManuscript(t *testing.T) {
	f := mutationEnv(t)
	f.crashDelete = true
	p := mutationRequest("rename", "mutation-rename-crash")
	if _, e := ApplyReliableMutation(context.Background(), f, 1, p); e == nil {
		t.Fatal("crash acknowledged")
	}
	r, e := ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || r.Superseded || r.NewNote == nil || r.NewNote.Content != "manuscript" || f.deletes != 1 {
		t.Fatal("rename failed to recover saved manuscript", r, e)
	}
	r, e = ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || !r.Replay || f.deletes != 1 {
		t.Fatal("rename executed twice", r, e)
	}
}
func TestReliableRenameInterruptedNeverOverwritesNewTarget(t *testing.T) {
	f := mutationEnv(t)
	f.crashDelete = true
	p := mutationRequest("rename", "mutation-rename-occupied")
	if _, e := ApplyReliableMutation(context.Background(), f, 1, p); e == nil {
		t.Fatal("crash acknowledged")
	}
	f.notes[p.Path] = &dto.NoteDTO{Path: p.Path, PathHash: p.PathHash, Content: "occupied newer target", Action: "create", UpdatedTimestamp: 999}
	r, e := ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || !r.Superseded || f.notes[p.Path].Content != "occupied newer target" {
		t.Fatal("overwrote occupied target", r, e)
	}
}
func TestReliableMutationRejectsReusedIdentity(t *testing.T) {
	f := mutationEnv(t)
	p := mutationRequest("delete", "mutation-fixed-identity")
	if _, e := ApplyReliableMutation(context.Background(), f, 1, p); e != nil {
		t.Fatal(e)
	}
	p.Path = "other.md"
	p.PathHash = util.EncodeHash32(p.Path)
	if _, e := ApplyReliableMutation(context.Background(), f, 1, p); e == nil {
		t.Fatal("reused mutation identity accepted")
	}
}

func TestReliableMutationFirstArrivalChecksConfirmedRevision(t *testing.T) {
	f := mutationEnv(t)
	p := mutationRequest("delete", "mutation-stale-first-arrival")
	p.BaseRevision = "100"
	r, e := ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || !r.Superseded || f.deletes != 0 || f.notes["note.md"].Action == "delete" {
		t.Fatal("stale first request deleted unseen writer", r, e)
	}
}

func TestReliableRenameInterruptedBeforeTargetNeverResurrectsExternalDelete(t *testing.T) {
	f := mutationEnv(t)
	p := mutationRequest("rename", "mutation-external-delete")
	f.fail = true
	if _, e := ApplyReliableMutation(context.Background(), f, 1, p); e == nil {
		t.Fatal("target failure acknowledged")
	}
	f.fail = false
	f.notes["note.md"].Action = "delete"
	f.notes["note.md"].UpdatedTimestamp++
	r, e := ApplyReliableMutation(context.Background(), f, 1, p)
	if e != nil || !r.Superseded || f.notes[p.Path] != nil {
		t.Fatal("interrupted rename resurrected a later deletion", r, e)
	}
}
