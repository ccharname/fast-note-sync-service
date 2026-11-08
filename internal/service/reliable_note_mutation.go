package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

// ReliableMutationRequest identifies one immutable delete/rename intent.
type ReliableMutationRequest struct {
	MergeProtocol                                                              int
	RequestID, Kind, Vault, Path, PathHash, OldPath, OldPathHash, BaseRevision string
}
type ReliableMutationResult struct {
	MergeProtocol int          `json:"mergeProtocol"`
	RequestID     string       `json:"requestId"`
	Kind          string       `json:"kind"`
	Path          string       `json:"path"`
	PathHash      string       `json:"pathHash"`
	OldPath       string       `json:"oldPath,omitempty"`
	Revision      string       `json:"revision"`
	OldRevision   string       `json:"oldRevision,omitempty"`
	LastTime      int64        `json:"lastTime"`
	Superseded    bool         `json:"superseded,omitempty"`
	Replay        bool         `json:"-"`
	OldNote       *dto.NoteDTO `json:"-"`
	NewNote       *dto.NoteDTO `json:"-"`
}
type mutationSnapshot struct {
	Path, Content, Action  string
	Revision, Ctime, Mtime int64
}
type mutationIntent struct {
	Fingerprint  string                  `json:"fingerprint"`
	Before       *mutationSnapshot       `json:"before"`
	TargetBefore *mutationSnapshot       `json:"targetBefore,omitempty"`
	Result       *ReliableMutationResult `json:"result,omitempty"`
}

func mutationSnap(n *dto.NoteDTO) *mutationSnapshot {
	if n == nil {
		return nil
	}
	return &mutationSnapshot{Path: n.Path, Content: n.Content, Action: n.Action, Revision: n.UpdatedTimestamp, Ctime: n.Ctime, Mtime: n.Mtime}
}
func mutationMatches(n *dto.NoteDTO, b *mutationSnapshot) bool {
	if n == nil || b == nil {
		return n == nil && b == nil
	}
	return n.Path == b.Path && n.Content == b.Content && n.Action == b.Action && n.UpdatedTimestamp == b.Revision
}

// ApplyReliableMutation journals before changing either path. A repeated request can
// finish an interrupted rename, but cannot delete a later recreation of the old path.
func ApplyReliableMutation(ctx context.Context, svc NoteService, uid int64, p *ReliableMutationRequest) (*ReliableMutationResult, error) {
	if p.MergeProtocol != 1 || len(p.RequestID) < 8 || len(p.RequestID) > 160 || (p.Kind != "delete" && p.Kind != "rename") {
		return nil, fmt.Errorf("invalid reliable mutation")
	}
	if revision, err := strconv.ParseInt(p.BaseRevision, 10, 64); err != nil || revision < 0 {
		return nil, fmt.Errorf("invalid mutation baseRevision")
	}
	if p.PathHash != util.EncodeHash32(p.Path) || (p.Kind == "rename" && (p.OldPath == p.Path || p.OldPathHash != util.EncodeHash32(p.OldPath))) {
		return nil, fmt.Errorf("mutation path hash mismatch")
	}
	sourcePath, sourceHash := p.Path, p.PathHash
	if p.Kind == "rename" {
		sourcePath, sourceHash = p.OldPath, p.OldPathHash
	}
	ctx, unlock := LockNote(ctx, uid, p.Vault, sourceHash, p.PathHash)
	defer unlock()
	get := func(path, hash string) (*dto.NoteDTO, error) {
		return currentReliableNote(ctx, svc, uid, &dto.NoteModifyOrCreateRequest{Vault: p.Vault, Path: path, PathHash: hash})
	}
	source, err := get(sourcePath, sourceHash)
	if err != nil {
		return nil, err
	}
	target := source
	if p.Kind == "rename" {
		target, err = get(p.Path, p.PathHash)
		if err != nil {
			return nil, err
		}
	}
	if source != nil && source.Path != sourcePath || target != nil && target.Path != p.Path {
		return nil, fmt.Errorf("mutation path hash collision")
	}
	encoded, _ := json.Marshal(p)
	fp := NoteDigest(string(encoded))
	receipt := filepath.Join("storage", "merge-receipts", strconv.FormatInt(uid, 10), NoteDigest(p.Vault), "mutations", NoteDigest(p.RequestID)+".json")
	intent := &mutationIntent{}
	data, e := os.ReadFile(receipt)
	if e == nil {
		if err = json.Unmarshal(data, intent); err != nil {
			return nil, err
		}
		if intent.Fingerprint != fp {
			return nil, fmt.Errorf("mutation requestId reused")
		}
		if intent.Result != nil {
			intent.Result.Replay = true
			return intent.Result, nil
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	} else {
		intent.Fingerprint = fp
		intent.Before = mutationSnap(source)
		if p.Kind == "rename" {
			intent.TargetBefore = mutationSnap(target)
		}
		if err = atomicMergeJSON(receipt, intent); err != nil {
			return nil, err
		}
	}
	result := &ReliableMutationResult{MergeProtocol: 1, RequestID: p.RequestID, Kind: p.Kind, Path: p.Path, PathHash: p.PathHash, OldPath: p.OldPath, Revision: "0"}
	if intent.Before != nil && NoteRevision(intent.Before.Revision) != p.BaseRevision {
		// A first request can itself arrive late, before any intent exists. Its
		// source precondition prevents deleting a manuscript the client never saw.
		result.Superseded = true
		result.NewNote = target
		if source != nil {
			result.OldRevision = NoteRevision(source.UpdatedTimestamp)
		}
		if target != nil {
			result.Revision = NoteRevision(target.UpdatedTimestamp)
			result.LastTime = target.UpdatedTimestamp
		}
	} else if p.Kind == "delete" {
		switch {
		case source == nil: // Nothing remains at this path; the intent must not recreate it.
		case source.Action == "delete":
		case !mutationMatches(source, intent.Before):
			result.Superseded = true
		default:
			source, err = svc.Delete(ctx, uid, &dto.NoteDeleteRequest{Vault: p.Vault, Path: p.Path, PathHash: p.PathHash})
			if err != nil {
				return nil, err
			}
		}
		result.NewNote = source
		if source != nil {
			result.Revision = NoteRevision(source.UpdatedTimestamp)
			result.LastTime = source.UpdatedTimestamp
		}
	} else {
		// A rename initially targeting an existing live path is not allowed to overwrite it.
		if intent.Before == nil || intent.Before.Action == "delete" || (intent.TargetBefore != nil && intent.TargetBefore.Action != "delete") {
			result.Superseded = true
		} else if source != nil && source.Action != "delete" && !mutationMatches(source, intent.Before) {
			result.Superseded = true // A later recreation/edit is a different manuscript.
		} else if source != nil && source.Action != "delete" {
			if target != nil && target.Action != "delete" && target.Content != intent.Before.Content {
				result.Superseded = true
			} else {
				// Commit the complete target before deleting the exact source. A crash
				// between these commits leaves two copies, never zero copies.
				if target == nil || target.Action == "delete" {
					if !mutationMatches(target, intent.TargetBefore) {
						result.Superseded = true
					} else {
						_, target, err = svc.ModifyOrCreate(ctx, uid, &dto.NoteModifyOrCreateRequest{Vault: p.Vault, Path: p.Path, PathHash: p.PathHash, Content: intent.Before.Content, ContentHash: util.EncodeHash32(intent.Before.Content), Ctime: intent.Before.Ctime, Mtime: intent.Before.Mtime, CreateOnly: true}, false)
						if err != nil {
							return nil, err
						}
					}
				}
				if !result.Superseded {
					if finalizer, ok := svc.(interface {
						FinalizeReliableRename(context.Context, int64, *dto.NoteDTO, *dto.NoteDTO) error
					}); ok {
						err = finalizer.FinalizeReliableRename(ctx, uid, source, target)
					} else {
						source, err = svc.Delete(ctx, uid, &dto.NoteDeleteRequest{Vault: p.Vault, Path: p.OldPath, PathHash: p.OldPathHash})
					}
				}
				if err != nil {
					return nil, err
				}
			}
		} else if target == nil || target.Action == "delete" {
			// A later deletion is authoritative. Never infer ownership of its tombstone
			// and resurrect a missing target. The original remains in the intent.
			result.Superseded = true
		} else if target.Content != intent.Before.Content {
			result.Superseded = true // Preserve both the occupied target and the saved intent.
		}
		result.OldNote = source
		result.NewNote = target
		if source != nil {
			result.OldRevision = NoteRevision(source.UpdatedTimestamp)
		}
		if target != nil {
			result.Revision = NoteRevision(target.UpdatedTimestamp)
			result.LastTime = target.UpdatedTimestamp
		}
	}
	if !result.Superseded {
		if p.Kind == "rename" && result.OldNote != nil && result.NewNote != nil {
			if finalizer, ok := svc.(interface {
				FinalizeReliableRename(context.Context, int64, *dto.NoteDTO, *dto.NoteDTO) error
			}); ok {
				if err = finalizer.FinalizeReliableRename(ctx, uid, result.OldNote, result.NewNote); err != nil {
					return nil, err
				}
			}
		}
		// Never acknowledge an in-memory DTO before checking the stored bodies.
		verifiedSource, e := get(sourcePath, sourceHash)
		if e != nil {
			return nil, e
		}
		verifiedTarget := verifiedSource
		if p.Kind == "rename" {
			verifiedTarget, e = get(p.Path, p.PathHash)
			if e != nil {
				return nil, e
			}
		}
		for _, n := range []*dto.NoteDTO{verifiedSource, verifiedTarget} {
			if n != nil && (n.ContentHash != util.EncodeHash32(n.Content) || n.Size != int64(len(n.Content))) {
				return nil, fmt.Errorf("mutation body verification failed")
			}
		}
		if verifiedSource != nil && verifiedSource.Action != "delete" {
			return nil, fmt.Errorf("mutation source was not deleted")
		}
		if p.Kind == "rename" && (verifiedTarget == nil || verifiedTarget.Action == "delete" || verifiedTarget.Content != intent.Before.Content) {
			return nil, fmt.Errorf("mutation target verification failed")
		}
		result.NewNote = verifiedTarget
		if verifiedTarget != nil {
			result.Revision = NoteRevision(verifiedTarget.UpdatedTimestamp)
			result.LastTime = verifiedTarget.UpdatedTimestamp
		} else {
			result.Revision = p.BaseRevision
		}
		if p.Kind == "rename" {
			result.OldNote = verifiedSource
			result.OldRevision = p.BaseRevision
			if verifiedSource != nil {
				result.OldRevision = NoteRevision(verifiedSource.UpdatedTimestamp)
				if verifiedSource.UpdatedTimestamp > result.LastTime {
					result.LastTime = verifiedSource.UpdatedTimestamp
				}
			}
		}
	}
	intent.Result = result
	if err = atomicMergeJSON(receipt, intent); err != nil {
		return nil, err
	}
	return result, nil
}
