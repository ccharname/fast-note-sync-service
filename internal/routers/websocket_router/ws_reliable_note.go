package websocket_router

import (
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/service"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

// The capability request is authenticated by the same interceptor as all note actions.
// Clients must negotiate this before sending a manuscript to a server they do not recognize.
func (h *NoteWSHandler) MergeCapabilities(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage) {
	c.ToResponse(code.Success.WithData(map[string]interface{}{"mergeProtocol": 1, "baseContent": true, "requestId": true}), "MergeCapabilities")
}

func (h *NoteWSHandler) reliableNoteModify(c *pkgapp.WebsocketClient, p *dto.NoteModifyOrCreateRequest) {
	ctx := c.Context()
	if _, err := h.App.VaultService.GetOrCreate(ctx, c.User.UID, p.Vault); err != nil {
		h.respondError(c, code.ErrorNoteModifyOrCreateFailed, err, "reliableNote.vault")
		return
	}
	// Keep broadcasts inside the same shared write lock so write and publication order agree.
	ctx, unlock := service.LockNote(ctx, c.User.UID, p.Vault, p.PathHash, util.EncodeHash32(service.ReliableConflictPath(p)))
	defer unlock()
	svc := h.App.GetNoteService(c.ClientType(), c.ClientName(), c.ClientVersion())
	result, conflict, err := service.ApplyReliableNote(ctx, svc, c.User.UID, p)
	if err != nil {
		h.respondError(c, code.ErrorNoteModifyOrCreateFailed, err, "reliableNote.modify")
		return
	}
	c.ToResponse(code.Success.WithData(result).WithVault(p.Vault).WithContext(p.Context), string(NoteModifyAck))
	if result.Replay {
		return
	}
	if !result.Deleted {
		c.BroadcastResponse(code.Success.WithData(dto.NoteSyncModifyMessage{
			Path: result.Path, PathHash: result.PathHash, Content: result.Content, ContentHash: result.ContentHash,
			Ctime: result.Ctime, Mtime: result.Mtime, UpdatedTimestamp: result.LastTime,
			MergeProtocol: 1, Revision: result.Revision, ContentDigest: result.ContentDigest,
		}).WithVault(p.Vault), false, NoteSyncModify)
	}
	if conflict != nil {
		c.BroadcastResponse(code.Success.WithData(dto.NoteSyncModifyMessage{
			Path: conflict.Path, PathHash: conflict.PathHash, Content: conflict.Content, ContentHash: conflict.ContentHash,
			Ctime: conflict.Ctime, Mtime: conflict.Mtime, UpdatedTimestamp: conflict.UpdatedTimestamp,
			MergeProtocol: 1, Revision: service.NoteRevision(conflict.UpdatedTimestamp), ContentDigest: service.NoteDigest(conflict.Content),
		}).WithVault(p.Vault), false, NoteSyncModify)
	}
}
