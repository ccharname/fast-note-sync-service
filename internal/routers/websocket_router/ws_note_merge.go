package websocket_router

import (
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/service"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
)

// lucky-v2: serverMerge request handling. The service layer calls emit INSIDE the per-note
// lock; everything sent here goes through the per-connection ordered queues so the order on
// each connection equals the write order.
// lucky-v2：serverMerge 请求处理。emit 在笔记锁内被调用，消息进入每连接有序队列。

func modifyPush(d *dto.NoteDTO) dto.NoteSyncModifyMessage {
	return dto.NoteSyncModifyMessage{
		Path: d.Path, PathHash: d.PathHash, Content: d.Content, ContentHash: d.ContentHash,
		Ctime: d.Ctime, Mtime: d.Mtime, UpdatedTimestamp: d.UpdatedTimestamp,
		Rev: d.UpdatedTimestamp, Sha: service.ContentSha(d.Content),
	}
}

// tryServerMergeModify returns true if the request was handled by the serverMerge path.
func (h *NoteWSHandler) tryServerMergeModify(c *pkgapp.WebsocketClient, noteSvc service.NoteService, params *dto.NoteModifyOrCreateRequest) bool {
	sm, ok := noteSvc.(service.NoteServerMerge)
	if !ok || !params.ServerMerge {
		return false
	}
	err := sm.ServerMergeModify(c.Context(), c.User.UID, params, func(r *service.MergeResult) {
		c.ToResponseOrdered(code.Success.WithData(dto.NoteModifyAckMessage{
			LastTime: r.LastTime, Path: r.Path, PathHash: r.PathHash, NoteAckExt: r.Ack,
		}).WithVault(params.Vault).WithContext(params.Context), string(NoteModifyAck))
		if r.Written != nil {
			c.BroadcastResponseOrdered(code.Success.WithData(modifyPush(r.Written)).WithVault(params.Vault), true, string(NoteSyncModify))
		}
		if r.Copy != nil {
			// submitter gets the copy with its request context, other devices with none
			c.ToResponseOrdered(code.Success.WithData(modifyPush(r.Copy)).WithVault(params.Vault).WithContext(params.Context), string(NoteSyncModify))
			c.BroadcastResponseOrdered(code.Success.WithData(modifyPush(r.Copy)).WithVault(params.Vault), true, string(NoteSyncModify))
		}
	})
	if err != nil {
		h.respondError(c, code.ErrorNoteModifyOrCreateFailed, err, "websocket_router.note.NoteModify.ServerMerge")
	}
	return true
}

func deletePush(d *dto.NoteDTO) dto.NoteSyncDeleteMessage {
	return dto.NoteSyncDeleteMessage{
		Path: d.Path, PathHash: d.PathHash, Ctime: d.Ctime, Mtime: d.Mtime, Size: d.Size,
		UpdatedTimestamp: d.UpdatedTimestamp, Rev: d.UpdatedTimestamp,
	}
}

func (h *NoteWSHandler) tryServerMergeDelete(c *pkgapp.WebsocketClient, noteSvc service.NoteService, params *dto.NoteDeleteRequest) bool {
	sm, ok := noteSvc.(service.NoteServerMerge)
	if !ok || !params.ServerMerge {
		return false
	}
	err := sm.ServerMergeDelete(c.Context(), c.User.UID, params, func(r *service.MergeResult) {
		c.ToResponseOrdered(code.Success.WithData(dto.NoteDeleteAckMessage{
			LastTime: r.LastTime, Path: r.Path, PathHash: r.PathHash, NoteAckExt: r.Ack,
		}).WithVault(params.Vault).WithContext(params.Context), string(NoteDeleteAck))
		if r.Deleted != nil {
			c.BroadcastResponseOrdered(code.Success.WithData(deletePush(r.Deleted)).WithVault(params.Vault), true, string(NoteSyncDelete))
		}
	})
	if err != nil {
		h.respondError(c, code.ErrorNoteDeleteFailed, err, "websocket_router.note.NoteDelete.ServerMerge")
	}
	return true
}

func (h *NoteWSHandler) tryServerMergeRename(c *pkgapp.WebsocketClient, noteSvc service.NoteService, params *dto.NoteRenameRequest) bool {
	sm, ok := noteSvc.(service.NoteServerMerge)
	if !ok || !params.ServerMerge {
		return false
	}
	err := sm.ServerMergeRename(c.Context(), c.User.UID, params, func(r *service.MergeResult) {
		c.ToResponseOrdered(code.Success.WithData(dto.NoteRenameAckMessage{
			LastTime: r.LastTime, Path: r.Path, PathHash: r.PathHash, NoteAckExt: r.Ack,
		}).WithVault(params.Vault).WithContext(params.Context), string(NoteRenameAck))
		if r.RenamedNew != nil {
			n, o := r.RenamedNew, r.RenamedOld
			c.BroadcastResponseOrdered(code.Success.WithData(dto.NoteSyncRenameMessage{
				Path: n.Path, PathHash: n.PathHash, ContentHash: n.ContentHash,
				Ctime: n.Ctime, Mtime: n.Mtime, Size: n.Size, UpdatedTimestamp: n.UpdatedTimestamp,
				OldPath: o.Path, OldPathHash: o.PathHash,
				Rev: n.UpdatedTimestamp, Sha: service.ContentSha(n.Content),
			}).WithVault(params.Vault), true, string(NoteSyncRename))
		} else if r.Deleted != nil {
			c.BroadcastResponseOrdered(code.Success.WithData(deletePush(r.Deleted)).WithVault(params.Vault), true, string(NoteSyncDelete))
		}
	})
	if err != nil {
		h.respondError(c, code.ErrorRenameNoteTargetExist, err, "websocket_router.note.NoteRename.ServerMerge")
	}
	return true
}

// recoveryBroadcast pushes a crash roll-forward result to every connection of the user.
func recoveryBroadcast(wss *pkgapp.WebsocketServer, uid int64, ev *service.MergeResult) {
	push := func(data any, action string) {
		wss.BroadcastToUserOrdered(uid, code.Success.WithData(data).WithVault(ev.Vault), action)
	}
	if ev.Written != nil {
		push(modifyPush(ev.Written), string(NoteSyncModify))
	}
	if ev.Copy != nil {
		push(modifyPush(ev.Copy), string(NoteSyncModify))
	}
	if ev.RenamedNew != nil && ev.RenamedOld != nil {
		n, o := ev.RenamedNew, ev.RenamedOld
		push(dto.NoteSyncRenameMessage{
			Path: n.Path, PathHash: n.PathHash, ContentHash: n.ContentHash,
			Ctime: n.Ctime, Mtime: n.Mtime, Size: n.Size, UpdatedTimestamp: n.UpdatedTimestamp,
			OldPath: o.Path, OldPathHash: o.PathHash,
			Rev: n.UpdatedTimestamp, Sha: service.ContentSha(n.Content),
		}, string(NoteSyncRename))
	} else if ev.Deleted != nil {
		push(deletePush(ev.Deleted), string(NoteSyncDelete))
	}
}
