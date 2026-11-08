package websocket_router

import (
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	v1 "github.com/haierkeys/fast-note-sync-service/internal/proto/v1"
	"google.golang.org/protobuf/proto"
)

// lucky-v2: the new optional fields survive protobuf encode/decode, and requests that do not
// set them decode to zero values (original clients).
func TestProtobufV2Fields(t *testing.T) {
	req, _ := proto.Marshal(&v1.NoteModifyOrCreateRequest{Vault: "v", Path: "p", ServerMerge: true, OpId: "o", BaseSha: "s"})
	var m dto.NoteModifyOrCreateRequest
	if ok, err := DeReceiveProtobufToDTO(NoteReceiveModify, req, &m); !ok || err != nil || !m.ServerMerge || m.OpID != "o" || m.BaseSha != "s" {
		t.Fatalf("modify decode: %v %v %+v", ok, err, m)
	}
	old, _ := proto.Marshal(&v1.NoteModifyOrCreateRequest{Vault: "v", Path: "p", BaseHash: "1"})
	var m2 dto.NoteModifyOrCreateRequest
	if ok, _ := DeReceiveProtobufToDTO(NoteReceiveModify, old, &m2); !ok || m2.ServerMerge || m2.OpID != "" || m2.BaseSha != "" {
		t.Fatalf("old modify decode: %+v", m2)
	}
	del, _ := proto.Marshal(&v1.NoteDeleteRequest{Vault: "v", Path: "p", BaseSha: "s", OpId: "o", ServerMerge: true})
	var d dto.NoteDeleteRequest
	if ok, _ := DeReceiveProtobufToDTO(NoteReceiveDelete, del, &d); !ok || !d.ServerMerge || d.OpID != "o" || d.BaseSha != "s" {
		t.Fatalf("delete decode: %+v", d)
	}
	ren, _ := proto.Marshal(&v1.NoteRenameRequest{Vault: "v", Path: "p", OldPath: "q", BaseSha: "s", OpId: "o", ServerMerge: true})
	var r dto.NoteRenameRequest
	if ok, _ := DeReceiveProtobufToDTO(NoteReceiveRename, ren, &r); !ok || !r.ServerMerge || r.OpID != "o" || r.BaseSha != "s" {
		t.Fatalf("rename decode: %+v", r)
	}

	b, err := enSendDataPayload(NoteModifyAck, dto.NoteModifyAckMessage{LastTime: 9, Path: "p", PathHash: "h",
		NoteAckExt: dto.NoteAckExt{OpID: "o", Rev: 9, Sha: "s", Merged: true, Content: "c", ConflictCopy: "x", Rejected: true, RejectReason: "r"}})
	if err != nil {
		t.Fatal(err)
	}
	var ack v1.NoteModifyAckMessage
	if err := proto.Unmarshal(b, &ack); err != nil || ack.OpId != "o" || ack.Rev != 9 || ack.Sha != "s" || !ack.Merged || ack.Content != "c" || ack.ConflictCopy != "x" || !ack.Rejected || ack.RejectReason != "r" || ack.LastTime != 9 {
		t.Fatalf("ack: %v %+v", err, &ack)
	}
	b, _ = enSendDataPayload(NoteSyncModify, dto.NoteSyncModifyMessage{Path: "p", Rev: 5, Sha: "z", UpdatedTimestamp: 5})
	var pm v1.NoteSyncModifyMessage
	if err := proto.Unmarshal(b, &pm); err != nil || pm.Rev != 5 || pm.Sha != "z" || pm.LastTime != 5 {
		t.Fatalf("push: %v %+v", err, &pm)
	}
}
