package dto

import (
	"encoding/json"
	"testing"
)

// lucky-v2: Acks of requests without serverMerge must stay byte-identical to upstream
// (only lastTime/path/pathHash); the extension fields appear only when filled.
func TestAckWireCompat(t *testing.T) {
	b, _ := json.Marshal(NoteModifyAckMessage{LastTime: 5, Path: "a.md", PathHash: "h"})
	if string(b) != `{"lastTime":5,"path":"a.md","pathHash":"h"}` {
		t.Fatalf("upstream ack changed: %s", b)
	}
	b, _ = json.Marshal(NoteModifyAckMessage{LastTime: 5, Path: "a.md", PathHash: "h", NoteAckExt: NoteAckExt{OpID: "o", Rev: 5, Sha: "s", Merged: true, Content: "c", ConflictCopy: "x.md"}})
	want := `{"lastTime":5,"path":"a.md","pathHash":"h","opId":"o","rev":5,"sha":"s","merged":true,"content":"c","conflictCopy":"x.md"}`
	if string(b) != want {
		t.Fatalf("ext ack: %s", b)
	}
	// requests from original clients decode with the new fields zero
	var r NoteModifyOrCreateRequest
	if err := json.Unmarshal([]byte(`{"vault":"v","path":"p","baseHash":"1"}`), &r); err != nil || r.ServerMerge || r.OpID != "" || r.BaseSha != "" {
		t.Fatalf("old request decode: %+v %v", r, err)
	}
}
