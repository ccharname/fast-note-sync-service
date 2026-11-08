package service
import (
 "context"
 "errors"
 "fmt"
 "os"
 "path/filepath"
 "testing"
 "time"
 "github.com/haierkeys/fast-note-sync-service/internal/dao"
 "github.com/haierkeys/fast-note-sync-service/internal/dto"
 "github.com/haierkeys/fast-note-sync-service/pkg/util"
 "github.com/stretchr/testify/assert"
 "github.com/stretchr/testify/require"
 "go.uber.org/zap"
 "go.uber.org/zap/zaptest/observer"
)
func TestR3_ReviewContentNewDBOld(t *testing.T) {
 s:=newMergeEnv(t); defer restart(t)
 base:="l1\nl2\nl3\nl4\nl5\nl6\nl7\n"
 modify(t,s,mreq("n.md",base,"<none>","seed"))
 modify(t,s,mreq("n.md","SRV\nl2\nl3\nl4\nl5\nl6\nl7\n",base,"srv"))
 req:=mreq("n.md","l1\nl2\nl3\nl4\nl5\nl6\nCLI\n",base,"op1")
 want:="SRV\nl2\nl3\nl4\nl5\nl6\nCLI\n"
 crashAt("after_intent")
 require.Error(t,s.ServerMergeModify(context.Background(),tuid,req,func(*MergeResult){}))
 n:=current(t,s,"n.md"); oldRev:=n.UpdatedTimestamp
 require.NoError(t,dao.AtomicWriteFile(filepath.Dir(snapDir(tuid,n.ID)),"content.txt",[]byte(want)))
 restart(t); unlock:=s.lockAndRecover(context.Background(),tuid,1,req.PathHash);unlock()
 n=current(t,s,"n.md")
 t.Logf("after rollforward: content=%q hash=%s expectedHash=%s rev=%d oldRev=%d pending=%d",n.Content,n.ContentHash,util.EncodeHash32(want),n.UpdatedTimestamp,oldRev,pendingCount())
 require.Equal(t,util.EncodeHash32(want),n.ContentHash,"must repair DB metadata for visible new body")
 require.Greater(t,n.UpdatedTimestamp,oldRev,"must publish new content with newer rev")
}
func TestR3_ReviewReplayEvictedSnapshot(t *testing.T) {
 s:=newMergeEnv(t); defer restart(t)
 base:="a\nb\nc\nd\ne\nf\ng\n"
 modify(t,s,mreq("n.md",base,"<none>","seed"))
 modify(t,s,mreq("n.md","SRV\nb\nc\nd\ne\nf\ng\n",base,"srv"))
 req:=mreq("n.md","a\nb\nc\nd\ne\nf\nCLI\n",base,"lost-ack")
 first:=modify(t,s,req); n:=current(t,s,"n.md");dir:=snapDir(tuid,n.ID)
 prev:=n.Content
 for i:=0;i<80;i++ {next:=fmt.Sprintf("new-%03d\n",i); modify(t,s,mreq("n.md",next,prev,fmt.Sprintf("op-%d",i)));prev=next}
 pruneSnaps(dir)
 _,err:=os.Stat(filepath.Join(dir,first.Ack.Sha+".txt")); require.True(t,os.IsNotExist(err),"old snapshot must be evicted")
 got:=modify(t,s,req)
 t.Logf("replay: sha=%s computed=%s content=%q rev=%d",got.Ack.Sha,sha256Hex(got.Ack.Content),got.Ack.Content,got.Ack.Rev)
 require.Equal(t,got.Ack.Sha,sha256Hex(got.Ack.Content),"replay content must match its sha")
}
func TestR3_ReviewTornDoneReplayDuplicates(t *testing.T) {
 s:=newMergeEnv(t); defer restart(t)
 base:="a\n"
 modify(t,s,mreq("n.md",base,"<none>","seed"))
 modify(t,s,mreq("n.md","s\na\n",base,"srv"))
 req:=mreq("n.md","c\na\n",base,"lost-ack")
 first:=modify(t,s,req); before:=body(t,s,"n.md")
 n:=current(t,s,"n.md");jp:=filepath.Join(snapDir(tuid,n.ID),journalName)
 data,err:=os.ReadFile(jp); require.NoError(t,err)
 last:=len(data)-2;for last>=0&&data[last]!='\n' {last--}
 // Lose the end of the non-fsynced done line at a power-loss boundary.
 require.NoError(t,os.WriteFile(jp,data[:last+1+10],0644))
 restart(t)
 replay:=modify(t,s,req)
 after:=body(t,s,"n.md")
 t.Logf("before=%q after=%q oldAck=%+v newAck=%+v",before,after,first.Ack,replay.Ack)
 require.Equal(t,before,after,"same request after torn done must not run diff3 again")
}
func TestR3_ReviewLegacyRestoreRunsBeforeRecovery(t *testing.T) {
 s:=newMergeEnv(t);defer restart(t)
 base:="a\n"
 modify(t,s,mreq("n.md",base,"<none>","seed"))
 req:=&dto.NoteDeleteRequest{Vault:"v",Path:"n.md",PathHash:util.EncodeHash32("n.md"),BaseSha:sha256Hex(base),OpID:"del-lost-ack"}
 crashAt("after_write")
 require.Error(t,s.ServerMergeDelete(context.Background(),tuid,req,func(*MergeResult){}))
 restart(t)
 _,err:=s.Restore(context.Background(),tuid,&dto.NoteRestoreRequest{Vault:"v",Path:"n.md",PathHash:req.PathHash});require.NoError(t,err)
 require.NotEqual(t,"delete",string(current(t,s,"n.md").Action))
 unlock:=s.lockAndRecover(context.Background(),tuid,1,req.PathHash);unlock()
 t.Logf("after acknowledged restore and then lazy recovery: action=%s",string(current(t,s,"n.md").Action))
 require.NotEqual(t,"delete",string(current(t,s,"n.md").Action),"an acknowledged newer restore must survive recovery")
}
func TestR3_ReviewRollforwardNeverBroadcasts(t *testing.T) {
 s:=newMergeEnv(t);defer restart(t)
 base:="a\nb\nc\nd\ne\nf\ng\n"
 modify(t,s,mreq("n.md",base,"<none>","seed"))
 modify(t,s,mreq("n.md","SRV\nb\nc\nd\ne\nf\ng\n",base,"srv"))
 req:=mreq("n.md","a\nb\nc\nd\ne\nf\nCLI\n",base,"lost-ack")
 before:=body(t,s,"n.md")
 crashAt("after_intent")
 require.Error(t,s.ServerMergeModify(context.Background(),tuid,req,func(*MergeResult){}))
 restart(t)
 replay:=modify(t,s,req)
 after:=body(t,s,"n.md")
 require.NotEqual(t,before,after)
 t.Logf("before=%q after=%q ACK=%+v Written=%+v Copy=%+v",before,after,replay.Ack,replay.Written,replay.Copy)
 require.NotNil(t,replay.Written,"newly recovered committed body must notify other connected devices")
}

// RecycleClear must serialize with writers of the same note.
func TestR3_RecycleClearTakesNoteLock(t *testing.T) {
	s := newMergeEnv(t)
	modify(t, s, mreq("n.md", "a\n", "<none>", "seed"))
	h := util.EncodeHash32("n.md")
	_, err := s.Delete(context.Background(), tuid, &dto.NoteDeleteRequest{Vault: "v", Path: "n.md", PathHash: h})
	require.NoError(t, err)
	unlock := lockNotes(noteLockKey(tuid, 1, h))
	done := make(chan error, 1)
	go func() { done <- s.noteRepo.RecycleClear(context.Background(), "n.md", h, 1, tuid) }()
	select {
	case <-done:
		t.Fatal("RecycleClear ran while the note lock was held")
	case <-time.After(150 * time.Millisecond):
	}
	unlock()
	require.NoError(t, <-done)
}

// A failing journal merge must surface as an error, not be swallowed.
func TestR3_MoveSnapDirReportsJournalError(t *testing.T) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "from"), filepath.Join(dir, "to")
	require.NoError(t, os.MkdirAll(from, 0755))
	require.NoError(t, os.MkdirAll(to, 0755))
	require.NoError(t, journalAppend(from, &jrec{T: "op", K: "k"}, true))
	// destination journal path is a directory: appending to it must fail loudly
	require.NoError(t, os.MkdirAll(filepath.Join(to, journalName), 0755))
	require.Error(t, moveSnapDir(from, to))
}

// Crash while the staging dir of a new note is being adopted (before / after the move).
func TestR3_CrashDuringStagingAdopt(t *testing.T) {
	for _, stage := range []string{"adopt_before", "adopt_after"} {
		t.Run(stage, func(t *testing.T) {
			defer restart(t)
			s := newMergeEnv(t)
			req := mreq("n.md", "hello\n", "<none>", "op1")
			crashAt(stage)
			err := s.ServerMergeModify(context.Background(), tuid, req, func(*MergeResult) { t.Fatal("ack on crash") })
			require.True(t, errors.Is(err, errCrash))
			restart(t)
			r := modify(t, s, mreq("n.md", "hello\n", "<none>", "op-retry"))
			assert.Equal(t, sha256Hex("hello\n"), r.Ack.Sha)
			assert.Equal(t, "hello\n", body(t, s, "n.md"))
			n := current(t, s, "n.md")
			st := journalRead(snapDir(tuid, n.ID))
			assert.Equal(t, 0, len(st.pending))
			assert.Equal(t, 0, pendingCount())
			// no leftover staging dir with a pending journal
			left, _ := filepath.Glob(filepath.Join("storage", "vault", "u_7", "note", "_new", "*", journalName))
			for _, f := range left {
				assert.Equal(t, 0, len(journalRead(filepath.Dir(f)).pending), f)
			}
		})
	}
}


// Recovery triggered by a legacy entry (not a serverMerge request) broadcasts what it committed.
func TestR3_LegacyEntryRecoveryBroadcasts(t *testing.T) {
	s := newMergeEnv(t)
	defer restart(t)
	var got []*MergeResult
	RecoveryBroadcast = func(uid int64, ev *MergeResult) { got = append(got, ev) }
	defer func() { RecoveryBroadcast = nil }()
	base := "a\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	crashAt("after_intent")
	req := mreq("n.md", "b\n", base, "op1")
	require.Error(t, s.ServerMergeModify(context.Background(), tuid, req, func(*MergeResult) {}))
	restart(t)
	p := mreq("n.md", "b\n", "<none>", "")
	p.ServerMerge = false
	_, _, err := s.ModifyOrCreate(context.Background(), tuid, p, false)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Written)
	assert.Equal(t, "b\n", got[0].Written.Content)
	assert.Equal(t, tuid, got[0].UID)
}

// The reason of every op is visible in the journal (intent and done), and ops log at info level
// through the injected logger.
func TestR3_JournalCarriesReasonAndOpLogIsInfo(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	SetOpLogger(zap.New(core))
	defer SetOpLogger(nil)
	s := newMergeEnv(t)
	base := "a\nb\nc\nd\ne\nf\ng\n"
	modify(t, s, mreq("n.md", base, "<none>", "seed"))
	modify(t, s, mreq("n.md", "SRV\nb\nc\nd\ne\nf\ng\n", base, "srv"))
	modify(t, s, mreq("n.md", "a\nb\nc\nd\ne\nf\nCLI\n", base, "merge"))
	modify(t, s, mreq("n.md", "a\nSRV2\nc\nd\ne\nf\ng\n", "SRV\nb\nc\nd\ne\nf\ng\n", "x")) // adjacent => overlap/conflict path
	n := current(t, s, "n.md")
	reasons := map[string]bool{}
	for _, r := range journalRead(snapDir(tuid, n.ID)).all {
		reasons[r.Reason] = true
	}
	assert.True(t, reasons["merged"], "%v", reasons)
	assert.True(t, reasons["fastforward"], "%v", reasons)
	assert.GreaterOrEqual(t, logs.FilterMessage("serverMerge op").Len(), 4)
}
