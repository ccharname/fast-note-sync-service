package websocket_router

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/service"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/workerpool"
	"go.uber.org/zap"
)

func TestSyncDownloadContentFailureDoesNotDeliverOrAdvance(t *testing.T) {
	for _, kind := range []string{"read error", "mixed read error", "worker queue full"} {
		t.Run(kind, func(t *testing.T) {
			queueSize := 4
			if kind == "worker queue full" {
				queueSize = 1
			}
			pool := workerpool.New(&workerpool.Config{MaxWorkers: 1, QueueSize: queueSize}, nil)
			previousPool := noteContentFillPool
			noteContentFillPool = pool
			release := make(chan struct{})
			t.Cleanup(func() {
				close(release)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := pool.Shutdown(ctx); err != nil {
					t.Error(err)
				}
				noteContentFillPool = previousPool
			})
			if kind == "worker queue full" {
				started := make(chan struct{})
				if err := pool.SubmitAsync(context.Background(), func(context.Context) error {
					close(started)
					<-release
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				<-started
				if err := pool.SubmitAsync(context.Background(), func(context.Context) error { return nil }); err != nil {
					t.Fatal(err)
				}
				if pool.QueuedCount() != 1 {
					t.Fatal("queue not saturated")
				}
			}
			previousResponse := syncPageResponse
			var successes, resyncs int
			syncPageResponse = func(_ *pkgapp.WebsocketClient, response *code.Code, _ string) {
				if response.Code() == code.Success.Code() {
					successes++
				}
			}
			previousResync := syncPageRequireFullResync
			syncPageRequireFullResync = func(c *pkgapp.WebsocketClient) {
				resyncs++
				c.RequireFullResync()
			}
			t.Cleanup(func() {
				syncPageResponse = previousResponse
				syncPageRequireFullResync = previousResync
			})
			var reads atomic.Int32
			entry := &syncDownloadEntry{
				Context: t.Name(), TypeName: "note", Vault: "test", PageSize: 1,
				MessageQueue: []dto.WSQueuedMessage{{NoteID: 1, Action: string(NoteSyncModify),
					Data: dto.NoteSyncModifyMessage{Path: "keep.md", ContentHash: "nonempty-hash", Rev: 10}}},
				FillContent: func(context.Context, int64) (string, error) {
					reads.Add(1)
					return "", errors.New("injected read failure")
				},
			}
			if kind == "mixed read error" {
				entry.PageSize = 2
				entry.MessageQueue = append(entry.MessageQueue, dto.WSQueuedMessage{
					NoteID: 2, Action: string(NoteSyncModify), Data: dto.NoteSyncModifyMessage{Path: "good.md"},
				})
				entry.FillContent = func(_ context.Context, id int64) (string, error) {
					reads.Add(1)
					if id == 2 {
						return "valid body", nil
					}
					return "", errors.New("injected read failure")
				}
			}
			syncDownloadStore(entry.Context, entry.TypeName, entry)
			t.Cleanup(func() { syncDownloadDelete(entry.Context, entry.TypeName) })
			client := &pkgapp.WebsocketClient{WsCtx: context.Background(), ProtoVersion: 2}
			pump(client, entry)
			if successes != 0 || entry.SentPage != 0 || entry.AckedPage != 0 {
				t.Errorf("failed body delivered as success or advanced: successes=%d sent=%d acked=%d", successes, entry.SentPage, entry.AckedPage)
			}
			if resyncs != 1 {
				t.Errorf("resync closes=%d, want 1", resyncs)
			}
			if _, ok := syncDownloadGet(entry.Context, entry.TypeName); ok {
				t.Error("failed sync entry still available for success ACKs")
			}
			if kind == "worker queue full" && reads.Load() != 0 {
				t.Errorf("rejected job unexpectedly read content: %d", reads.Load())
			}
			if kind == "mixed read error" && reads.Load() != 2 {
				t.Errorf("mixed page did not exercise both successful and failing reads: %d", reads.Load())
			}
			for _, queued := range entry.MessageQueue {
				if queued.Data.(dto.NoteSyncModifyMessage).Content != "" {
					t.Error("failed page mutated the cached queue")
				}
			}
			// An already-resolved concurrent ACK must not revive the failed entry.
			handlePageAck(client, entry, -1, "note", zap.NewNop(), "test")
			if successes != 0 || entry.SentPage != 0 || entry.AckedPage != 0 || resyncs != 1 {
				t.Errorf("late ACK revived failed page: successes=%d sent=%d acked=%d resyncs=%d", successes, entry.SentPage, entry.AckedPage, resyncs)
			}
		})
	}
}

func TestSyncDownloadContentSuccessIncludesLegitimateEmptyBody(t *testing.T) {
	previousResponse, previousResync := syncPageResponse, syncPageRequireFullResync
	var messages []*code.Code
	syncPageResponse = func(_ *pkgapp.WebsocketClient, response *code.Code, _ string) {
		messages = append(messages, response)
	}
	syncPageRequireFullResync = func(*pkgapp.WebsocketClient) { t.Error("successful page requested resync") }
	t.Cleanup(func() { syncPageResponse, syncPageRequireFullResync = previousResponse, previousResync })
	entry := &syncDownloadEntry{
		Context: t.Name(), TypeName: "note", Vault: "test", PageSize: 2,
		MessageQueue: []dto.WSQueuedMessage{
			{NoteID: 1, Context: t.Name(), Action: string(NoteSyncModify), Data: dto.NoteSyncModifyMessage{Path: "empty.md", Rev: 10}},
			{NoteID: 2, Context: t.Name(), Action: string(NoteSyncModify), Data: dto.NoteSyncModifyMessage{Path: "normal.md", Rev: 11}},
		},
		FillContent: func(_ context.Context, id int64) (string, error) {
			if id == 1 {
				return "", nil
			}
			return "正常正文\n", nil
		},
	}
	syncDownloadStore(entry.Context, entry.TypeName, entry)
	t.Cleanup(func() { syncDownloadDelete(entry.Context, entry.TypeName) })
	pump(&pkgapp.WebsocketClient{WsCtx: context.Background(), ProtoVersion: 2}, entry)
	if len(messages) != 3 || entry.SentPage != 1 {
		t.Fatalf("success page: messages=%d sent=%d, want 3 and 1", len(messages), entry.SentPage)
	}
	page := messages[0].Data().(dto.SyncPageMessage)
	if page.TotalCount != 2 || !page.IsLast || page.PageIndex != 0 {
		t.Fatalf("page metadata: %+v", page)
	}
	for i, body := range []string{"", "正常正文\n"} {
		message := messages[i+1]
		note := message.Data().(dto.NoteSyncModifyMessage)
		if message.Code() != code.Success.Code() || message.PageIndex() != 1 || note.Content != body || note.Sha != service.ContentSha(body) || note.Rev != int64(10+i) {
			t.Errorf("note %d wrong body/hash/rev/page: %+v", i, note)
		}
		if entry.MessageQueue[i].Data.(dto.NoteSyncModifyMessage).Content != "" {
			t.Error("filled body retained in cached queue")
		}
	}
	if _, ok := syncDownloadGet(entry.Context, entry.TypeName); ok {
		t.Error("completed final page retained in cache")
	}
}
