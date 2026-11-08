package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lxzan/gws"
)

func TestRequireFullResync_ConcurrentFailuresClose4001(t *testing.T) {
	ss := &serverSide{ready: make(chan *gws.Conn, 1)}
	up := gws.NewUpgrader(ss, &gws.ServerOption{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r)
		if err == nil {
			go conn.ReadLoop()
		}
	}))
	defer srv.Close()
	cw := &codeWatch{code: make(chan uint16, 1)}
	cc, _, err := gws.NewClient(cw, &gws.ClientOption{Addr: "ws" + strings.TrimPrefix(srv.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.NetConn().Close()
	go cc.ReadLoop()
	client := &WebsocketClient{conn: <-ss.ready}
	client.outQ.q = []orderedMsg{{opcode: gws.OpcodeText, payload: []byte("do-not-deliver")}}
	client.outQ.bytes = len("do-not-deliver")
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(client.RequireFullResync)
	}
	wg.Wait()
	select {
	case got := <-cw.code:
		if got != CloseResyncRequired {
			t.Fatalf("close code = %d, want 4001", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failure did not close the connection")
	}
	client.enqueueOrdered(gws.OpcodeText, []byte("late message"))
	client.outQ.mu.Lock()
	defer client.outQ.mu.Unlock()
	if !client.outQ.closed || len(client.outQ.q) != 0 || client.outQ.bytes != 0 {
		t.Fatalf("queue not failed closed: closed=%v messages=%d bytes=%d", client.outQ.closed, len(client.outQ.q), client.outQ.bytes)
	}
	// The sync.Once also covers the grace timer, not only queue mutation.
	calledAgain := false
	client.outQ.resyncOnce.Do(func() { calledAgain = true })
	if calledAgain {
		t.Fatal("resync close was not marked once")
	}
}
