package app

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lxzan/gws"
	"go.uber.org/zap"
)

type orderedTestHandler struct {
	gws.BuiltinEventHandler
	mu   sync.Mutex
	msgs []string
}

func (h *orderedTestHandler) OnMessage(c *gws.Conn, m *gws.Message) {
	h.mu.Lock()
	h.msgs = append(h.msgs, string(m.Bytes()))
	h.mu.Unlock()
	m.Close()
}

type serverSide struct {
	gws.BuiltinEventHandler
	ready chan *gws.Conn
}

func (s *serverSide) OnOpen(c *gws.Conn) { s.ready <- c }

// lucky-v2: messages enqueued in a given order (as done under a note lock) arrive in that
// order on the connection, even though writes happen on a drain goroutine.
func TestOrderedQueue_PreservesEnqueueOrder(t *testing.T) {
	oldM := orderedMaxMsgs
	orderedMaxMsgs = 1 << 20 // this test bursts 2000 messages on purpose
	defer func() { orderedMaxMsgs = oldM }()
	ss := &serverSide{ready: make(chan *gws.Conn, 1)}
	up := gws.NewUpgrader(ss, &gws.ServerOption{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r)
		if err != nil {
			return
		}
		go conn.ReadLoop()
	}))
	defer srv.Close()

	ch := &orderedTestHandler{}
	cc, _, err := gws.NewClient(ch, &gws.ClientOption{Addr: "ws" + strings.TrimPrefix(srv.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	go cc.ReadLoop()
	sconn := <-ss.ready

	SetWSLogger(zapDev())
	client := &WebsocketClient{conn: sconn, Server: &WebsocketServer{config: &WSConfig{}}}

	const total = 2000
	var lock sync.Mutex // stands in for the per-note lock
	var wg sync.WaitGroup
	next := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lock.Lock()
				if next >= total {
					lock.Unlock()
					return
				}
				n := next
				next++
				client.enqueueOrdered(gws.OpcodeText, []byte(fmt.Sprintf("%05d", n)))
				lock.Unlock()
			}
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ch.mu.Lock()
		n := len(ch.msgs)
		ch.mu.Unlock()
		if n == total {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.msgs) != total {
		t.Fatalf("got %d/%d messages", len(ch.msgs), total)
	}
	for i, m := range ch.msgs {
		if m != fmt.Sprintf("%05d", i) {
			t.Fatalf("out of order at %d: %s", i, m)
		}
	}
}

func zapDev() *zap.Logger { l, _ := zap.NewDevelopment(); return l }

// lucky-v2 R2: a client that never reads is disconnected once its backlog exceeds the cap,
// and the writer goroutine is released (queue closed, nothing more is accepted).
func TestOrderedQueue_SlowClientOverflowDisconnects(t *testing.T) {
	oldM, oldB := orderedMaxMsgs, orderedMaxBytes
	orderedMaxMsgs, orderedMaxBytes = 1000, 8<<20
	oldG := orderedCloseGrace
	orderedCloseGrace = 300 * time.Millisecond
	defer func() { orderedMaxMsgs, orderedMaxBytes, orderedCloseGrace = oldM, oldB, oldG }()

	ss := &serverSide{ready: make(chan *gws.Conn, 1)}
	closed := make(chan struct{}, 1)
	ss2 := &closeWatch{serverSide: ss, closed: closed}
	up := gws.NewUpgrader(ss2, &gws.ServerOption{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r)
		if err != nil {
			return
		}
		go conn.ReadLoop()
	}))
	defer srv.Close()
	// client never starts ReadLoop => its socket buffers fill and server writes block
	cc, _, err := gws.NewClient(&orderedTestHandler{}, &gws.ClientOption{Addr: "ws" + strings.TrimPrefix(srv.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	_ = cc
	sconn := <-ss.ready
	SetWSLogger(zapDev())
	client := &WebsocketClient{conn: sconn, Server: &WebsocketServer{config: &WSConfig{}}}

	before := OrderedStats.Overflow.Load()
	chunk := []byte(strings.Repeat("x", 1<<20))
	for i := 0; i < 40; i++ {
		client.enqueueOrdered(gws.OpcodeBinary, chunk)
	}
	if OrderedStats.Overflow.Load() != before+1 {
		t.Fatalf("overflow counter = %d, want %d", OrderedStats.Overflow.Load(), before+1)
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("slow connection was not closed")
	}
	client.outQ.mu.Lock()
	n, cl := len(client.outQ.q), client.outQ.closed
	client.outQ.mu.Unlock()
	if !cl || n != 0 {
		t.Fatalf("queue should be closed and empty, closed=%v len=%d", cl, n)
	}
	client.enqueueOrdered(gws.OpcodeBinary, chunk) // ignored after close
	if OrderedStats.Overflow.Load() != before+1 {
		t.Fatal("closed queue must not count again")
	}
}

type closeWatch struct {
	*serverSide
	closed chan struct{}
}

func (c *closeWatch) OnClose(*gws.Conn, error) {
	select {
	case c.closed <- struct{}{}:
	default:
	}
}

type codeWatch struct {
	gws.BuiltinEventHandler
	code chan uint16
}

func (h *codeWatch) OnMessage(c *gws.Conn, m *gws.Message) { m.Close() }
func (h *codeWatch) OnClose(c *gws.Conn, err error) {
	if ce, ok := err.(*gws.CloseError); ok {
		h.code <- ce.Code
	} else {
		h.code <- 0
	}
}

// The overflow disconnect carries close code 4001 (resyncRequired) so the client reconciles fully.
func TestOrderedQueue_OverflowCloseCodeResyncRequired(t *testing.T) {
	oldM, oldB := orderedMaxMsgs, orderedMaxBytes
	orderedMaxMsgs, orderedMaxBytes = 1000, 8<<20
	defer func() { orderedMaxMsgs, orderedMaxBytes = oldM, oldB }()
	ss := &serverSide{ready: make(chan *gws.Conn, 1)}
	up := gws.NewUpgrader(ss, &gws.ServerOption{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r)
		if err != nil {
			return
		}
		go conn.ReadLoop()
	}))
	defer srv.Close()
	cw := &codeWatch{code: make(chan uint16, 1)}
	cc, _, err := gws.NewClient(cw, &gws.ClientOption{Addr: "ws" + strings.TrimPrefix(srv.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	sconn := <-ss.ready
	SetWSLogger(zapDev())
	client := &WebsocketClient{conn: sconn, Server: &WebsocketServer{config: &WSConfig{}}}
	chunk := []byte(strings.Repeat("x", 1<<20))
	for i := 0; i < 40; i++ {
		client.enqueueOrdered(gws.OpcodeBinary, chunk)
	}
	go cc.ReadLoop() // the slow client finally reads: it must see the close code after the backlog
	select {
	case c := <-cw.code:
		if c != CloseResyncRequired {
			t.Fatalf("close code = %d, want %d", c, CloseResyncRequired)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no close")
	}
}
