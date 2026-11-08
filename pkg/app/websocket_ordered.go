package app

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/safego"
	"github.com/lxzan/gws"
	"go.uber.org/zap"
)

// lucky-v2: per-connection ordered send queue.
// Messages enqueued while holding a note lock are written by a single drain goroutine per
// connection in enqueue order, so "Ack/broadcast order == write order" holds per connection
// even though the actual network write happens after the lock is released.
// lucky-v2：每连接有序发送队列。锁内入队，每连接单个 drain goroutine 按入队顺序写出。

type orderedMsg struct {
	opcode  gws.Opcode
	payload []byte
}

// Per-connection backlog caps; a client that cannot keep up is disconnected (it resyncs on
// reconnect) instead of letting the queue grow without bound. Vars so tests can shrink them.
// 每连接积压上限；超限断开，客户端重连后重新同步。
// CloseResyncRequired is the WebSocket close code sent when the server dropped queued messages.
const CloseResyncRequired = 4001

var (
	orderedCloseGrace = 5 * time.Second
	orderedMaxMsgs  = 1024
	orderedMaxBytes = 64 << 20
)

// OrderedStats are process-wide counters for the health endpoint.
var OrderedStats struct {
	Overflow   atomic.Int64 // connections dropped for exceeding the cap
	MaxBacklog atomic.Int64 // largest queued message count seen
}

type orderedQueue struct {
	mu         sync.Mutex
	resyncOnce sync.Once
	q          []orderedMsg
	bytes      int
	running    bool
	closed     bool
}

// closeOrdered stops the writer and drops anything still queued (called from OnClose).
func (c *WebsocketClient) closeOrdered() {
	oq := &c.outQ
	oq.mu.Lock()
	oq.closed = true
	oq.q, oq.bytes = nil, 0
	oq.mu.Unlock()
}

// RequireFullResync fails a connection closed when delivery could be incomplete.
// 4001 makes clients discard incremental watermarks and reconcile on reconnect.
// Concurrent failures share one close attempt and one forced-close grace timer.
func (c *WebsocketClient) RequireFullResync() {
	c.outQ.resyncOnce.Do(func() {
		c.closeOrdered()
		if c.conn != nil {
			conn := c.conn
			time.AfterFunc(orderedCloseGrace, func() { _ = conn.NetConn().Close() })
			safego.Go(wsLogger, func() { conn.WriteClose(CloseResyncRequired, []byte("resyncRequired")) })
		}
	})
}

func (c *WebsocketClient) enqueueOrdered(opcode gws.Opcode, payload []byte) {
	oq := &c.outQ
	oq.mu.Lock()
	if oq.closed {
		oq.mu.Unlock()
		return
	}
	if len(oq.q)+1 > orderedMaxMsgs || oq.bytes+len(payload) > orderedMaxBytes {
		oq.closed = true
		oq.q, oq.bytes = nil, 0
		oq.mu.Unlock()
		OrderedStats.Overflow.Add(1)
		log(LogWarn, "WS ordered send queue overflow, disconnecting slow client", zap.String("traceID", c.TraceID))
		c.RequireFullResync()
		return
	}
	oq.q = append(oq.q, orderedMsg{opcode, payload})
	oq.bytes += len(payload)
	if n := int64(len(oq.q)); n > OrderedStats.MaxBacklog.Load() {
		OrderedStats.MaxBacklog.Store(n)
	}
	if oq.running {
		oq.mu.Unlock()
		return
	}
	oq.running = true
	oq.mu.Unlock()

	safego.Go(wsLogger, func() {
		for {
			oq.mu.Lock()
			if oq.closed || len(oq.q) == 0 {
				oq.running = false
				oq.q = nil
				oq.bytes = 0
				oq.mu.Unlock()
				return
			}
			m := oq.q[0]
			oq.q = oq.q[1:]
			oq.mu.Unlock()

			err := c.safeWrite(m)

			oq.mu.Lock()
			if !oq.closed {
				oq.bytes -= len(m.payload)
			}
			oq.mu.Unlock()
			if err != nil {
				if c.failCount.Add(1) == 4 && c.conn != nil {
					c.conn.WriteClose(1000, []byte("send failed"))
				}
			} else {
				c.failCount.Store(0)
			}
		}
	})
}

// safeWrite never panics, so one bad write cannot wedge the drain loop.
func (c *WebsocketClient) safeWrite(m orderedMsg) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ordered write panic: %v", r)
		}
	}()
	return c.writeMessage(m.opcode, m.payload)
}

func (c *WebsocketClient) resFromCode(cd *code.Code) *Res {
	content := Res{
		Code:    cd.Code(),
		Status:  cd.Status(),
		Message: cd.MsgIn(c.Lang),
		Data:    cd.Data(),
	}
	if cd.HaveDetails() {
		content.Details = strings.Join(cd.Details(), ",")
	}
	if cd.HaveVault() {
		content.Vault = cd.Vault()
	}
	if cd.HaveContext() {
		content.Context = cd.Context()
	}
	return &content
}

// encodeFor encodes res for target connection uc (protobuf if negotiated, else "action|json").
func encodeFor(uc *WebsocketClient, res *Res, actionType string) (gws.Opcode, []byte, error) {
	if uc.UseProtobuf() && uc.Server.ProtobufEncoder != nil && actionType != "" {
		pb, err := uc.Server.ProtobufEncoder(actionType, res)
		if err == nil {
			return gws.OpcodeBinary, pb, nil
		}
		log(LogError, "WS Protobuf encode failed, falling back to JSON", zap.Error(err))
	}
	b, err := json.Marshal(res)
	if err != nil {
		return gws.OpcodeText, nil, err
	}
	if actionType != "" {
		b = []byte(fmt.Sprintf(`%s|%s`, actionType, string(b)))
	}
	return gws.OpcodeText, b, nil
}

// ToResponseOrdered is ToResponse but the write goes through the ordered queue.
// ToResponseOrdered 同 ToResponse，但经有序队列发送。
func (c *WebsocketClient) ToResponseOrdered(cd *code.Code, actionType string) {
	op, b, err := encodeFor(c, c.resFromCode(cd), actionType)
	if err != nil {
		return
	}
	c.enqueueOrdered(op, b)
}

// BroadcastResponseOrdered is BroadcastResponse but each target write goes through that
// target's ordered queue (no waiting for the network).
// BroadcastResponseOrdered 同 BroadcastResponse，但每个目标连接经其有序队列发送。
func (c *WebsocketClient) BroadcastResponseOrdered(cd *code.Code, isExcludeSelf bool, actionType string) {
	res := c.resFromCode(cd)
	c.Server.mu.RLock()
	targets := make([]*WebsocketClient, 0, len(c.UserClients))
	for _, uc := range c.UserClients {
		if uc.conn == nil || (isExcludeSelf && uc.conn == c.conn) {
			continue
		}
		targets = append(targets, uc)
	}
	c.Server.mu.RUnlock()
	for _, uc := range targets {
		if op, b, err := encodeFor(uc, res, actionType); err == nil {
			uc.enqueueOrdered(op, b)
		}
	}
}

// BroadcastToUserOrdered pushes to every connection of the user through their ordered queues
// (used for crash-recovery results, which have no originating connection).
// BroadcastToUserOrdered 经有序队列推送给该用户所有连接。
func (w *WebsocketServer) BroadcastToUserOrdered(uid int64, cd *code.Code, actionType string) {
	w.mu.RLock()
	var targets []*WebsocketClient
	for _, uc := range w.userClients[fmt.Sprintf("%d", uid)] {
		if uc.conn != nil {
			targets = append(targets, uc)
		}
	}
	w.mu.RUnlock()
	for _, uc := range targets {
		if op, b, err := encodeFor(uc, uc.resFromCode(cd), actionType); err == nil {
			uc.enqueueOrdered(op, b)
		}
	}
}
