package tunnel

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
	"go.uber.org/zap"
)

// controlChannel wraps the single control WS for a node. It serializes writes
// (one WS cannot be written concurrently) and runs the ping/pong keepalive.
type controlChannel struct {
	conn *websocket.Conn
	log  *zap.Logger

	writeMu sync.Mutex

	lastSeen int64 // unix nanos, atomic
	rttMs    int64 // atomic
	closed   int32 // atomic

	// onStats, when set, receives client telemetry frames (design §5 'stats').
	onStats func(*proto.Stats)

	done chan struct{}
}

func newControlChannel(conn *websocket.Conn, log *zap.Logger) *controlChannel {
	c := &controlChannel{conn: conn, log: log, done: make(chan struct{})}
	c.touch()
	return c
}

func (c *controlChannel) touch() {
	atomic.StoreInt64(&c.lastSeen, time.Now().UnixNano())
}

func (c *controlChannel) lastSeenTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastSeen))
}

func (c *controlChannel) rtt() int64 { return atomic.LoadInt64(&c.rttMs) }

func (c *controlChannel) isClosed() bool { return atomic.LoadInt32(&c.closed) == 1 }

// sendEnvelope marshals and writes a typed control message as one binary WS msg.
func (c *controlChannel) sendEnvelope(ctx context.Context, typ string, payload any) error {
	raw, err := proto.WrapEnvelope(typ, payload)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Write(ctx, websocket.MessageBinary, raw)
}

func (c *controlChannel) close(reason string) {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		close(c.done)
		_ = c.conn.Close(websocket.StatusNormalClosure, reason)
	}
}

// readLoop reads control messages until the connection drops. It updates
// lastSeen, answers pings, records pong RTT, and dispatches stats. When it
// returns, the control channel is considered dropped and onDrop is invoked.
func (c *controlChannel) readLoop(ctx context.Context, onDrop func()) {
	defer onDrop()
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		c.touch()
		var env proto.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		switch env.Type {
		case proto.TypePing:
			var p proto.Ping
			_ = json.Unmarshal(env.Payload, &p)
			_ = c.sendEnvelope(ctx, proto.TypePong, proto.Pong{Nonce: p.Nonce, Ts: p.Ts})
		case proto.TypePong:
			var p proto.Pong
			if json.Unmarshal(env.Payload, &p) == nil && p.Ts != 0 {
				rtt := (time.Now().UnixNano() - p.Ts) / int64(time.Millisecond)
				if rtt >= 0 {
					atomic.StoreInt64(&c.rttMs, rtt)
				}
			}
		case proto.TypeStats:
			// Client telemetry (design §5): channel/stream counts, client-side
			// byte totals and local dial errors. Server counters stay
			// authoritative for traffic; dial errors only exist on the client.
			var st proto.Stats
			if json.Unmarshal(env.Payload, &st) == nil && c.onStats != nil {
				c.onStats(&st)
			}
		case proto.TypeBye:
			return
		}
	}
}

// pingLoop sends a server-side ping every heartbeat interval and records the
// timestamp so the matching pong yields RTT. Stops when the channel closes.
func (c *controlChannel) pingLoop(ctx context.Context, heartbeat time.Duration) {
	t := time.NewTicker(heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-t.C:
			_ = c.sendEnvelope(ctx, proto.TypePing, proto.Ping{Ts: time.Now().UnixNano()})
		}
	}
}
