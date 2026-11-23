package tunnel

import (
	"context"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
	"github.com/xtaci/smux"
	"go.uber.org/zap"
)

// smuxConfig builds the smux config shared by all data channels. Data-channel
// keepalive is smux's job (design §5), not the control heartbeat.
func smuxConfig() *smux.Config {
	cfg := smux.DefaultConfig()
	cfg.KeepAliveInterval = 10 * time.Second
	cfg.KeepAliveTimeout = 30 * time.Second
	return cfg
}

// attachControl binds a freshly-authenticated control WS to the node. If a
// previous control channel is within its grace window this reconnect restores
// routing (design §3 decision #3). It mints (or, on in-grace reconnect,
// preserves) the binding session, sends welcome with embedded config, and
// starts the read/ping loops. Blocks until the control channel drops.
func (n *Node) attachControl(ctx context.Context, conn *websocket.Conn, heartbeat time.Duration, log *zap.Logger) error {
	cc := newControlChannel(conn, log)
	// Client telemetry frames are applied to this node (design §5 'stats').
	cc.onStats = n.applyStats

	n.mu.Lock()
	// Preserve session across in-grace reconnect so existing data channels
	// (which were validated against the old session) remain valid.
	inGraceReconnect := !n.controlDown.IsZero() && n.session != ""
	if n.control != nil {
		n.control.close("replaced by new control channel")
	}
	if n.session == "" {
		n.session = generateSession()
	}
	n.control = cc
	n.controlDown = time.Time{} // mark up
	session := n.session
	channels := n.cfg.Channels
	// Build the wire config under the read lock: toProtoConfig copies the ports
	// map, which Reconfigure can swap concurrently.
	wireCfg := n.cfg.toProtoConfig(n.dialTimeout, n.queueTimeout)
	n.mu.Unlock()

	if inGraceReconnect {
		log.Info("control channel reconnected within grace window", zap.String("node", n.Name))
	}

	welcome := proto.Welcome{
		Session:   session,
		Heartbeat: heartbeat.String(),
		Channels:  channels,
		Config:    wireCfg,
		Mux: &proto.Mux{
			KeepAliveItvl:    (10 * time.Second).String(),
			KeepAliveTimeout: (30 * time.Second).String(),
		},
	}
	if err := cc.sendEnvelope(ctx, proto.TypeWelcome, welcome); err != nil {
		cc.close("welcome write failed")
		return fmt.Errorf("send welcome: %w", err)
	}

	go cc.pingLoop(ctx, heartbeat)

	// readLoop blocks until the control WS drops; onDrop records the drop time
	// so the health sweep can enforce the grace window.
	cc.readLoop(ctx, func() {
		n.mu.Lock()
		if n.control == cc {
			n.controlDown = time.Now()
		}
		n.mu.Unlock()
		cc.close("read loop ended")
		log.Info("control channel dropped, entering grace window",
			zap.String("node", n.Name), zap.Duration("grace", n.graceWindow))
	})
	return nil
}

// attachData validates the binding session, registers a new smux data channel,
// and blocks serving it until the WS drops. On drop the channel is removed; the
// client reconnects to restore the fixed count (design §11).
func (n *Node) attachData(ctx context.Context, conn *websocket.Conn, hello *proto.Hello, log *zap.Logger) error {
	n.mu.RLock()
	want := n.session
	n.mu.RUnlock()

	// Session binding (design §10): reject orphaned data connections.
	if want == "" || hello.Session != want {
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid or missing session")
		return fmt.Errorf("data channel session mismatch for node %s", n.Name)
	}

	wsConn := websocket.NetConn(ctx, conn, websocket.MessageBinary)
	ch, err := newDataChannel(hello.ChannelID, wsConn, smuxConfig())
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "smux init failed")
		return fmt.Errorf("smux server: %w", err)
	}

	n.mu.Lock()
	if old, ok := n.channels[hello.ChannelID]; ok {
		old.close()
		// Fold the replaced channel's byte counters into the node totals before
		// overwriting the map entry, so traffic/rate stats survive reconnects.
		n.retireChannelLocked(old)
		n.dataReconnectTotal++ // Track reconnect events
	}
	n.channels[hello.ChannelID] = ch
	n.mu.Unlock()

	// Send data-channel welcome echoing the channel id.
	cid := hello.ChannelID
	wel := proto.Welcome{ChannelID: &cid}
	if raw, err := proto.WrapEnvelope(proto.TypeWelcome, wel); err == nil {
		_ = conn.Write(ctx, websocket.MessageBinary, raw)
	}

	log.Info("data channel attached", zap.String("node", n.Name), zap.Int("channel_id", hello.ChannelID))

	// Block until the smux session dies (WS closed / keepalive failure). We
	// detect this by waiting on the session's done signal via a cheap poll on
	// AcceptStream: the server never expects inbound streams, so AcceptStream
	// blocks until the session errors, at which point we tear down.
	for {
		if _, err := ch.session.AcceptStream(); err != nil {
			break
		}
		// Unexpected inbound stream (client must not OpenStream); drop it.
	}

	n.mu.Lock()
	if n.channels[hello.ChannelID] == ch {
		delete(n.channels, hello.ChannelID)
		n.retireChannelLocked(ch)
	}
	n.mu.Unlock()
	ch.close()
	log.Info("data channel detached", zap.String("node", n.Name), zap.Int("channel_id", hello.ChannelID))
	return nil
}

// checkGrace enforces the control-down grace window. If control has been down
// longer than graceWindow, it tears the node down: closes all data channels
// (in-flight streams thereby error -> 502) and clears the session so stale data
// reconnects are rejected. Called periodically by the app health sweep.
func (n *Node) checkGrace() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.controlDown.IsZero() {
		return // control is up
	}
	elapsed := time.Since(n.controlDown)
	if elapsed < n.graceWindow {
		return // still within grace; keep in-flight streams alive
	}
	// Grace expired: tear down everything.
	activeChannels := len(n.channels)
	session := n.session

	for id, ch := range n.channels {
		ch.close()
		n.retireChannelLocked(ch)
		delete(n.channels, id)
	}
	n.session = ""
	n.control = nil

	// Log teardown for production diagnostics
	n.log.Warn("grace window expired, node torn down",
		zap.String("node", n.Name),
		zap.Duration("down_duration", elapsed),
		zap.Duration("grace_window", n.graceWindow),
		zap.Int("channels_closed", activeChannels),
		zap.String("session_cleared", session),
	)
}

// pushReloadConfig sends the current authoritative config to the client over
// the control channel without reconnecting (design §13 hot update).
func (n *Node) pushReloadConfig(ctx context.Context) error {
	n.mu.RLock()
	cc := n.control
	cfg := n.cfg.toProtoConfig(n.dialTimeout, n.queueTimeout)
	n.mu.RUnlock()
	if cc == nil || cc.isClosed() {
		return ErrNodeDownNewStream
	}
	return cc.sendEnvelope(ctx, proto.TypeReloadConfig, cfg)
}

// PushReloadConfig is the exported form of pushReloadConfig for test/admin use.
func (n *Node) PushReloadConfig(ctx context.Context) error { return n.pushReloadConfig(ctx) }

// Reconfigure replaces the node's port allowlist in place (used by hot reload
// and tests). It does not push to the client; call PushReloadConfig after.
func (n *Node) Reconfigure(ports map[string]string) {
	n.mu.Lock()
	cp := make(map[string]string, len(ports))
	for k, v := range ports {
		cp[k] = v
	}
	n.cfg.Ports = cp
	n.mu.Unlock()
}

// sendDrain asks the client to stop accepting new streams and finish in-flight
// ones before the deadline (graceful shutdown, design §13).
func (n *Node) sendDrain(ctx context.Context, deadline time.Duration) {
	n.mu.RLock()
	cc := n.control
	n.mu.RUnlock()
	if cc == nil || cc.isClosed() {
		return
	}
	_ = cc.sendEnvelope(ctx, proto.TypeDrain, proto.Drain{Deadline: deadline.String()})
}

// shutdown closes the control channel and all data channels (used on Stop).
func (n *Node) shutdown() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.control != nil {
		n.control.close("server shutdown")
		n.control = nil
	}
	for id, ch := range n.channels {
		ch.close()
		n.retireChannelLocked(ch)
		delete(n.channels, id)
	}
	n.session = ""
}
