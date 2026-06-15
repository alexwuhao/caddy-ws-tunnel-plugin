// Package stubclient is a minimal tunnel-agent used only by integration tests.
// It is NOT a deliverable; the real client lives in a separate repo. It mirrors
// the proto contract: control hello/welcome, N data channels bound by session,
// smux client sessions, AcceptStream -> read header -> allowlist -> dial local
// -> io.Copy.
package stubclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
	"github.com/xtaci/smux"
)

// Client is a stub tunnel agent.
type Client struct {
	URL string
	Key string
	// LocalAddrOverride, if set, replaces all allowlist addresses (so tests can
	// point every port id at one echo server).
	LocalAddrOverride string

	mu        sync.Mutex
	allow     map[string]string // port id -> local addr
	session   string
	channels  int
	dataConns []*dataConn
	ctrl      *websocket.Conn

	dialErrors int64
}

type dataConn struct {
	id   int
	conn *websocket.Conn
	sess *smux.Session
}

// Start performs the control handshake, reads welcome, then opens the fixed set
// of data channels and serves them until ctx is cancelled.
func (c *Client) Start(ctx context.Context) error {
	if err := c.connectControl(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	n := c.channels
	c.mu.Unlock()
	for i := 0; i < n; i++ {
		if err := c.connectData(ctx, i); err != nil {
			return fmt.Errorf("data channel %d: %w", i, err)
		}
	}
	return nil
}

// connectControl dials /ws, sends the control hello, and reads welcome.
func (c *Client) connectControl(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, c.URL, nil)
	if err != nil {
		return err
	}
	conn.SetReadLimit(-1)

	if err := c.writeHello(ctx, conn, proto.Hello{
		Role: proto.RoleControl, Node: c.nodeName(), Key: c.Key,
		Nonce: fmt.Sprintf("%d", time.Now().UnixNano()), Ts: time.Now().Unix(),
	}); err != nil {
		return err
	}

	wel, err := readWelcome(ctx, conn)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.ctrl = conn
	c.session = wel.Session
	c.channels = wel.Channels
	c.allow = map[string]string{}
	if wel.Config != nil {
		for k, v := range wel.Config.Ports {
			c.allow[k] = v
		}
	}
	c.mu.Unlock()

	go c.controlLoop(ctx, conn)
	return nil
}

// nodeName is injected by the test via a package-level hook; default "node1".
var NodeNameForKey = func(key string) string { return "node1" }

func (c *Client) nodeName() string { return NodeNameForKey(c.Key) }

func (c *Client) writeHello(ctx context.Context, conn *websocket.Conn, h proto.Hello) error {
	raw, err := proto.WrapEnvelope(proto.TypeHello, h)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageBinary, raw)
}

func readWelcome(ctx context.Context, conn *websocket.Conn) (*proto.Welcome, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("welcome not binary")
	}
	var env proto.Envelope
	if err := jsonUnmarshal(data, &env); err != nil {
		return nil, err
	}
	if env.Type != proto.TypeWelcome {
		return nil, fmt.Errorf("expected welcome, got %q", env.Type)
	}
	var wel proto.Welcome
	if err := jsonUnmarshal(env.Payload, &wel); err != nil {
		return nil, err
	}
	return &wel, nil
}

// controlLoop answers pings and applies reload_config until the conn drops.
func (c *Client) controlLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		var env proto.Envelope
		if jsonUnmarshal(data, &env) != nil {
			continue
		}
		switch env.Type {
		case proto.TypePing:
			var p proto.Ping
			_ = jsonUnmarshal(env.Payload, &p)
			raw, _ := proto.WrapEnvelope(proto.TypePong, proto.Pong{Nonce: p.Nonce, Ts: p.Ts})
			_ = conn.Write(ctx, websocket.MessageBinary, raw)
		case proto.TypeReloadConfig:
			var cfg proto.Config
			if jsonUnmarshal(env.Payload, &cfg) == nil {
				c.mu.Lock()
				c.allow = map[string]string{}
				for k, v := range cfg.Ports {
					c.allow[k] = v
				}
				c.mu.Unlock()
			}
		}
	}
}

// connectData opens one data WS, sends the data hello with the bound session,
// reads the channel welcome, starts a smux client session, and serves it.
func (c *Client) connectData(ctx context.Context, id int) error {
	conn, _, err := websocket.Dial(ctx, c.URL, nil)
	if err != nil {
		return err
	}
	conn.SetReadLimit(-1)

	c.mu.Lock()
	sess := c.session
	c.mu.Unlock()
	if err := c.writeHello(ctx, conn, proto.Hello{
		Role: proto.RoleData, Node: c.nodeName(), Key: c.Key,
		Session: sess, ChannelID: id,
	}); err != nil {
		return err
	}
	if _, err := readWelcome(ctx, conn); err != nil {
		return err
	}

	netConn := websocket.NetConn(ctx, conn, websocket.MessageBinary)
	smuxCfg := smux.DefaultConfig()
	smuxCfg.KeepAliveInterval = 10 * time.Second
	smuxCfg.KeepAliveTimeout = 30 * time.Second
	smuxSess, err := smux.Client(netConn, smuxCfg)
	if err != nil {
		return err
	}

	dc := &dataConn{id: id, conn: conn, sess: smuxSess}
	c.mu.Lock()
	c.dataConns = append(c.dataConns, dc)
	c.mu.Unlock()

	go c.serveData(ctx, smuxSess)
	return nil
}

// serveData accepts streams and forwards each to its local target.
func (c *Client) serveData(ctx context.Context, sess *smux.Session) {
	for {
		stream, err := sess.AcceptStream()
		if err != nil {
			return
		}
		go c.handleStream(stream)
	}
}

// handleStream reads the stream header, resolves the port id against the
// allowlist, dials the local target, and pipes bytes both ways. Port not in
// allowlist -> close stream (server renders 502).
func (c *Client) handleStream(stream *smux.Stream) {
	defer stream.Close()
	br := proto.NewHeaderReader(stream)
	_, portID, err := proto.ReadStreamHeader(br)
	if err != nil {
		return
	}
	idStr := fmt.Sprintf("%d", portID)

	c.mu.Lock()
	addr, ok := c.allow[idStr]
	override := c.LocalAddrOverride
	c.mu.Unlock()
	if !ok {
		return // not in allowlist -> close -> 502
	}
	if override != "" {
		addr = override
	}

	local, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		c.mu.Lock()
		c.dialErrors++
		c.mu.Unlock()
		return // local unreachable -> close -> 502
	}
	defer local.Close()

	done := make(chan struct{}, 2)
	// local <- stream: header reader 'br' holds buffered bytes after header.
	go func() { _, _ = io.Copy(local, br); done <- struct{}{} }()
	go func() { _, _ = io.Copy(stream, local); done <- struct{}{} }()
	<-done
}

// Channels returns the number of data channels established.
func (c *Client) Channels() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.dataConns)
}

// AllowlistSize returns the current allowlist size (updated by reload_config).
func (c *Client) AllowlistSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.allow)
}

// CloseControl closes only the control WS (to test grace-window reconnect).
func (c *Client) CloseControl() {
	c.mu.Lock()
	conn := c.ctrl
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close(websocket.StatusGoingAway, "test close control")
	}
}

// CloseDataChannel closes one data channel by index.
func (c *Client) CloseDataChannel(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx < 0 || idx >= len(c.dataConns) {
		return
	}
	_ = c.dataConns[idx].sess.Close()
	_ = c.dataConns[idx].conn.Close(websocket.StatusGoingAway, "test close data")
}

// ReconnectData reconnects a single data channel by id (for testing re-add).
func (c *Client) ReconnectData(ctx context.Context, id int) error {
	return c.connectData(ctx, id)
}

// ReconnectControl re-establishes only the control channel (for testing the
// grace-window reconnect). Within the server's grace window the session is
// preserved, so existing data channels remain valid.
func (c *Client) ReconnectControl(ctx context.Context) error {
	return c.connectControl(ctx)
}

// Close tears everything down.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctrl != nil {
		_ = c.ctrl.Close(websocket.StatusNormalClosure, "bye")
	}
	for _, dc := range c.dataConns {
		_ = dc.sess.Close()
		_ = dc.conn.Close(websocket.StatusNormalClosure, "bye")
	}
}
