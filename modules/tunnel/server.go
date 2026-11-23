package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/coder/websocket"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(TunnelServer{})
}

// TunnelServer is the /ws HTTP handler that accepts client WS connections,
// reads the hello, authenticates, and routes to control/data attach
// (design §8.2). Node configuration lives on this handler's block and is
// installed into the resident TunnelApp at provision time.
type TunnelServer struct {
	// NodesRaw holds node configs parsed from the Caddyfile block. They are
	// merged into the app on Provision.
	Nodes map[string]*NodeConfig `json:"nodes,omitempty"`

	// Optional global tunables set as tunnel_server Caddyfile directives. They
	// override the resident app's defaults at Provision time (JSON users set the
	// same knobs in apps.tunnel instead). Zero means "keep app/default value".
	Heartbeat    caddy.Duration `json:"heartbeat,omitempty"`
	DialTimeout  caddy.Duration `json:"dial_timeout,omitempty"`
	QueueTimeout caddy.Duration `json:"queue_timeout,omitempty"`

	app *TunnelApp
	log *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (TunnelServer) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.tunnel_server",
		New: func() caddy.Module { return new(TunnelServer) },
	}
}

// Provision fetches (creating if needed) the resident tunnel app and registers
// this handler's nodes into it.
func (h *TunnelServer) Provision(ctx caddy.Context) error {
	h.log = ctx.Logger(h)
	appIface, err := ctx.App("tunnel")
	if err != nil {
		return fmt.Errorf("loading tunnel app: %w", err)
	}
	h.app = appIface.(*TunnelApp)

	// Apply tunnel_server-level tunables before nodes are created below, so the
	// per-node grace window and dial/queue timeouts see them.
	if h.Heartbeat > 0 {
		h.app.Heartbeat = h.Heartbeat
	}
	if h.DialTimeout > 0 {
		h.app.DialTimeout = h.DialTimeout
	}
	if h.QueueTimeout > 0 {
		h.app.QueueTimeout = h.QueueTimeout
	}

	// Merge handler-declared nodes into the app registry so a standalone
	// tunnel_server block works without a separate global app config.
	hb := h.app.heartbeat()
	for name, cfg := range h.Nodes {
		if cfg == nil {
			cfg = &NodeConfig{}
		}
		cfg.fill()
		if err := validateNodeConfig(name, cfg); err != nil {
			return err
		}
		if h.app.Nodes == nil {
			h.app.Nodes = map[string]*NodeConfig{}
		}
		h.app.Nodes[name] = cfg
		if _, ok := h.app.registry.Get(name); !ok {
			nd := newNode(name, cfg, hb, h.log)
			nd.dialTimeout = h.app.dialTimeout()
			nd.queueTimeout = h.app.queueTimeout()
			h.app.registry.put(nd)
		}
	}
	return nil
}

// ServeHTTP upgrades to WebSocket, reads the hello envelope, authenticates, and
// dispatches by role. It blocks for the lifetime of the channel.
func (h *TunnelServer) ServeHTTP(w http.ResponseWriter, r *http.Request, _ caddyhttp.Handler) error {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Binary stream mode; no compression for raw byte pipe.
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return err
	}
	// Disable the read size limit: smux frames over data channels exceed the
	// default 32KiB cap. (Control messages are small, but the same conn type.)
	c.SetReadLimit(-1)

	ctx := r.Context()
	hello, err := readHello(ctx, c)
	if err != nil {
		_ = c.Close(websocket.StatusPolicyViolation, "bad hello")
		return nil
	}

	// AuthenticateHello enforces the node's configured posture: HMAC signature
	// on both control- and data-channel hellos when hmac_secret is set,
	// otherwise the legacy plaintext key (constant-time compare).
	if err := h.app.AuthenticateHello(hello); err != nil {
		h.log.Warn("tunnel auth failed",
			zap.String("node", hello.Node),
			zap.String("role", hello.Role),
			zap.String("remote", r.RemoteAddr),
			zap.Error(err))
		// The close reason is deliberately generic: diagnostics go to the log,
		// never to an unauthenticated peer (no secret or replay hints leak).
		_ = c.Close(websocket.StatusPolicyViolation, "auth failed")
		return nil
	}
	node, ok := h.app.Node(hello.Node)
	if !ok {
		_ = c.Close(websocket.StatusPolicyViolation, "unknown node")
		return nil
	}

	switch hello.Role {
	case proto.RoleControl:
		return node.attachControl(ctx, c, h.app.heartbeat(), h.log)
	case proto.RoleData:
		return node.attachData(ctx, c, hello, h.log)
	default:
		_ = c.Close(websocket.StatusUnsupportedData, "unknown role")
		return nil
	}
}

// readHello reads the first binary WS message and decodes it as a hello
// envelope. The read limit must already be lifted by the caller.
func readHello(ctx context.Context, c *websocket.Conn) (*proto.Hello, error) {
	typ, data, err := c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("hello must be binary message, got %v", typ)
	}
	var env proto.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if env.Type != proto.TypeHello {
		return nil, fmt.Errorf("first message must be hello, got %q", env.Type)
	}
	var hello proto.Hello
	if err := json.Unmarshal(env.Payload, &hello); err != nil {
		return nil, fmt.Errorf("decode hello: %w", err)
	}
	return &hello, nil
}

// Interface guards.
var (
	_ caddy.Provisioner           = (*TunnelServer)(nil)
	_ caddyhttp.MiddlewareHandler = (*TunnelServer)(nil)
)
