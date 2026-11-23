package tunnel

import (
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func init() {
	httpcaddyfile.RegisterHandlerDirective("tunnel_server", parseTunnelServerCaddyfile)
	httpcaddyfile.RegisterHandlerDirective("tunnel_status", parseTunnelStatusCaddyfile)
}

// parseTunnelServerCaddyfile parses:
//
//	tunnel_server {
//	    heartbeat    15s            # optional: control ping interval (grace = ×3)
//	    dial_timeout 10s            # optional: stream open + header write timeout
//	    queue_timeout 5s            # optional: backpressure queue timeout
//
//	    node node1 {
//	        key      xx1
//	        hmac_secret <secret>
//	        channels 4
//	        port 8080
//	        port 9090 127.0.0.1:9090
//	    }
//	}
func parseTunnelServerCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var s TunnelServer
	s.Nodes = map[string]*NodeConfig{}

	for h.Next() { // consume "tunnel_server"
		for h.NextBlock(0) {
			opt := h.Val()
			switch opt {
			case "heartbeat", "dial_timeout", "queue_timeout":
				// Global tunables, passed to the resident app at Provision time.
				if !h.NextArg() {
					return nil, h.ArgErr()
				}
				d, err := time.ParseDuration(h.Val())
				if err != nil || d < 0 {
					return nil, h.Errf("%s must be a positive duration (e.g. 15s): %v", opt, h.Val())
				}
				switch opt {
				case "heartbeat":
					s.Heartbeat = caddy.Duration(d)
				case "dial_timeout":
					s.DialTimeout = caddy.Duration(d)
				case "queue_timeout":
					s.QueueTimeout = caddy.Duration(d)
				}
			case "node":
				if !h.NextArg() {
					return nil, h.ArgErr()
				}
				name := h.Val()
				cfg := &NodeConfig{Ports: map[string]string{}}
				if err := parseNodeBlock(h.Dispenser, cfg); err != nil {
					return nil, err
				}
				cfg.fill()
				if err := validateNodeConfig(name, cfg); err != nil {
					return nil, h.Errf("%v", err)
				}
				s.Nodes[name] = cfg
			default:
				return nil, h.Errf("unexpected token %q; expected 'node', 'heartbeat', 'dial_timeout' or 'queue_timeout'", opt)
			}
		}
	}
	return &s, nil
}

// parseNodeBlock parses the body of a node block into cfg.
func parseNodeBlock(d *caddyfile.Dispenser, cfg *NodeConfig) error {
	for d.NextBlock(1) {
		switch d.Val() {
		case "key":
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.Key = d.Val()
		case "hmac_secret":
			// Enables HMAC-SHA256 signed hellos for this node (design §10).
			// If key is also present it is kept for reference/migration but is
			// no longer used for authentication.
			if !d.NextArg() {
				return d.ArgErr()
			}
			cfg.HMACSecret = d.Val()
		case "channels":
			if !d.NextArg() {
				return d.ArgErr()
			}
			v, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("channels must be an integer: %v", err)
			}
			// Fixed pool size: 0/negative is a config error, not "use default"
			// (omit the directive to get the default).
			if v <= 0 {
				return d.Errf("channels must be > 0 (got %d); omit it to use the default of %d", v, DefaultChannels)
			}
			cfg.Channels = v
		case "max_streams_per_conn":
			if !d.NextArg() {
				return d.ArgErr()
			}
			v, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("max_streams_per_conn must be an integer: %v", err)
			}
			if v <= 0 {
				return d.Errf("max_streams_per_conn must be > 0 (got %d); omit it to use the default of %d", v, DefaultMaxStreamsPerConn)
			}
			cfg.MaxStreamsPerConn = v
		case "port":
			if !d.NextArg() {
				return d.ArgErr()
			}
			id := d.Val()
			if _, err := strconv.ParseUint(id, 10, 64); err != nil {
				return d.Errf("port id %q must be numeric", id)
			}
			addr := "127.0.0.1:" + id
			if d.NextArg() {
				addr = d.Val() // explicit local address
			}
			cfg.Ports[id] = addr
		default:
			return d.Errf("unknown node option %q", d.Val())
		}
	}
	return nil
}

// parseTunnelStatusCaddyfile parses:
//
//	tunnel_status {
//	    auth_token <token>
//	}
func parseTunnelStatusCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var s TunnelStatus
	for h.Next() {
		for h.NextBlock(0) {
			switch h.Val() {
			case "auth_token":
				if !h.NextArg() {
					return nil, h.ArgErr()
				}
				s.AuthToken = h.Val()
			default:
				return nil, h.Errf("unknown tunnel_status option %q", h.Val())
			}
		}
	}
	return &s, nil
}
