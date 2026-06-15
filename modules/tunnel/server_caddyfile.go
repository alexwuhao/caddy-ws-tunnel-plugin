package tunnel

import (
	"strconv"

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
//	    node node1 {
//	        key      xx1
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
			if h.Val() != "node" {
				return nil, h.Errf("unexpected token %q; expected 'node'", h.Val())
			}
			if !h.NextArg() {
				return nil, h.ArgErr()
			}
			name := h.Val()
			cfg := &NodeConfig{Ports: map[string]string{}}
			if err := parseNodeBlock(h.Dispenser, cfg); err != nil {
				return nil, err
			}
			s.Nodes[name] = cfg
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
		case "channels":
			if !d.NextArg() {
				return d.ArgErr()
			}
			v, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("channels must be an integer: %v", err)
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
