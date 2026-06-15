// Package caddywstunnel is the module entry point for xcaddy builds. Importing
// this package (which xcaddy does via --with
// github.com/wjzhangq/caddy-ws-tunnel-plugin=.) pulls in the tunnel plugin's
// Caddy module registrations.
package caddywstunnel

import (
	// Register all tunnel Caddy modules (app, handlers, transport).
	_ "github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel"
)
