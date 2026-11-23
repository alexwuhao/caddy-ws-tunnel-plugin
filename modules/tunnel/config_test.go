package tunnel

import (
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
)

// parseTunnelServer parses a tunnel_server Caddyfile block for tests.
func parseTunnelServer(t *testing.T, input string) *TunnelServer {
	t.Helper()
	mh, err := parseTunnelServerCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(input)})
	if err != nil {
		t.Fatalf("parse tunnel_server: %v", err)
	}
	ts, ok := mh.(*TunnelServer)
	if !ok {
		t.Fatalf("handler type = %T, want *TunnelServer", mh)
	}
	return ts
}

// TestTunnelServerGlobalDirectives covers the three tunnel_server-level tunables
// that used to be documented but not wired (heartbeat/dial_timeout/queue_timeout).
func TestTunnelServerGlobalDirectives(t *testing.T) {
	ts := parseTunnelServer(t, `tunnel_server {
	heartbeat 20s
	dial_timeout 8s
	queue_timeout 3s
	node node1 {
		key xx1
		channels 2
		port 8080
	}
}`)

	if got := time.Duration(ts.Heartbeat); got != 20*time.Second {
		t.Fatalf("heartbeat = %v, want 20s", got)
	}
	if got := time.Duration(ts.DialTimeout); got != 8*time.Second {
		t.Fatalf("dial_timeout = %v, want 8s", got)
	}
	if got := time.Duration(ts.QueueTimeout); got != 3*time.Second {
		t.Fatalf("queue_timeout = %v, want 3s", got)
	}
	if ts.Nodes["node1"] == nil {
		t.Fatal("node1 missing")
	}
}

func TestTunnelServerGlobalDefaults(t *testing.T) {
	ts := parseTunnelServer(t, `tunnel_server {
	node node1 {
		key xx1
		port 8080
	}
}`)
	if ts.Heartbeat != 0 || ts.DialTimeout != 0 || ts.QueueTimeout != 0 {
		t.Fatalf("unset tunables must stay zero (app defaults apply): %v/%v/%v",
			ts.Heartbeat, ts.DialTimeout, ts.QueueTimeout)
	}
	// Parsing fills node defaults so validateNodeConfig sees 4/256.
	n := ts.Nodes["node1"]
	if n.Channels != DefaultChannels || n.MaxStreamsPerConn != DefaultMaxStreamsPerConn {
		t.Fatalf("node defaults not filled: channels=%d max=%d", n.Channels, n.MaxStreamsPerConn)
	}
}

func TestTunnelServerRejectsInvalidDuration(t *testing.T) {
	for _, v := range []string{"heartbeat abc", "dial_timeout -1s", "queue_timeout"} {
		input := "tunnel_server {\n\t" + v + "\n\tnode n1 {\n\t\tkey k\n\t\tport 8080\n\t}\n}"
		if _, err := parseTunnelServerCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(input)}); err == nil {
			t.Fatalf("directive %q should be rejected", v)
		}
	}
}

func TestTunnelServerRejectsZeroChannels(t *testing.T) {
	for _, v := range []string{"channels 0", "channels -2", "max_streams_per_conn 0"} {
		input := `tunnel_server {
	node node1 {
		key xx1
		` + v + `
		port 8080
	}
}`
		_, err := parseTunnelServerCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(input)})
		if err == nil {
			t.Fatalf("%q should be rejected at parse time", v)
		}
	}
}

func TestTunnelServerRejectsNodeWithoutPorts(t *testing.T) {
	input := `tunnel_server {
	node node1 {
		key xx1
	}
}`
	_, err := parseTunnelServerCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(input)})
	if err == nil || !strings.Contains(err.Error(), "at least one 'port'") {
		t.Fatalf("node without ports: err = %v, want at-least-one-port error", err)
	}
}

func TestTunnelServerRejectsUnauthenticatedNode(t *testing.T) {
	// Neither key nor hmac_secret: an empty plaintext key would authenticate any
	// client sending an empty key, so this must be a hard config error.
	input := `tunnel_server {
	node node1 {
		channels 1
		port 8080
	}
}`
	_, err := parseTunnelServerCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(input)})
	if err == nil || !strings.Contains(err.Error(), "hmac_secret") {
		t.Fatalf("unauthenticated node: err = %v, want key-or-hmac_secret error", err)
	}
}

func TestValidateNodeConfig(t *testing.T) {
	base := func() *NodeConfig {
		c := &NodeConfig{Key: "k", Ports: map[string]string{"8080": "127.0.0.1:8080"}}
		c.fill()
		return c
	}
	if err := validateNodeConfig("n1", base()); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	// hmac_secret alone (no key) is a valid posture.
	hmacOnly := base()
	hmacOnly.Key = ""
	hmacOnly.HMACSecret = "s3cret"
	if err := validateNodeConfig("n1", hmacOnly); err != nil {
		t.Fatalf("hmac-only config rejected: %v", err)
	}

	cases := map[string]func(*NodeConfig){
		"no ports":       func(c *NodeConfig) { c.Ports = nil },
		"no auth at all": func(c *NodeConfig) { c.Key = ""; c.HMACSecret = "" },
		"zero max":       func(c *NodeConfig) { c.MaxStreamsPerConn = 0 },
		"named port":     func(c *NodeConfig) { c.Ports = map[string]string{"db": "127.0.0.1:5432"} },
		"zero channel":   func(c *NodeConfig) { c.Channels = 0 },
	}
	for name, mutate := range cases {
		c := base()
		mutate(c)
		if err := validateNodeConfig("n1", c); err == nil {
			t.Fatalf("%s: config should be rejected", name)
		}
	}
}

// TestNodeProtoConfigCarriesTimeouts ensures the welcome/reload_config payload
// mirrors the real app-level timeouts instead of hard-coded defaults.
func TestNodeProtoConfigCarriesTimeouts(t *testing.T) {
	cfg := &NodeConfig{
		Key:               "k",
		Channels:          2,
		MaxStreamsPerConn: 10,
		Ports:             map[string]string{"8080": "127.0.0.1:8080"},
	}
	cfg.fill()

	wire := cfg.toProtoConfig(8*time.Second, 3*time.Second)
	if wire.DialTimeout != "8s" || wire.QueueTimeout != "3s" {
		t.Fatalf("wire timeouts = %q/%q, want 8s/3s", wire.DialTimeout, wire.QueueTimeout)
	}
	if wire.Channels != 2 || wire.MaxStreamsPerConn != 10 || wire.Ports["8080"] != "127.0.0.1:8080" {
		t.Fatalf("wire config lost fields: %+v", wire)
	}

	// Mutating the returned map must not alias the node's allowlist.
	wire.Ports["9090"] = "127.0.0.1:9090"
	if _, ok := cfg.Ports["9090"]; ok {
		t.Fatal("toProtoConfig returned an aliased ports map")
	}
}

func TestAppTimeoutDefaultsAndOverrides(t *testing.T) {
	var a TunnelApp
	if got := a.heartbeat(); got != DefaultHeartbeat {
		t.Fatalf("heartbeat default = %v", got)
	}
	if got := a.dialTimeout(); got != DefaultDialTimeout {
		t.Fatalf("dial_timeout default = %v", got)
	}
	if got := a.queueTimeout(); got != DefaultQueueTimeout {
		t.Fatalf("queue_timeout default = %v", got)
	}

	a.DialTimeout = caddy.Duration(8 * time.Second)
	a.QueueTimeout = caddy.Duration(3 * time.Second)
	a.Heartbeat = caddy.Duration(20 * time.Second)
	if got := a.dialTimeout(); got != 8*time.Second {
		t.Fatalf("dial_timeout override = %v, want 8s", got)
	}
	if got := a.queueTimeout(); got != 3*time.Second {
		t.Fatalf("queue_timeout override = %v, want 3s", got)
	}
	if got := a.heartbeat(); got != 20*time.Second {
		t.Fatalf("heartbeat override = %v, want 20s", got)
	}
}

func TestNodeApplyStats(t *testing.T) {
	n := testNode(t, 1, 4)
	stats := &proto.Stats{Channels: 1, Streams: 7, BytesIn: 100, BytesOut: 200, LocalDialErrors: 3}
	n.applyStats(stats)

	n.mu.RLock()
	got := n.clientStats
	n.mu.RUnlock()
	if got == nil || got.Streams != 7 || got.BytesOut != 200 {
		t.Fatalf("clientStats = %+v, want applied frame", got)
	}
	if errs := n.clientDialErrors; errs != 3 {
		t.Fatalf("clientDialErrors = %d, want 3", errs)
	}

	// Malformed/empty frames must not crash or corrupt state.
	n.applyStats(nil)
	n.applyStats(&proto.Stats{})
	if got := n.clientDialErrors; got != 0 {
		t.Fatalf("clientDialErrors after empty frame = %d, want 0", got)
	}
}
