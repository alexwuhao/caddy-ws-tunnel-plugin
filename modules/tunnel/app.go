package tunnel

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(TunnelApp{})
}

// TunnelApp is the resident Caddy app holding the node registry, the fixed
// connection pools, and the peak/capacity stats. It owns node lifecycle
// (Provision/Start/Stop) and the health-sweep goroutine that enforces the
// control-down grace window (design §8.1).
type TunnelApp struct {
	// Nodes maps node name -> config, parsed from the tunnel_server Caddyfile
	// block (or JSON). The key (node name) plus the per-node Key authenticate
	// clients.
	Nodes map[string]*NodeConfig `json:"nodes,omitempty"`

	// Heartbeat is the control-channel ping interval; grace window = ×3.
	Heartbeat caddy.Duration `json:"heartbeat,omitempty"`

	registry  *NodeRegistry
	logger    *zap.Logger
	metrics   *metrics
	ctx       caddy.Context
	sweepStop chan struct{}
	sweepDone chan struct{}
}

// CaddyModule returns the Caddy module information.
func (TunnelApp) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "tunnel",
		New: func() caddy.Module { return new(TunnelApp) },
	}
}

func (a *TunnelApp) heartbeat() time.Duration {
	if a.Heartbeat > 0 {
		return time.Duration(a.Heartbeat)
	}
	return DefaultHeartbeat
}

// Provision builds the registry from config, validates ports, and registers
// metrics.
func (a *TunnelApp) Provision(ctx caddy.Context) error {
	a.ctx = ctx
	a.logger = ctx.Logger(a)
	a.registry = newNodeRegistry()
	a.metrics = newMetrics()

	hb := a.heartbeat()
	for name, cfg := range a.Nodes {
		if cfg == nil {
			cfg = &NodeConfig{}
			a.Nodes[name] = cfg
		}
		cfg.fill()
		if err := validatePorts(name, cfg.Ports); err != nil {
			return err
		}
		a.registry.put(newNode(name, cfg, hb, a.logger))
	}
	a.logger.Info("tunnel app provisioned",
		zap.Int("nodes", len(a.Nodes)), zap.Duration("heartbeat", hb))
	return nil
}

// validatePorts enforces digits-only port ids (design §0 decision #1).
func validatePorts(node string, ports map[string]string) error {
	for id := range ports {
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			return fmt.Errorf("node %s: port id %q must be numeric", node, id)
		}
	}
	return nil
}

// Start launches the health-sweep goroutine (grace window enforcement + metric
// refresh).
func (a *TunnelApp) Start() error {
	a.sweepStop = make(chan struct{})
	a.sweepDone = make(chan struct{})
	go a.healthSweep()
	lastApp = a
	a.logger.Info("tunnel app started")
	return nil
}

// Stop drains nodes and stops the sweep.
func (a *TunnelApp) Stop() error {
	if a.sweepStop != nil {
		close(a.sweepStop)
		<-a.sweepDone
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, n := range a.registry.all() {
		n.sendDrain(ctx, 2*time.Second)
		n.shutdown()
	}
	a.logger.Info("tunnel app stopped")
	return nil
}

// Node returns the live node by name (used by the transport).
func (a *TunnelApp) Node(name string) (*Node, bool) {
	return a.registry.Get(name)
}

// lastApp records the most recently started app instance so integration tests
// (which boot Caddy via caddy.Run) can reach the live registry. Not used in
// production code paths.
var lastApp *TunnelApp

// LastApp returns the most recently started TunnelApp, for test use only.
func LastApp() *TunnelApp { return lastApp }

// healthSweep periodically enforces grace windows and refreshes metrics.
func (a *TunnelApp) healthSweep() {
	defer close(a.sweepDone)
	interval := a.heartbeat()
	if interval > 5*time.Second {
		interval = 5 * time.Second // sweep at least every 5s for snappy grace handling
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-a.sweepStop:
			return
		case <-t.C:
			for _, n := range a.registry.all() {
				n.checkGrace()
				if a.metrics != nil {
					a.metrics.update(n)
				}
			}
		}
	}
}

// Interface guards.
var (
	_ caddy.App         = (*TunnelApp)(nil)
	_ caddy.Provisioner = (*TunnelApp)(nil)
)
