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
	// DialTimeout bounds opening a stream and writing its header, and the
	// reverse_proxy response-header wait (design §3.4). Zero = default.
	DialTimeout caddy.Duration `json:"dial_timeout,omitempty"`
	// QueueTimeout bounds how long a request waits for a free stream slot when
	// every data channel is saturated (design §7.2). Zero = default.
	QueueTimeout caddy.Duration `json:"queue_timeout,omitempty"`

	registry  *NodeRegistry
	logger    *zap.Logger
	metrics   *metrics
	ctx       caddy.Context
	sweepStop chan struct{}
	sweepDone chan struct{}

	// rate sampler lifecycle (design §12: rates estimated over a sliding
	// window instead of being reported as zero).
	rateStop chan struct{}
	rateDone chan struct{}
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

// dialTimeout returns the configured stream-open timeout or its default.
func (a *TunnelApp) dialTimeout() time.Duration {
	if a.DialTimeout > 0 {
		return time.Duration(a.DialTimeout)
	}
	return DefaultDialTimeout
}

// queueTimeout returns the configured backpressure queue timeout or its default.
func (a *TunnelApp) queueTimeout() time.Duration {
	if a.QueueTimeout > 0 {
		return time.Duration(a.QueueTimeout)
	}
	return DefaultQueueTimeout
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
		if err := validateNodeConfig(name, cfg); err != nil {
			return err
		}
		nd := newNode(name, cfg, hb, a.logger)
		nd.dialTimeout = a.dialTimeout()
		nd.queueTimeout = a.queueTimeout()
		a.registry.put(nd)
	}
	a.logger.Info("tunnel app provisioned",
		zap.Int("nodes", len(a.Nodes)), zap.Duration("heartbeat", hb),
		zap.Duration("dial_timeout", a.dialTimeout()), zap.Duration("queue_timeout", a.queueTimeout()))
	return nil
}

// validateNodeConfig rejects node configs that cannot work or that would silently
// fall back to defaults the operator did not ask for (design §3.1/§3.4).
func validateNodeConfig(name string, cfg *NodeConfig) error {
	if len(cfg.Ports) == 0 {
		return fmt.Errorf("node %s: at least one 'port' mapping is required", name)
	}
	// Never allow an unauthenticated node: an empty key with no hmac_secret would
	// let any client with an empty key through (empty == empty).
	if cfg.Key == "" && cfg.HMACSecret == "" {
		return fmt.Errorf("node %s: set 'key' (plaintext auth) or 'hmac_secret' (HMAC auth)", name)
	}
	if cfg.Channels <= 0 {
		return fmt.Errorf("node %s: channels must be > 0", name)
	}
	if cfg.MaxStreamsPerConn <= 0 {
		return fmt.Errorf("node %s: max_streams_per_conn must be > 0", name)
	}
	return validatePorts(name, cfg.Ports)
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
// refresh) and the traffic-rate sampler (design §12).
func (a *TunnelApp) Start() error {
	a.sweepStop = make(chan struct{})
	a.sweepDone = make(chan struct{})
	go a.healthSweep()

	a.rateStop = make(chan struct{})
	a.rateDone = make(chan struct{})
	go a.rateSampler()

	lastApp = a
	a.logger.Info("tunnel app started")
	return nil
}

// Stop drains nodes and stops the background goroutines.
func (a *TunnelApp) Stop() error {
	if a.sweepStop != nil {
		close(a.sweepStop)
		<-a.sweepDone
	}
	if a.rateStop != nil {
		close(a.rateStop)
		<-a.rateDone
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

// rateSampler folds each node's cumulative byte counters into its sliding-window
// rate meters once per rateSampleInterval (design §12). The window is short and
// the sample cheap (a few atomic loads), so this runs independently of the
// health sweep to keep rate resolution at ~1s regardless of heartbeat.
func (a *TunnelApp) rateSampler() {
	defer close(a.rateDone)
	t := time.NewTicker(rateSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-a.rateStop:
			return
		case <-t.C:
			for _, n := range a.registry.all() {
				n.sampleTraffic()
			}
		}
	}
}

// Interface guards.
var (
	_ caddy.App         = (*TunnelApp)(nil)
	_ caddy.Provisioner = (*TunnelApp)(nil)
)
