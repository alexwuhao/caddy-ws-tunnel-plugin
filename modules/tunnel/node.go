package tunnel

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
	"go.uber.org/zap"
)

// Defaults (design §3.4 / dev.md §9).
const (
	DefaultChannels          = 4
	DefaultMaxStreamsPerConn = 256
	DefaultHeartbeat         = 15 * time.Second
	DefaultDialTimeout       = 10 * time.Second
	DefaultQueueTimeout      = 5 * time.Second
)

// Sentinel errors. The dialer maps these to HTTP status (design §11):
//
//	ErrQueueTimeout / ErrNodeDownNewStream -> 503 (capacity, retryable)
//	everything else (open-stream failure, no online channels)  -> 502
var (
	// ErrQueueTimeout: all channels saturated and no slot freed within
	// queue_timeout. Maps to 503.
	ErrQueueTimeout = errors.New("tunnel: queue timeout (all channels saturated)")
	// ErrNodeDownNewStream: control channel is down (within or past grace);
	// new streams are refused. Maps to 503.
	ErrNodeDownNewStream = errors.New("tunnel: node control down, refusing new streams")
	// ErrNoOnlineChannels: node up but zero data channels online. Maps to 502.
	ErrNoOnlineChannels = errors.New("tunnel: no online data channels")
)

// NodeConfig is the per-node configuration parsed from the Caddyfile
// tunnel_server block. Ports is the allowlist: numeric id -> local host:port.
type NodeConfig struct {
	Key               string            `json:"key,omitempty"`
	Channels          int               `json:"channels,omitempty"`
	MaxStreamsPerConn int               `json:"max_streams_per_conn,omitempty"`
	Ports             map[string]string `json:"ports,omitempty"`
}

// fill applies defaults to zero-valued fields.
func (c *NodeConfig) fill() {
	if c.Channels <= 0 {
		c.Channels = DefaultChannels
	}
	if c.MaxStreamsPerConn <= 0 {
		c.MaxStreamsPerConn = DefaultMaxStreamsPerConn
	}
	if c.Ports == nil {
		c.Ports = map[string]string{}
	}
}

// toProtoConfig builds the wire Config pushed to the client inside welcome and
// reload_config.
func (c *NodeConfig) toProtoConfig() *proto.Config {
	ports := make(map[string]string, len(c.Ports))
	for k, v := range c.Ports {
		ports[k] = v
	}
	return &proto.Config{
		Ports:             ports,
		MaxStreamsPerConn: c.MaxStreamsPerConn,
		Channels:          c.Channels,
		DialTimeout:       DefaultDialTimeout.String(),
		QueueTimeout:      DefaultQueueTimeout.String(),
	}
}

// Node is the live runtime state for one configured node: its control channel,
// its fixed set of data channels, the binding session, and capacity stats.
type Node struct {
	Name string
	cfg  *NodeConfig
	log  *zap.Logger

	mu sync.RWMutex
	// control channel
	control      *controlChannel
	session      string    // minted at control attach; data channels must match
	controlDown  time.Time // zero when control is up; set when control drops
	graceWindow  time.Duration
	// data channels keyed by channel id
	channels map[int]*dataChannel

	// capacity / peak stats (design §7.3)
	queued                int64 // requests currently waiting for a slot
	openedTotal           int64
	peakDemand            int64
	channelsNeededAtPeak  int
	saturatedTotal        int64
	lastError             string
}

func newNode(name string, cfg *NodeConfig, hb time.Duration, log *zap.Logger) *Node {
	return &Node{
		Name:        name,
		cfg:         cfg,
		log:         log,
		graceWindow: hb * 3,
		channels:    map[int]*dataChannel{},
	}
}

// onlineChannels returns a snapshot slice of currently-online data channels.
func (n *Node) onlineChannels() []*dataChannel {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]*dataChannel, 0, len(n.channels))
	for _, ch := range n.channels {
		if ch.online() {
			out = append(out, ch)
		}
	}
	return out
}

// controlUp reports whether the control channel is currently considered up
// (attached and not past its grace window).
func (n *Node) controlUp() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.control != nil && n.controlDown.IsZero()
}

// newStreamsAllowed reports whether new streams may be opened. New streams are
// blocked the moment the control channel drops (design §3 decision #3 → 503),
// even though in-flight streams continue during the grace window.
func (n *Node) newStreamsAllowed() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.control != nil && n.controlDown.IsZero()
}

// PickChannel selects the least-loaded online data channel with a free stream
// slot. If all online channels are saturated it waits up to queue_timeout for a
// slot to free (design §7.2). Returns ErrQueueTimeout (→503) on timeout,
// ErrNodeDownNewStream (→503) if control is down, ErrNoOnlineChannels (→502) if
// the node has no online channels at all.
func (n *Node) PickChannel(ctx context.Context, queueTimeout time.Duration) (*dataChannel, error) {
	if !n.newStreamsAllowed() {
		return nil, ErrNodeDownNewStream
	}

	max := n.cfg.MaxStreamsPerConn
	deadline := time.NewTimer(queueTimeout)
	defer deadline.Stop()

	// Count this caller as queued demand for peak stats while it waits.
	n.markQueued(1)
	defer n.markQueued(-1)

	backoff := 2 * time.Millisecond
	const maxBackoff = 50 * time.Millisecond
	for {
		ch := n.leastLoaded(max)
		if ch != nil {
			n.recordPeak()
			return ch, nil
		}
		// No free slot: saturated. Record and wait.
		n.recordSaturated()
		n.recordPeak()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, ErrQueueTimeout
		case <-time.After(backoff):
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
		if !n.newStreamsAllowed() {
			return nil, ErrNodeDownNewStream
		}
	}
}

// leastLoaded returns the online channel with the fewest active streams that is
// still below max, or nil if none has a free slot. It also reserves a slot on
// the chosen channel atomically so concurrent callers don't oversubscribe.
func (n *Node) leastLoaded(max int) *dataChannel {
	chans := n.onlineChannels()
	if len(chans) == 0 {
		return nil
	}
	// Deterministic order for fairness across equal loads.
	sort.Slice(chans, func(i, j int) bool { return chans[i].id < chans[j].id })

	var best *dataChannel
	var bestLoad int
	for _, ch := range chans {
		load := ch.activeStreams()
		if load >= max {
			continue
		}
		if best == nil || load < bestLoad {
			best, bestLoad = ch, load
		}
	}
	if best == nil {
		return nil
	}
	// Try to reserve; if reservation lost a race (now full), retry selection.
	if !best.reserve(max) {
		return nil
	}
	return best
}

func (n *Node) markQueued(delta int64) {
	n.mu.Lock()
	n.queued += delta
	if n.queued < 0 {
		n.queued = 0
	}
	n.mu.Unlock()
}

// recordPeak updates the sliding peak_demand high-water mark = active + queued.
func (n *Node) recordPeak() {
	active := int64(0)
	for _, ch := range n.onlineChannels() {
		active += int64(ch.activeStreams())
	}
	n.mu.Lock()
	demand := active + n.queued
	if demand > n.peakDemand {
		n.peakDemand = demand
	}
	max := n.cfg.MaxStreamsPerConn
	if max > 0 {
		needed := int((n.peakDemand + int64(max) - 1) / int64(max)) // ceil
		n.channelsNeededAtPeak = needed
	}
	n.mu.Unlock()
}

func (n *Node) recordSaturated() {
	n.mu.Lock()
	n.saturatedTotal++
	n.mu.Unlock()
}

func (n *Node) incOpened() {
	n.mu.Lock()
	n.openedTotal++
	n.mu.Unlock()
}

func (n *Node) setLastError(s string) {
	n.mu.Lock()
	n.lastError = s
	n.mu.Unlock()
}

// NodeRegistry holds all configured nodes for the app.
type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

func newNodeRegistry() *NodeRegistry {
	return &NodeRegistry{nodes: map[string]*Node{}}
}

func (r *NodeRegistry) put(n *Node) {
	r.mu.Lock()
	r.nodes[n.Name] = n
	r.mu.Unlock()
}

// Get returns the node by name.
func (r *NodeRegistry) Get(name string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[name]
	return n, ok
}

func (r *NodeRegistry) all() []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
