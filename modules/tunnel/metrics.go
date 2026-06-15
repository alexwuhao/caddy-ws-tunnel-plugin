package tunnel

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds all Prometheus collectors. Every metric carries a "node" label
// (design §12). Registration uses a private registerer-safe pattern so multiple
// app instances (reloads) don't panic on duplicate registration.
type metrics struct {
	nodeUp             *prometheus.GaugeVec
	channels           *prometheus.GaugeVec // labels: node, state(configured|online)
	streamsActive      *prometheus.GaugeVec
	streamsPeak        *prometheus.GaugeVec
	channelsNeededPeak *prometheus.GaugeVec
	streamOpenTotal    *prometheus.CounterVec // labels: node, result
	bytesTotal         *prometheus.GaugeVec   // labels: node, dir(in|out)
	saturatedTotal     *prometheus.GaugeVec
	localDialErrors    *prometheus.GaugeVec
}

func newMetrics() *metrics {
	factory := func(name, help string, labels []string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		_ = registerOrExisting(g)
		return g
	}
	m := &metrics{
		nodeUp:             factory("tunnel_node_up", "1 if node control channel is up", []string{"node"}),
		channels:           factory("tunnel_channels", "channel counts by state", []string{"node", "state"}),
		streamsActive:      factory("tunnel_streams_active", "active streams", []string{"node"}),
		streamsPeak:        factory("tunnel_streams_peak", "peak concurrent demand (incl queued)", []string{"node"}),
		channelsNeededPeak: factory("tunnel_channels_needed_peak", "channels needed at peak demand", []string{"node"}),
		bytesTotal:         factory("tunnel_bytes_total", "bytes transferred by direction", []string{"node", "dir"}),
		saturatedTotal:     factory("tunnel_node_saturated_total", "times all channels were saturated", []string{"node"}),
		localDialErrors:    factory("tunnel_local_dial_errors_total", "client-reported local dial errors", []string{"node"}),
	}
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tunnel_stream_open_total", Help: "stream opens by result",
	}, []string{"node", "result"})
	_ = registerOrExistingCounter(c)
	m.streamOpenTotal = c
	return m
}

// registerOrExisting registers a collector, ignoring AlreadyRegisteredError so
// config reloads (which re-provision the app) reuse the existing collector.
func registerOrExisting(c prometheus.Collector) error {
	if err := prometheus.Register(c); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return nil
		}
		return err
	}
	return nil
}

func registerOrExistingCounter(c prometheus.Collector) error {
	return registerOrExisting(c)
}

// update refreshes the per-node gauges from live node state.
func (m *metrics) update(n *Node) {
	n.mu.RLock()
	up := n.control != nil && n.controlDown.IsZero()
	online := 0
	var in, out int64
	for _, ch := range n.channels {
		if ch.online() {
			online++
		}
		bi, bo := ch.trafficBytes()
		in += bi
		out += bo
	}
	configured := n.cfg.Channels
	peak := n.peakDemand
	needed := n.channelsNeededAtPeak
	sat := n.saturatedTotal
	var active int64
	for _, ch := range n.channels {
		active += atomic.LoadInt64(&ch.active)
	}
	name := n.Name
	n.mu.RUnlock()

	upv := 0.0
	if up {
		upv = 1
	}
	m.nodeUp.WithLabelValues(name).Set(upv)
	m.channels.WithLabelValues(name, "configured").Set(float64(configured))
	m.channels.WithLabelValues(name, "online").Set(float64(online))
	m.streamsActive.WithLabelValues(name).Set(float64(active))
	m.streamsPeak.WithLabelValues(name).Set(float64(peak))
	m.channelsNeededPeak.WithLabelValues(name).Set(float64(needed))
	m.bytesTotal.WithLabelValues(name, "in").Set(float64(in))
	m.bytesTotal.WithLabelValues(name, "out").Set(float64(out))
	m.saturatedTotal.WithLabelValues(name).Set(float64(sat))
}
