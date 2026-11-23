package tunnel

import (
	"sync"
	"time"
)

// Traffic rate estimation (design §12 "速率按滑动窗口估算").
//
// The node keeps cumulative byte counters (per live data channel, plus a
// "retired" accumulator for channels that have gone away). A resident sampler
// folds those counters into a rateMeter once per interval; rateMeter turns the
// cumulative total into a bytes/second estimate over a sliding window.
//
// Units: bytes per second (B/s), NOT bits per second. The /status field names
// keep the historical rate_in_bps / rate_out_bps spelling.

const (
	// rateWindowSeconds is the length of the sliding window, in one-second
	// samples. Ten samples smooth out bursty traffic while still decaying to
	// zero within ~10s of the last byte.
	rateWindowSeconds = 10

	// rateSampleInterval is how often the resident sampler samples each node.
	// It should stay well below rateWindowSeconds so the window is well filled.
	rateSampleInterval = time.Second
)

// rateSample is one cumulative-counter snapshot in the window.
type rateSample struct {
	at    time.Time
	total int64
}

// rateMeter estimates a byte rate from a monotonically increasing cumulative
// counter using a sliding window of samples. It is safe for concurrent use.
type rateMeter struct {
	mu      sync.Mutex
	clock   func() time.Time
	samples []rateSample // sequential until full, then a ring
	// next is the write cursor once the ring is full; it is also the index of
	// the oldest sample (the one about to be overwritten).
	next   int
	filled int
	rate   int64 // last estimate, bytes/second
}

func newRateMeter() *rateMeter {
	return &rateMeter{
		clock:   time.Now,
		samples: make([]rateSample, rateWindowSeconds),
	}
}

// observe samples the cumulative total at the meter's clock and returns the
// current estimate in bytes/second.
func (m *rateMeter) observe(total int64) int64 {
	return m.observeAt(m.clock(), total)
}

// observeAt is observe with an explicit timestamp (tests inject a fake clock).
func (m *rateMeter) observeAt(now time.Time, total int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.filled < len(m.samples) {
		m.samples[m.filled] = rateSample{at: now, total: total}
		m.filled++
	} else {
		m.samples[m.next] = rateSample{at: now, total: total}
		m.next = (m.next + 1) % len(m.samples)
	}

	// Base = oldest retained sample. Before the ring is full that is samples[0].
	base := m.samples[0]
	if m.filled == len(m.samples) {
		base = m.samples[m.next]
	}
	elapsed := now.Sub(base.at)
	delta := total - base.total
	if elapsed < time.Millisecond || delta < 0 {
		// delta < 0 can only happen transiently if a channel's counters are
		// retired after the total was already read; report no rate rather than
		// a negative one.
		m.rate = 0
		return m.rate
	}
	m.rate = int64(float64(delta) / elapsed.Seconds())
	return m.rate
}

// last returns the most recent estimate in bytes/second without sampling.
func (m *rateMeter) last() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rate
}

// Rates returns the node's most recent inbound/outbound byte rates (bytes per
// second), as estimated by the resident sampler.
func (n *Node) Rates() (inBps, outBps int64) {
	return n.rateIn.last(), n.rateOut.last()
}

// TrafficTotals returns cumulative bytes in/out for this node, including bytes
// carried by data channels that have since been closed or replaced.
//
// Bytes are counted below smux (design §12), so these are real link bytes, not
// per-stream payload.
func (n *Node) TrafficTotals() (in, out int64) {
	n.mu.RLock()
	in, out = n.retiredIn, n.retiredOut
	for _, ch := range n.channels {
		ci, co := ch.trafficBytes()
		in += ci
		out += co
	}
	n.mu.RUnlock()
	return in, out
}

// retireChannelLocked folds a closing channel's byte counters into the node
// accumulators so cumulative traffic and rate estimation survive channel churn
// (reconnect, replace, grace-expiry teardown).
//
// The caller must hold n.mu for writing, and must call this exactly once per
// channel — at the point the channel leaves n.channels. Retiring under the same
// write lock as the removal keeps readers from double-counting a channel that is
// both live in the map and already retired.
func (n *Node) retireChannelLocked(ch *dataChannel) {
	if ch == nil {
		return
	}
	in, out := ch.trafficBytes()
	n.retiredIn += in
	n.retiredOut += out
}

// sampleTraffic refreshes the node's rate estimates from its cumulative
// counters. Called once per rateSampleInterval by the app's rate sampler.
func (n *Node) sampleTraffic() {
	in, out := n.TrafficTotals()
	n.rateIn.observe(in)
	n.rateOut.observe(out)
}
