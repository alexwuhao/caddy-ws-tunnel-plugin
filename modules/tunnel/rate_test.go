package tunnel

import (
	"testing"
	"time"
)

// fakeClock drives a rateMeter deterministically.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// newFakeMeter returns a meter whose clock is controlled by the returned clock.
func newFakeMeter() (*rateMeter, *fakeClock) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	m := newRateMeter()
	m.clock = clk.Now
	return m, clk
}

func TestRateMeterFirstSampleIsZero(t *testing.T) {
	m, _ := newFakeMeter()
	if got := m.observe(0); got != 0 {
		t.Fatalf("first sample rate = %d, want 0", got)
	}
	if got := m.last(); got != 0 {
		t.Fatalf("last() = %d, want 0", got)
	}
}

func TestRateMeterSteadyRate(t *testing.T) {
	m, clk := newFakeMeter()
	var total int64
	m.observe(total) // t0 baseline
	const perSec = int64(1024)
	want := int64(0)
	for i := 0; i < 5; i++ {
		total += perSec
		clk.advance(time.Second)
		want = m.observe(total)
	}
	// Within the window the estimate is the average over the elapsed span. With
	// a full window (10 samples would be needed) it converges to perSec; at 5s
	// the average of the accumulated deltas is still perSec because the counter
	// grew linearly.
	if want != perSec {
		t.Fatalf("rate = %d, want %d B/s", want, perSec)
	}
}

func TestRateMeterDecaysToZeroWhenTrafficStops(t *testing.T) {
	m, clk := newFakeMeter()
	var total int64
	m.observe(total)
	for i := 0; i < rateWindowSeconds; i++ {
		total += 2048
		clk.advance(time.Second)
		m.observe(total)
	}
	if got := m.last(); got <= 0 {
		t.Fatalf("rate while transferring = %d, want > 0", got)
	}
	// Counter stops growing: after a full window the estimate must reach zero.
	var got int64
	for i := 0; i < rateWindowSeconds; i++ {
		clk.advance(time.Second)
		got = m.observe(total)
	}
	if got != 0 {
		t.Fatalf("rate after idle window = %d, want 0", got)
	}
}

func TestRateMeterZeroElapsedGuard(t *testing.T) {
	m, _ := newFakeMeter()
	m.observe(100)
	// Same timestamp with a different total: elapsed < 1ms, must not divide by 0
	// and must not report a bogus rate.
	if got := m.observe(999999); got != 0 {
		t.Fatalf("rate with zero elapsed = %d, want 0", got)
	}
}

func TestRateMeterCounterResetGuard(t *testing.T) {
	m, clk := newFakeMeter()
	m.observe(1000)
	clk.advance(time.Second)
	// A decreasing total (e.g. transient accounting edge) must not produce a
	// negative rate.
	if got := m.observe(10); got < 0 {
		t.Fatalf("rate = %d, want >= 0 on counter reset", got)
	}
}

func TestNodeSampleTrafficAndRetire(t *testing.T) {
	n := testNode(t, 1, 4)
	ch := &dataChannel{id: 0}
	ch.bytesIn = 300
	ch.bytesOut = 700

	n.mu.Lock()
	n.channels[0] = ch
	n.mu.Unlock()

	in, out := n.TrafficTotals()
	if in != 300 || out != 700 {
		t.Fatalf("live totals = %d/%d, want 300/700", in, out)
	}

	// Channel goes away: its bytes must be folded into the node accumulators so
	// cumulative traffic does not regress on reconnect.
	n.mu.Lock()
	delete(n.channels, 0)
	n.retireChannelLocked(ch)
	n.mu.Unlock()

	in, out = n.TrafficTotals()
	if in != 300 || out != 700 {
		t.Fatalf("retired totals = %d/%d, want 300/700", in, out)
	}

	// A fresh channel adds on top of the retired bytes.
	ch2 := &dataChannel{id: 1}
	ch2.bytesIn = 5
	n.mu.Lock()
	n.channels[1] = ch2
	n.mu.Unlock()
	if in, _ := n.TrafficTotals(); in != 305 {
		t.Fatalf("combined in = %d, want 305", in)
	}
}
