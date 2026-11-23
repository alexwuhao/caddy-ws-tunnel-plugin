package tunnel

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// testNode builds a Node with control marked up for unit-testing pool logic
// (selection math is tested via reserve/release and peak counters without a
// real smux session).
func testNode(t *testing.T, channels, maxStreams int) *Node {
	t.Helper()
	cfg := &NodeConfig{Channels: channels, MaxStreamsPerConn: maxStreams, Ports: map[string]string{"8080": "127.0.0.1:8080"}}
	cfg.fill()
	n := newNode("node1", cfg, DefaultHeartbeat, zap.NewNop())
	// Mark control up so PickChannel is allowed (non-nil control, not down).
	n.session = "s"
	n.control = &controlChannel{done: make(chan struct{})}
	return n
}

func TestChannelReserveRelease(t *testing.T) {
	ch := &dataChannel{id: 0}
	max := 2
	if !ch.reserve(max) {
		t.Fatal("first reserve should succeed")
	}
	if !ch.reserve(max) {
		t.Fatal("second reserve should succeed")
	}
	if ch.reserve(max) {
		t.Fatal("third reserve should fail (at max)")
	}
	if ch.activeStreams() != 2 {
		t.Fatalf("active = %d, want 2", ch.activeStreams())
	}
	ch.release()
	if ch.activeStreams() != 1 {
		t.Fatalf("active after release = %d, want 1", ch.activeStreams())
	}
	if !ch.reserve(max) {
		t.Fatal("reserve after release should succeed")
	}
}

func TestPickChannelControlDown503(t *testing.T) {
	n := testNode(t, 1, 4)
	n.controlDown = time.Now() // control down
	_, err := n.PickChannel(context.Background(), 100*time.Millisecond)
	if err != ErrNodeDownNewStream {
		t.Fatalf("err = %v, want ErrNodeDownNewStream", err)
	}
}

func TestPickChannelNoOnlineQueueTimeout(t *testing.T) {
	n := testNode(t, 1, 4)
	// No channels online at all → PickChannel should queue then time out 503.
	start := time.Now()
	_, err := n.PickChannel(context.Background(), 150*time.Millisecond)
	if err != ErrQueueTimeout {
		t.Fatalf("err = %v, want ErrQueueTimeout", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned too fast (%v); should have waited ~queue_timeout", elapsed)
	}
}

func TestPeakDemandTracksQueued(t *testing.T) {
	n := testNode(t, 2, 256)
	// Simulate queued demand and record peak.
	n.markQueued(5)
	n.recordPeak()
	if n.peakDemand < 5 {
		t.Fatalf("peak_demand = %d, want >= 5 (queued counted)", n.peakDemand)
	}
	// channels_needed_at_peak = ceil(peak / max_streams_per_conn) = ceil(5/256)=1
	if n.channelsNeededAtPeak != 1 {
		t.Fatalf("channels_needed_at_peak = %d, want 1", n.channelsNeededAtPeak)
	}
	n.markQueued(-5)
}

func TestPeakDemandCeilMath(t *testing.T) {
	n := testNode(t, 2, 4)
	n.markQueued(9) // demand 9 over max 4 → ceil(9/4) = 3
	n.recordPeak()
	if n.channelsNeededAtPeak != 3 {
		t.Fatalf("channels_needed_at_peak = %d, want 3 (ceil 9/4)", n.channelsNeededAtPeak)
	}
	n.markQueued(-9)
}

func TestValidatePortsNumericOnly(t *testing.T) {
	if err := validatePorts("node1", map[string]string{"8080": "x", "9090": "y"}); err != nil {
		t.Fatalf("numeric ports should pass: %v", err)
	}
	if err := validatePorts("node1", map[string]string{"db": "x"}); err == nil {
		t.Fatal("named port 'db' should be rejected (digits only)")
	}
}

func TestNodeConfigDefaults(t *testing.T) {
	c := &NodeConfig{}
	c.fill()
	if c.Channels != DefaultChannels {
		t.Fatalf("channels default = %d, want %d", c.Channels, DefaultChannels)
	}
	if c.MaxStreamsPerConn != DefaultMaxStreamsPerConn {
		t.Fatalf("max_streams default = %d, want %d", c.MaxStreamsPerConn, DefaultMaxStreamsPerConn)
	}
}

func TestCheckGraceExpiry(t *testing.T) {
	cfg := &NodeConfig{Channels: 2, MaxStreamsPerConn: 10}
	cfg.fill()
	graceWindow := 3 * time.Second
	n := newNode("test", cfg, 1*time.Second, zap.NewNop())
	n.graceWindow = graceWindow

	// Simulate control down longer than grace window
	n.mu.Lock()
	n.controlDown = time.Now().Add(-4 * time.Second) // expired
	n.session = "test-session"
	n.control = &controlChannel{done: make(chan struct{})}
	// Add some data channels
	n.channels[0] = &dataChannel{id: 0}
	n.channels[1] = &dataChannel{id: 1}
	n.mu.Unlock()

	// Run grace check
	n.checkGrace()

	// Verify teardown
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.session != "" {
		t.Error("session should be cleared after grace expiry")
	}
	if n.control != nil {
		t.Error("control should be nil after grace expiry")
	}
	if len(n.channels) != 0 {
		t.Errorf("channels should be torn down after grace expiry, got %d", len(n.channels))
	}
}

func TestCheckGraceWithinWindow(t *testing.T) {
	cfg := &NodeConfig{Channels: 2, MaxStreamsPerConn: 10}
	cfg.fill()
	graceWindow := 3 * time.Second
	n := newNode("test", cfg, 1*time.Second, zap.NewNop())
	n.graceWindow = graceWindow

	// Simulate control down but within grace window
	n.mu.Lock()
	n.controlDown = time.Now().Add(-1 * time.Second) // still within 3s grace
	n.session = "test-session"
	n.control = &controlChannel{done: make(chan struct{})}
	n.channels[0] = &dataChannel{id: 0}
	n.mu.Unlock()

	// Run grace check
	n.checkGrace()

	// Verify no teardown
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.session == "" {
		t.Error("session should NOT be cleared within grace window")
	}
	if len(n.channels) == 0 {
		t.Error("channels should NOT be torn down within grace window")
	}
}
