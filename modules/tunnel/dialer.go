package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
)

// NodeDialer turns an HTTP transport DialContext into a tunnel stream open.
// One dialer is bound to one (node, port) pair by the transport at provision
// time. DisableKeepAlives on the transport ensures one request = one stream.
// The node is resolved by name on each dial so it works regardless of when the
// client registers the node.
type NodeDialer struct {
	app      *TunnelApp
	nodeName string
	portID   uint64
}

// DialContext picks a data channel, opens a smux stream, writes the stream
// header (ver + port id), and returns the stream as a net.Conn. Errors are
// shaped so the reverse_proxy layer renders the right status:
//   - ErrQueueTimeout / ErrNodeDownNewStream  → 503 (handled in transport)
//   - all other failures                       → 502
func (d *NodeDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	node, ok := d.app.Node(d.nodeName)
	if !ok {
		// Node not registered yet (client hasn't connected): capacity-class,
		// retryable → 503.
		d.countStreamOpen(d.nodeName, "node_down")
		return nil, fmt.Errorf("%w: node %q not registered", ErrNodeDownNewStream, d.nodeName)
	}

	qt := d.app.queueTimeout()
	ch, err := node.PickChannel(ctx, qt)
	if err != nil {
		node.setLastError(err.Error())
		d.countStreamOpen(d.nodeName, streamOpenResult(err))
		return nil, err // sentinel preserved for status mapping
	}

	stream, err := ch.session.OpenStream()
	if err != nil {
		ch.release() // PickChannel reserved a slot; give it back
		node.setLastError(err.Error())
		d.countStreamOpen(d.nodeName, "open_stream_error")
		return nil, fmt.Errorf("open stream: %w", err)
	}

	// Wrap so Close releases the reserved slot exactly once.
	sc := &streamConn{Conn: stream, ch: ch}

	// Bound the header write: dial_timeout covers "open stream until the header
	// is written" (design §3.4). A wedged peer must not pin the request here;
	// the response itself is not time-limited by the tunnel (use reverse_proxy
	// timeouts if the backend may hang).
	if dt := d.app.dialTimeout(); dt > 0 {
		_ = stream.SetWriteDeadline(time.Now().Add(dt))
	}
	err = proto.WriteStreamHeader(sc, d.portID)
	_ = stream.SetWriteDeadline(time.Time{})
	if err != nil {
		_ = sc.Close()
		node.setLastError(err.Error())
		d.countStreamOpen(d.nodeName, "header_error")
		return nil, fmt.Errorf("write stream header: %w", err)
	}

	node.incOpened()
	d.countStreamOpen(d.nodeName, "ok")
	return sc, nil
}

// streamOpenResult names the metric label for a PickChannel failure.
func streamOpenResult(err error) string {
	switch {
	case errors.Is(err, ErrQueueTimeout):
		return "queue_timeout"
	case errors.Is(err, ErrNodeDownNewStream):
		return "control_down"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "client_canceled"
	default:
		return "pick_error"
	}
}

// countStreamOpen increments tunnel_stream_open_total{node,result} (design §12).
func (d *NodeDialer) countStreamOpen(node, result string) {
	if d.app == nil || d.app.metrics == nil || d.app.metrics.streamOpenTotal == nil {
		return
	}
	d.app.metrics.streamOpenTotal.WithLabelValues(node, result).Inc()
}
