package tunnel

import (
	"context"
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
	app          *TunnelApp
	nodeName     string
	portID       uint64
	queueTimeout time.Duration
	dialTimeout  time.Duration
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
		return nil, fmt.Errorf("%w: node %q not registered", ErrNodeDownNewStream, d.nodeName)
	}

	qt := d.queueTimeout
	if qt <= 0 {
		qt = DefaultQueueTimeout
	}
	ch, err := node.PickChannel(ctx, qt)
	if err != nil {
		node.setLastError(err.Error())
		return nil, err // sentinel preserved for status mapping
	}

	stream, err := ch.session.OpenStream()
	if err != nil {
		ch.release() // PickChannel reserved a slot; give it back
		node.setLastError(err.Error())
		return nil, fmt.Errorf("open stream: %w", err)
	}

	// Wrap so Close releases the reserved slot exactly once.
	sc := &streamConn{Conn: stream, ch: ch}

	if err := proto.WriteStreamHeader(sc, d.portID); err != nil {
		_ = sc.Close()
		node.setLastError(err.Error())
		return nil, fmt.Errorf("write stream header: %w", err)
	}

	node.incOpened()
	return sc, nil
}
