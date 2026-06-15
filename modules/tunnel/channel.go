package tunnel

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/xtaci/smux"
)

// countingConn wraps a net.Conn and tallies bytes read/written. It sits BELOW
// smux (design §12) so it measures real link bytes, not per-stream payload.
type countingConn struct {
	net.Conn
	bytesIn  *int64 // bytes read from the wire (client -> server)
	bytesOut *int64 // bytes written to the wire (server -> client)
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		atomic.AddInt64(c.bytesIn, int64(n))
	}
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		atomic.AddInt64(c.bytesOut, int64(n))
	}
	return n, err
}

// dataChannel is one WS-backed smux session carrying many streams.
type dataChannel struct {
	id      int
	session *smux.Session
	raw     net.Conn // the underlying counting conn (for byte stats / close)

	bytesIn  int64
	bytesOut int64

	active int64 // active stream count (atomic)
	closed int32 // atomic flag
}

// newDataChannel wraps an already-accepted WS net.Conn in a counting conn and
// starts a smux *server* session on it (server opens streams; client accepts).
func newDataChannel(id int, wsConn net.Conn, cfg *smux.Config) (*dataChannel, error) {
	ch := &dataChannel{id: id}
	cc := &countingConn{Conn: wsConn, bytesIn: &ch.bytesIn, bytesOut: &ch.bytesOut}
	ch.raw = cc
	sess, err := smux.Server(cc, cfg)
	if err != nil {
		return nil, err
	}
	ch.session = sess
	return ch, nil
}

func (c *dataChannel) online() bool {
	return atomic.LoadInt32(&c.closed) == 0 && c.session != nil && !c.session.IsClosed()
}

func (c *dataChannel) activeStreams() int {
	return int(atomic.LoadInt64(&c.active))
}

// reserve atomically claims a stream slot if the channel is below max.
// Returns false if claiming would exceed max (caller should retry elsewhere).
func (c *dataChannel) reserve(max int) bool {
	for {
		cur := atomic.LoadInt64(&c.active)
		if int(cur) >= max {
			return false
		}
		if atomic.CompareAndSwapInt64(&c.active, cur, cur+1) {
			return true
		}
	}
}

// release returns a previously-reserved slot.
func (c *dataChannel) release() {
	atomic.AddInt64(&c.active, -1)
}

func (c *dataChannel) close() {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		if c.session != nil {
			_ = c.session.Close()
		}
	}
}

func (c *dataChannel) trafficBytes() (in, out int64) {
	return atomic.LoadInt64(&c.bytesIn), atomic.LoadInt64(&c.bytesOut)
}

// streamConn wraps a smux stream so closing it releases the channel's reserved
// slot exactly once. This is the net.Conn handed to http.Transport.
type streamConn struct {
	net.Conn
	ch       *dataChannel
	once     sync.Once
}

func (s *streamConn) Close() error {
	err := s.Conn.Close()
	s.once.Do(s.ch.release)
	return err
}
