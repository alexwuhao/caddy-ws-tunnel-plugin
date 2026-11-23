package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func init() {
	caddy.RegisterModule(TunnelTransport{})
}

// TunnelTransport is a reverse_proxy transport that routes requests through the
// tunnel instead of dialing TCP (design §8.4). It reuses stdlib http.Transport
// with DialContext swapped for the tunnel dialer and DisableKeepAlives=true so
// each request maps to exactly one smux stream.
type TunnelTransport struct {
	// Node is the target node name.
	Node string `json:"node,omitempty"`
	// Port is the numeric port id, resolved by the client against its allowlist.
	Port string `json:"port,omitempty"`

	app    *TunnelApp
	portID uint64
	rt     http.RoundTripper
}

// CaddyModule returns the Caddy module information.
func (TunnelTransport) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.transport.tunnel",
		New: func() caddy.Module { return new(TunnelTransport) },
	}
}

// UnmarshalCaddyfile parses both forms:
//
//	transport tunnel node1:8080            # inline
//	transport tunnel {                     # block
//	    node node1
//	    port 8080
//	}
func (t *TunnelTransport) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() { // consume "tunnel"
		if d.NextArg() { // inline node:port
			host, port, found := strings.Cut(d.Val(), ":")
			if !found {
				return d.Errf("inline transport arg must be node:port, got %q", d.Val())
			}
			t.Node, t.Port = host, port
		}
		for d.NextBlock(0) {
			switch d.Val() {
			case "node":
				if !d.NextArg() {
					return d.ArgErr()
				}
				t.Node = d.Val()
			case "port":
				if !d.NextArg() {
					return d.ArgErr()
				}
				t.Port = d.Val()
			default:
				return d.Errf("unknown tunnel transport option %q", d.Val())
			}
		}
	}
	return nil
}

// Provision wires the dialer to the resident app. The node is resolved lazily
// at dial time (not here), because client connections — and thus node
// registration — normally happen long after Caddy provisions, and config
// provision order across servers is not guaranteed.
func (t *TunnelTransport) Provision(ctx caddy.Context) error {
	appIface, err := ctx.App("tunnel")
	if err != nil {
		return fmt.Errorf("loading tunnel app: %w", err)
	}
	t.app = appIface.(*TunnelApp)

	if t.Node == "" || t.Port == "" {
		return errors.New("tunnel transport requires node and port")
	}
	pid, err := strconv.ParseUint(t.Port, 10, 64)
	if err != nil {
		return fmt.Errorf("tunnel transport port %q must be numeric: %w", t.Port, err)
	}
	t.portID = pid

	dialer := &NodeDialer{
		app:      t.app,
		nodeName: t.Node,
		portID:   pid,
	}
	// Timeouts are read from the resident app at dial time (not snapshotted
	// here): they can come from apps.tunnel JSON or from tunnel_server-level
	// Caddyfile directives, and handler vs transport provision order is not
	// guaranteed. ResponseHeaderTimeout is deliberately left at 0 so dial_timeout
	// only bounds stream opening (design §3.4) and a slow backend is not turned
	// into a spurious 502.
	t.rt = &http.Transport{
		DialContext:           dialer.DialContext,
		DisableKeepAlives:     true, // one request = one stream (design §0 #9)
		ExpectContinueTimeout: time.Second,
	}
	return nil
}

// RoundTrip executes the request through the tunnel and maps tunnel sentinel
// errors to the right HTTP status (design §11):
//   - 503 for capacity/retryable (queue timeout, control down)
//   - 502 for backend/tunnel fault (everything else)
func (t *TunnelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// reverse_proxy leaves the scheme empty for non-TLS custom transports; the
	// stdlib http.Transport requires a scheme. The tunnel always carries plain
	// HTTP/1.1 bytes (L4 passthrough), so force "http".
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = t.Node
	}
	resp, err := t.rt.RoundTrip(req)
	if err == nil {
		return resp, nil
	}
	// Capacity/retryable failures (design §11) must surface as 503, but Caddy's
	// reverse_proxy maps every transport error to 502 (or 504 for timeouts) and
	// has no hook for 503. So we synthesize a 503 response instead of returning
	// an error. Backend/tunnel faults stay as errors → 502.
	if errors.Is(err, ErrQueueTimeout) || errors.Is(err, ErrNodeDownNewStream) {
		return newStatusResponse(req, http.StatusServiceUnavailable, err.Error()), nil
	}
	return nil, err // → reverse_proxy renders 502
}

// newStatusResponse builds a minimal HTTP response with the given status and a
// Retry-After hint (capacity errors are retryable).
func newStatusResponse(req *http.Request, code int, msg string) *http.Response {
	body := io.NopCloser(strings.NewReader(msg))
	h := make(http.Header)
	h.Set("Content-Type", "text/plain; charset=utf-8")
	if code == http.StatusServiceUnavailable {
		h.Set("Retry-After", "1")
	}
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     h,
		Body:       body,
		Request:    req,
	}
}

// Interface guards.
var (
	_ caddy.Provisioner     = (*TunnelTransport)(nil)
	_ caddyfile.Unmarshaler = (*TunnelTransport)(nil)
	_ http.RoundTripper     = (*TunnelTransport)(nil)
)
