package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"

	_ "github.com/caddyserver/caddy/v2/modules/standard"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/test/stubclient"
	"go.uber.org/goleak"
)

// TestMain runs goleak after the suite to catch leaked goroutines/conns.
// Caddy and its dependencies spawn long-lived background goroutines (cert
// maintenance, admin server, etc.), so we filter those known sources rather
// than asserting zero goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/caddyserver/certmagic.(*Cache).maintainAssets"),
		goleak.IgnoreTopFunction("github.com/caddyserver/certmagic.keepLockfileFresh"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreAnyFunction("github.com/caddyserver/caddy/v2.(*UsagePool).LoadOrNew"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}

// startEchoServer starts a TCP echo server that echoes an HTTP 200 with the
// request body back. It actually speaks minimal HTTP so the reverse_proxy gets
// a valid response.
func startEchoServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Echo", "1")
		if len(body) == 0 {
			_, _ = io.WriteString(w, "hello from echo")
			return
		}
		_, _ = w.Write(body)
	})}
	go srv.Serve(ln)
	return ln.Addr().String(), func() { _ = srv.Close() }
}

// caddyJSON builds a Caddy JSON config: a plain-HTTP server exposing /ws
// (tunnel_server) and /status (tunnel_status) on wsPort, and a business server
// on proxyPort that reverse-proxies through the tunnel.
func caddyJSON(wsPort, proxyPort, channels, maxStreams int, ports map[string]string, transportPort string) string {
	var portLines []string
	for id, addr := range ports {
		portLines = append(portLines, fmt.Sprintf("%q: %q", id, addr))
	}
	portsJSON := "{" + strings.Join(portLines, ",") + "}"
	node := fmt.Sprintf(`{"key": "xx1", "channels": %d, "max_streams_per_conn": %d, "ports": %s}`,
		channels, maxStreams, portsJSON)
	return caddyJSONWithNode(wsPort, proxyPort, node, transportPort)
}

// caddyJSONWithNode is caddyJSON with the node1 object supplied verbatim, so
// tests can add fields like hmac_secret.
func caddyJSONWithNode(wsPort, proxyPort int, nodeJSON, transportPort string) string {
	return fmt.Sprintf(`{
  "admin": {"disabled": false, "listen": "127.0.0.1:0"},
  "apps": {
    "tunnel": {
      "heartbeat": %d,
      "nodes": {
        "node1": %s
      }
    },
    "http": {
      "servers": {
        "ingress": {
          "listen": [":%d"],
          "routes": [
            {"match": [{"path": ["/ws*"]}], "handle": [{"handler": "tunnel_server"}]},
            {"match": [{"path": ["/status*"]}], "handle": [{"handler": "tunnel_status"}]}
          ]
        },
        "business": {
          "listen": [":%d"],
          "routes": [
            {"handle": [{"handler": "reverse_proxy",
              "transport": {"protocol": "tunnel", "node": "node1", "port": %q},
              "upstreams": [{"dial": "tunnel"}]}]}
          ]
        }
      }
    }
  }
}`, int(15*time.Second), nodeJSON, wsPort, proxyPort, transportPort)
}

// freePortsHandedOut tracks ports already returned by freePort so two servers in
// one test never get the same listening address (the OS can hand the same
// ephemeral port back after the probe listener closes, which makes Caddy reject
// the config with "listener address repeated").
var (
	freePortsMu        sync.Mutex
	freePortsHandedOut = map[int]struct{}{}
)

func freePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 200; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		freePortsMu.Lock()
		_, taken := freePortsHandedOut[p]
		if !taken {
			freePortsHandedOut[p] = struct{}{}
		}
		freePortsMu.Unlock()
		if !taken {
			return p
		}
	}
	t.Fatal("could not find an unused port")
	return 0
}

// setup boots Caddy + stub client + echo, returns proxy base URL and cleanup.
func setup(t *testing.T, channels, maxStreams int, ports map[string]string, transportPort string) (proxyURL, wsURL, statusURL string, client *stubclient.Client, cleanup func()) {
	t.Helper()
	echoAddr, stopEcho := startEchoServer(t)

	wsPort := freePort(t)
	proxyPort := freePort(t)

	cfg := caddyJSON(wsPort, proxyPort, channels, maxStreams, ports, transportPort)
	var cc caddy.Config
	if err := json.Unmarshal([]byte(cfg), &cc); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	if err := caddy.Run(&cc); err != nil {
		t.Fatalf("caddy run: %v", err)
	}

	wsURL = fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort)
	statusURL = fmt.Sprintf("http://127.0.0.1:%d/status", wsPort)
	proxyURL = fmt.Sprintf("http://127.0.0.1:%d", proxyPort)

	client = &stubclient.Client{URL: wsURL, Key: "xx1", LocalAddrOverride: echoAddr}
	ctx, cancel := context.WithCancel(context.Background())
	if err := client.Start(ctx); err != nil {
		cancel()
		stopEcho()
		_ = caddy.Stop()
		t.Fatalf("stub client start: %v", err)
	}

	// Wait for data channels to register server-side.
	waitFor(t, 3*time.Second, func() bool { return client.Channels() == channels })

	cleanup = func() {
		client.Close()
		cancel()
		stopEcho()
		_ = caddy.Stop()
	}
	return
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}

// --- M1: single stream end-to-end ---

func TestM1_SingleStream(t *testing.T) {
	proxyURL, _, _, _, cleanup := setup(t, 1, 256, map[string]string{"8080": "127.0.0.1:8080"}, "8080")
	defer cleanup()

	resp, err := http.Post(proxyURL+"/", "text/plain", strings.NewReader("ping"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ping" {
		t.Fatalf("body = %q, want %q", body, "ping")
	}
}

// --- M2: 50 concurrent over one data channel ---

func TestM2_Concurrent(t *testing.T) {
	proxyURL, _, _, _, cleanup := setup(t, 1, 256, map[string]string{"8080": "127.0.0.1:8080"}, "8080")
	defer cleanup()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := fmt.Sprintf("req-%d", i)
			resp, err := http.Post(proxyURL+"/", "text/plain", strings.NewReader(payload))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 || string(body) != payload {
				errs <- fmt.Errorf("req %d: status=%d body=%q", i, resp.StatusCode, body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// --- M2: port id not in allowlist -> 502 ---

func TestM2_AllowlistMiss(t *testing.T) {
	// Allowlist contains only 8080, but the transport targets 9999.
	proxyURL, _, _, _, cleanup := setup(t, 1, 256,
		map[string]string{"8080": "127.0.0.1:8080"}, "9999")
	defer cleanup()

	resp, err := http.Post(proxyURL+"/", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (port not in allowlist)", resp.StatusCode)
	}
}

// --- M3: capacity backpressure -> 503 on queue timeout ---

func TestM3_QueueTimeout503(t *testing.T) {
	// channels=1, max_streams_per_conn=1 => only ONE concurrent stream. The
	// echo server holds each request briefly so a second concurrent request
	// must queue; with queue_timeout=5s and a slow-but-not-that-slow handler we
	// instead force saturation by blocking the first stream open-ended.
	echoAddr, stopEcho := startEchoServerBlocking(t)
	defer stopEcho()

	wsPort := freePort(t)
	proxyPort := freePort(t)
	cfg := caddyJSON(wsPort, proxyPort, 1, 1, map[string]string{"8080": echoAddr}, "8080")
	var cc caddy.Config
	if err := json.Unmarshal([]byte(cfg), &cc); err != nil {
		t.Fatal(err)
	}
	if err := caddy.Run(&cc); err != nil {
		t.Fatal(err)
	}
	defer caddy.Stop()

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort)
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", proxyPort)
	client := &stubclient.Client{URL: wsURL, Key: "xx1", LocalAddrOverride: echoAddr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, 3*time.Second, func() bool { return client.Channels() == 1 })

	// First request occupies the single stream slot (handler blocks).
	hold := make(chan struct{})
	go func() {
		resp, err := http.Post(proxyURL+"/hold", "text/plain", strings.NewReader("hold"))
		if err == nil {
			resp.Body.Close()
		}
		close(hold)
	}()

	// Give the first request time to occupy the slot.
	time.Sleep(300 * time.Millisecond)

	// Second request must find the channel saturated and 503 after queue_timeout.
	// Use a short client deadline well above queue_timeout (5s default).
	cl := &http.Client{Timeout: 12 * time.Second}
	resp, err := cl.Post(proxyURL+"/second", "text/plain", strings.NewReader("second"))
	if err != nil {
		t.Fatalf("second request error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second status = %d, want 503 (queue timeout)", resp.StatusCode)
	}
}

// startEchoServerBlocking echoes normally but blocks forever on /hold.
func startEchoServerBlocking(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/hold") {
			<-block // never returns during the test
			return
		}
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	})}
	go srv.Serve(ln)
	return ln.Addr().String(), func() { close(block); _ = srv.Close() }
}

// --- M3: peak_demand must not count the acquiring request twice ---

func TestM3_PeakDemandNoDoubleCount(t *testing.T) {
	// A slow echo keeps streams active long enough for concurrent acquisitions to
	// overlap, which is when the queued+active double count would show up.
	echoAddr, stopEcho := startSlowEchoServer(t, 150*time.Millisecond)
	defer stopEcho()

	wsPort := freePort(t)
	proxyPort := freePort(t)
	const conc = 10
	cfg := caddyJSON(wsPort, proxyPort, 2, 256, map[string]string{"8080": echoAddr}, "8080")
	var cc caddy.Config
	if err := json.Unmarshal([]byte(cfg), &cc); err != nil {
		t.Fatal(err)
	}
	if err := caddy.Run(&cc); err != nil {
		t.Fatal(err)
	}
	defer caddy.Stop()

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort)
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", proxyPort)
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", wsPort)
	client := &stubclient.Client{URL: wsURL, Key: "xx1", LocalAddrOverride: echoAddr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, 3*time.Second, func() bool { return client.Channels() == 2 })

	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(proxyURL+"/", "text/plain", strings.NewReader("peak"))
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	peak := statusPeakDemand(t, statusURL)
	// Active-stream demand is at most conc. The old bug also counted each
	// acquiring request as queued, pushing this to roughly 2×conc.
	if peak < conc/2 {
		t.Fatalf("peak_demand = %d, want ~%d (too low: requests did not overlap)", peak, conc)
	}
	if peak > conc+2 {
		t.Fatalf("peak_demand = %d, want <= %d (double counting queued+active?)", peak, conc+2)
	}
}

// startSlowEchoServer serves HTTP 200 after delay, keeping streams active.
func startSlowEchoServer(t *testing.T, delay time.Duration) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(delay)
		_, _ = io.WriteString(w, "slow-ok")
	})}
	go srv.Serve(ln)
	return ln.Addr().String(), func() { _ = srv.Close() }
}

func statusPeakDemand(t *testing.T, statusURL string) int64 {
	t.Helper()
	resp, err := http.Get(statusURL + "?node=node1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Nodes []struct {
			Capacity struct {
				PeakDemand int64 `json:"peak_demand"`
			} `json:"capacity"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || len(out.Nodes) == 0 {
		t.Fatalf("decode status: %v", err)
	}
	return out.Nodes[0].Capacity.PeakDemand
}

// --- M4: /status shape ---

func TestM4_Status(t *testing.T) {
	_, _, statusURL, _, cleanup := setup(t, 2, 256,
		map[string]string{"8080": "127.0.0.1:8080", "9090": "127.0.0.1:9090"}, "8080")
	defer cleanup()

	for _, url := range []string{statusURL, statusURL + "?node=node1"} {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Nodes []struct {
				Node     string `json:"node"`
				Up       bool   `json:"up"`
				Channels struct {
					Configured int `json:"configured"`
					Online     int `json:"online"`
				} `json:"channels"`
				Capacity struct {
					MaxConcurrent int `json:"max_concurrent"`
				} `json:"capacity"`
				Ports []string `json:"ports"`
			} `json:"nodes"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode status: %v", err)
		}
		resp.Body.Close()
		if len(out.Nodes) != 1 {
			t.Fatalf("%s: nodes = %d, want 1", url, len(out.Nodes))
		}
		n := out.Nodes[0]
		if n.Node != "node1" || !n.Up {
			t.Fatalf("%s: node=%q up=%v", url, n.Node, n.Up)
		}
		if n.Channels.Configured != 2 || n.Channels.Online != 2 {
			t.Fatalf("%s: channels configured=%d online=%d, want 2/2", url, n.Channels.Configured, n.Channels.Online)
		}
		if n.Capacity.MaxConcurrent != 2*256 {
			t.Fatalf("%s: max_concurrent=%d, want %d", url, n.Capacity.MaxConcurrent, 2*256)
		}
		if len(n.Ports) != 2 {
			t.Fatalf("%s: ports=%v, want 2", url, n.Ports)
		}
	}
}

// --- M4: traffic rate metrics come from the sliding window, not hard-coded 0 ---

func TestM4_TrafficRateMetrics(t *testing.T) {
	proxyURL, _, statusURL, _, cleanup := setup(t, 1, 256,
		map[string]string{"8080": "127.0.0.1:8080"}, "8080")
	defer cleanup()

	// Keep traffic flowing across several 1s sampler ticks: a burst shorter than
	// the first sample interval could land entirely inside one sample and then
	// legitimately show a zero window delta.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			resp, err := http.Post(proxyURL+"/", "text/plain", strings.NewReader("rate-probe-payload"))
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	deadline := time.Now().Add(15 * time.Second)
	var rateIn, rateOut int64
	for time.Now().Before(deadline) {
		rateIn, rateOut = statusRates(t, statusURL)
		if rateIn > 0 && rateOut > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	<-done
	if rateIn <= 0 || rateOut <= 0 {
		t.Fatalf("rate_in_bps=%d rate_out_bps=%d, want both > 0 (sliding window)", rateIn, rateOut)
	}

	// Cumulative byte counters must be non-zero and must not regress once the
	// traffic stops (bytes from retired channels are folded into the node).
	bytesIn1, bytesOut1 := statusBytes(t, statusURL)
	if bytesIn1 <= 0 || bytesOut1 <= 0 {
		t.Fatalf("bytes_in=%d bytes_out=%d, want both > 0", bytesIn1, bytesOut1)
	}
	time.Sleep(200 * time.Millisecond)
	bytesIn2, bytesOut2 := statusBytes(t, statusURL)
	if bytesIn2 < bytesIn1 || bytesOut2 < bytesOut1 {
		t.Fatalf("cumulative bytes regressed: in %d->%d out %d->%d", bytesIn1, bytesIn2, bytesOut1, bytesOut2)
	}
}

// statusTraffic is the subset of /status traffic fields the tests care about.
type statusTraffic struct {
	BytesIn    int64 `json:"bytes_in"`
	BytesOut   int64 `json:"bytes_out"`
	RateInBps  int64 `json:"rate_in_bps"`
	RateOutBps int64 `json:"rate_out_bps"`
}

func getStatusTraffic(t *testing.T, statusURL string) (statusTraffic, bool) {
	t.Helper()
	resp, err := http.Get(statusURL + "?node=node1")
	if err != nil {
		return statusTraffic{}, false
	}
	defer resp.Body.Close()
	var out struct {
		Nodes []struct {
			Traffic statusTraffic `json:"traffic"`
		} `json:"nodes"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil || len(out.Nodes) == 0 {
		return statusTraffic{}, false
	}
	return out.Nodes[0].Traffic, true
}

func statusRates(t *testing.T, statusURL string) (inBps, outBps int64) {
	t.Helper()
	tr, ok := getStatusTraffic(t, statusURL)
	if !ok {
		return 0, 0
	}
	return tr.RateInBps, tr.RateOutBps
}

func statusBytes(t *testing.T, statusURL string) (in, out int64) {
	t.Helper()
	tr, ok := getStatusTraffic(t, statusURL)
	if !ok {
		return 0, 0
	}
	return tr.BytesIn, tr.BytesOut
}

// --- M5: HMAC-signed hellos work end-to-end; plaintext clients are refused ---

func TestM5_HMACHelloEndToEnd(t *testing.T) {
	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()

	wsPort := freePort(t)
	proxyPort := freePort(t)
	const secret = "test-hmac-secret-0123456789abcdef0123456789abcdef"
	node := fmt.Sprintf(`{"key": "unused-in-hmac", "hmac_secret": %q, "channels": 1,
		"max_streams_per_conn": 256, "ports": {"8080": %q}}`, secret, echoAddr)
	cfg := caddyJSONWithNode(wsPort, proxyPort, node, "8080")
	var cc caddy.Config
	if err := json.Unmarshal([]byte(cfg), &cc); err != nil {
		t.Fatal(err)
	}
	if err := caddy.Run(&cc); err != nil {
		t.Fatal(err)
	}
	defer caddy.Stop()

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort)
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", proxyPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A properly signed client authenticates and routes traffic.
	signed := &stubclient.Client{URL: wsURL, Key: "unused-in-hmac", HMACSecret: secret, LocalAddrOverride: echoAddr}
	if err := signed.Start(ctx); err != nil {
		t.Fatalf("signed client start: %v", err)
	}
	defer signed.Close()
	waitFor(t, 3*time.Second, func() bool { return signed.Channels() == 1 })
	if code := postCode(t, proxyURL, "hmac-ok"); code != 200 {
		t.Fatalf("signed request status = %d, want 200", code)
	}

	// A plaintext client must be refused even though it sends the same key value
	// the config carries: hmac_secret makes the node signature-only.
	plain := &stubclient.Client{URL: wsURL, Key: "unused-in-hmac", LocalAddrOverride: echoAddr}
	if err := plain.Start(ctx); err == nil {
		plain.Close()
		t.Fatal("plaintext client must be rejected by an HMAC node")
	}

	// A signed client using the wrong secret must be refused too.
	wrong := &stubclient.Client{URL: wsURL, Key: "unused-in-hmac", HMACSecret: "wrong-secret", LocalAddrOverride: echoAddr}
	if err := wrong.Start(ctx); err == nil {
		wrong.Close()
		t.Fatal("client with wrong hmac secret must be rejected")
	}
}

// --- M5: data channel death -> re-add restores online set ---

func TestM5_DataChannelReAdd(t *testing.T) {
	proxyURL, wsURL, statusURL, client, cleanup := setup(t, 2, 256,
		map[string]string{"8080": "127.0.0.1:8080"}, "8080")
	defer cleanup()
	_ = wsURL

	// Sanity: a request works.
	if code := postCode(t, proxyURL, "a"); code != 200 {
		t.Fatalf("pre-kill status = %d", code)
	}

	// Kill one data channel; online should drop to 1 then the stub reconnects.
	client.CloseDataChannel(0)
	waitFor(t, 3*time.Second, func() bool { return onlineChannels(t, statusURL) == 1 })

	// Stub re-adds the channel.
	ctx := context.Background()
	if err := client.ReconnectData(ctx, 0); err != nil {
		t.Fatalf("reconnect data: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return onlineChannels(t, statusURL) == 2 })

	if code := postCode(t, proxyURL, "b"); code != 200 {
		t.Fatalf("post-readd status = %d", code)
	}
}

func postCode(t *testing.T, proxyURL, body string) int {
	t.Helper()
	resp, err := http.Post(proxyURL+"/", "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func onlineChannels(t *testing.T, statusURL string) int {
	t.Helper()
	resp, err := http.Get(statusURL + "?node=node1")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var out struct {
		Nodes []struct {
			Channels struct {
				Online int `json:"online"`
			} `json:"channels"`
		} `json:"nodes"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil || len(out.Nodes) == 0 {
		return -1
	}
	return out.Nodes[0].Channels.Online
}

func nodeUp(t *testing.T, statusURL string) bool {
	t.Helper()
	resp, err := http.Get(statusURL + "?node=node1")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var out struct {
		Nodes []struct {
			Up bool `json:"up"`
		} `json:"nodes"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil || len(out.Nodes) == 0 {
		return false
	}
	return out.Nodes[0].Up
}

// --- M5: control-drop grace window -> reconnect restores routing ---

func TestM5_ControlGraceReconnect(t *testing.T) {
	proxyURL, _, statusURL, client, cleanup := setup(t, 1, 256,
		map[string]string{"8080": "127.0.0.1:8080"}, "8080")
	defer cleanup()

	if !nodeUp(t, statusURL) {
		t.Fatal("node should be up initially")
	}

	// Drop the control channel. New streams should immediately 503, but the
	// data channel stays online during the grace window (heartbeat×3 = 45s).
	client.CloseControl()
	waitFor(t, 3*time.Second, func() bool { return !nodeUp(t, statusURL) })

	// New request during control-down → 503 (capacity / not 502).
	if code := postCode(t, proxyURL, "x"); code != http.StatusServiceUnavailable {
		t.Fatalf("control-down status = %d, want 503", code)
	}
	// Data channel should still be online (grace window keeps it).
	if onlineChannels(t, statusURL) != 1 {
		t.Fatalf("data channel should survive grace window, online=%d", onlineChannels(t, statusURL))
	}

	// Reconnect control within the grace window → routing restored.
	if err := client.ReconnectControl(context.Background()); err != nil {
		t.Fatalf("reconnect control: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return nodeUp(t, statusURL) })

	if code := postCode(t, proxyURL, "y"); code != 200 {
		t.Fatalf("post-reconnect status = %d, want 200", code)
	}
}

// --- M5: reload_config pushes a new allowlist without reconnect ---

func TestM5_ReloadConfig(t *testing.T) {
	proxyURL, _, statusURL, client, cleanup := setup(t, 1, 256,
		map[string]string{"8080": "127.0.0.1:8080"}, "8080")
	defer cleanup()
	_ = proxyURL

	// Drive a reload that adds port 9090 to the allowlist, then confirm the
	// stub received it (without reconnecting).
	app := tunnel.LastApp()
	if app == nil {
		t.Fatal("no running tunnel app")
	}
	node, ok := app.Node("node1")
	if !ok {
		t.Fatal("node1 not found in registry")
	}
	node.Reconfigure(map[string]string{"8080": "127.0.0.1:8080", "9090": "127.0.0.1:9090"})
	if err := node.PushReloadConfig(context.Background()); err != nil {
		t.Fatalf("push reload: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return client.AllowlistSize() == 2 })
	_ = statusURL
}
