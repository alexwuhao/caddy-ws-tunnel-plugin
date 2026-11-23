package tunnel

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
)

// testAuthApp returns an app with one legacy plaintext node plus two HMAC nodes
// that share a secret. The second HMAC node proves nonces are namespaced per
// node rather than globally.
func testAuthApp() *TunnelApp {
	return &TunnelApp{Nodes: map[string]*NodeConfig{
		"plain": {Key: "plain-key"},
		"hmac":  {Key: "legacy-key", HMACSecret: "hmac-secret"},
		"hmac2": {HMACSecret: "hmac-secret"},
	}}
}

// signedHello builds a hello signed exactly per the proto contract.
func signedHello(secret, node, role, nonce string) *proto.Hello {
	ts := time.Now().Unix()
	return &proto.Hello{
		Role:  role,
		Node:  node,
		Ts:    ts,
		Nonce: nonce,
		Sig:   proto.SignHello(secret, node, ts, nonce),
	}
}

func requireAuthErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want errors.Is(..., %v)", err, want)
	}
}

// TestAuthenticatePlaintext covers the backward-compatible legacy posture:
// a matching plaintext key authenticates both channel roles.
func TestAuthenticatePlaintext(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	if !app.Authenticate("plain", "plain-key") {
		t.Fatal("Authenticate: matching plaintext key should succeed")
	}
	for _, role := range []string{proto.RoleControl, proto.RoleData} {
		h := &proto.Hello{Role: role, Node: "plain", Key: "plain-key"}
		if role == proto.RoleData {
			h.Session = "session-1"
			h.ChannelID = 1
		}
		if err := app.AuthenticateHello(h); err != nil {
			t.Fatalf("role %s: plaintext hello should authenticate: %v", role, err)
		}
	}
}

// TestAuthenticatePlaintextFailures covers wrong key, unknown node and nil
// hello in the legacy posture.
func TestAuthenticatePlaintextFailures(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	if app.Authenticate("plain", "wrong-key") {
		t.Fatal("Authenticate: wrong key must fail")
	}
	requireAuthErr(t,
		app.AuthenticateHello(&proto.Hello{Node: "plain", Key: "wrong-key"}),
		ErrKeyMismatch)

	requireAuthErr(t,
		app.AuthenticateHello(&proto.Hello{Node: "does-not-exist", Key: "plain-key"}),
		ErrUnknownNode)
	requireAuthErr(t, app.AuthenticateHello(nil), ErrUnknownNode)
}

// TestAuthenticateHMACSuccess covers signed control- and data-channel hellos.
// The plaintext Key field is omitted: only the signature matters in HMAC mode.
func TestAuthenticateHMACSuccess(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	ctrl := signedHello("hmac-secret", "hmac", proto.RoleControl, "ctrl-nonce-1")
	if err := app.AuthenticateHello(ctrl); err != nil {
		t.Fatalf("signed control hello should authenticate: %v", err)
	}

	data := signedHello("hmac-secret", "hmac", proto.RoleData, "data-nonce-1")
	data.Session = "session-1"
	data.ChannelID = 2
	if err := app.AuthenticateHello(data); err != nil {
		t.Fatalf("signed data hello should authenticate: %v", err)
	}

	// Uppercase hex is the same MAC and must be accepted.
	upper := signedHello("hmac-secret", "hmac", proto.RoleControl, "upper-nonce")
	upper.Sig = strings.ToUpper(upper.Sig)
	if err := app.AuthenticateHello(upper); err != nil {
		t.Fatalf("uppercase hex signature should authenticate: %v", err)
	}
}

// TestHMACSignatureRejected covers wrong secret, tampered canonical fields,
// malformed hex and truncated signatures.
func TestHMACSignatureRejected(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	mk := func(nonce string) *proto.Hello {
		return signedHello("hmac-secret", "hmac", proto.RoleControl, nonce)
	}

	wrongSecret := mk("bad-secret-nonce")
	wrongSecret.Sig = proto.SignHello("wrong-secret", "hmac", wrongSecret.Ts, wrongSecret.Nonce)

	tamperedTS := mk("bad-ts-nonce")
	tamperedTS.Ts++ // signature no longer covers the hello

	tamperedNode := mk("bad-node-nonce")
	tamperedNode.Sig = proto.SignHello("hmac-secret", "other-node", tamperedNode.Ts, tamperedNode.Nonce)

	malformed := mk("bad-hex-nonce")
	malformed.Sig = "zzzz-not-hex"

	truncated := mk("truncated-nonce")
	truncated.Sig = truncated.Sig[:32] // half a SHA-256 MAC

	emptySig := mk("empty-sig-nonce")
	emptySig.Sig = ""

	for _, tc := range []struct {
		name string
		h    *proto.Hello
		want error
	}{
		{"wrong secret", wrongSecret, ErrHMACInvalid},
		{"tampered ts", tamperedTS, ErrHMACInvalid},
		{"tampered node", tamperedNode, ErrHMACInvalid},
		{"malformed hex", malformed, ErrHMACInvalid},
		{"truncated mac", truncated, ErrHMACInvalid},
		{"empty sig", emptySig, ErrHMACRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireAuthErr(t, app.AuthenticateHello(tc.h), tc.want)
		})
	}
}

// TestHMACStaleTimestamp rejects hellos outside the ±60s window and accepts
// ones comfortably inside it.
func TestHMACStaleTimestamp(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	for i, delta := range []int64{-61, -3600, 120, 3600} {
		ts := time.Now().Unix() + delta
		nonce := fmt.Sprintf("stale-%d-%d", i, delta)
		h := &proto.Hello{
			Role:  proto.RoleControl,
			Node:  "hmac",
			Ts:    ts,
			Nonce: nonce,
			Sig:   proto.SignHello("hmac-secret", "hmac", ts, nonce),
		}
		requireAuthErr(t, app.AuthenticateHello(h), ErrHMACStaleTimestamp)
	}

	// 30s old is well inside the window and must be accepted.
	ts := time.Now().Unix() - 30
	h := &proto.Hello{
		Role:  proto.RoleControl,
		Node:  "hmac",
		Ts:    ts,
		Nonce: "fresh-nonce",
		Sig:   proto.SignHello("hmac-secret", "hmac", ts, "fresh-nonce"),
	}
	if err := app.AuthenticateHello(h); err != nil {
		t.Fatalf("ts 30s old should be accepted: %v", err)
	}
}

// TestHMACNonceReplay rejects a replayed signed hello, even with a fresh
// timestamp/signature for the same nonce, and keeps nonces namespaced per node.
func TestHMACNonceReplay(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	h := signedHello("hmac-secret", "hmac", proto.RoleControl, "replay-nonce")
	if err := app.AuthenticateHello(h); err != nil {
		t.Fatalf("first use should authenticate: %v", err)
	}
	requireAuthErr(t, app.AuthenticateHello(h), ErrHMACReplay)

	// Same nonce, freshly signed with a new timestamp: still a replay.
	fresh := signedHello("hmac-secret", "hmac", proto.RoleControl, "replay-nonce")
	requireAuthErr(t, app.AuthenticateHello(fresh), ErrHMACReplay)

	// Nonces are per node: the same nonce is fine on a different node.
	other := signedHello("hmac-secret", "hmac2", proto.RoleControl, "replay-nonce")
	if err := app.AuthenticateHello(other); err != nil {
		t.Fatalf("same nonce on another node should authenticate: %v", err)
	}
}

// TestHMACMissingSignature covers HMAC-mode hellos that carry no signature:
// a legacy plaintext client (correct key, no ts/nonce/sig) must not slip
// through, and both channel roles require the signed triple.
func TestHMACMissingSignature(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	for _, role := range []string{proto.RoleControl, proto.RoleData} {
		h := &proto.Hello{
			Role:  role,
			Node:  "hmac",
			Key:   "legacy-key", // correct plaintext key must not matter
			Ts:    time.Now().Unix(),
			Nonce: "missing-sig-" + role,
		}
		if role == proto.RoleData {
			h.Session = "session-1"
			h.ChannelID = 0
		}
		requireAuthErr(t, app.AuthenticateHello(h), ErrHMACRequired)
	}

	// A legacy hello with no HMAC fields at all is also rejected (nonce is
	// validated before the signature, so the reported reason is the nonce).
	legacy := &proto.Hello{Role: proto.RoleControl, Node: "hmac", Key: "legacy-key"}
	requireAuthErr(t, app.AuthenticateHello(legacy), ErrHMACBadNonce)
}

// TestAuthenticateHMACRejectsPlaintextKey verifies the legacy Authenticate
// helper never authenticates an HMAC node, so it cannot downgrade the posture.
func TestAuthenticateHMACRejectsPlaintextKey(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	if app.Authenticate("hmac", "legacy-key") {
		t.Fatal("Authenticate must refuse an HMAC-enabled node even with the matching plaintext key")
	}
	if app.Authenticate("hmac2", "") {
		t.Fatal("Authenticate must refuse an HMAC-enabled node with an empty key")
	}
}

// TestHMACBadNonce enforces nonce presence and the 1..128 byte bound.
func TestHMACBadNonce(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	build := func(nonce string) *proto.Hello {
		ts := time.Now().Unix()
		return &proto.Hello{
			Role:  proto.RoleControl,
			Node:  "hmac",
			Ts:    ts,
			Nonce: nonce,
			Sig:   proto.SignHello("hmac-secret", "hmac", ts, nonce),
		}
	}

	requireAuthErr(t, app.AuthenticateHello(build("")), ErrHMACBadNonce)
	requireAuthErr(t, app.AuthenticateHello(build(strings.Repeat("n", proto.MaxNonceLen+1))), ErrHMACBadNonce)

	// Exactly MaxNonceLen bytes is allowed.
	if err := app.AuthenticateHello(build(strings.Repeat("n", proto.MaxNonceLen))); err != nil {
		t.Fatalf("max-length nonce should be accepted: %v", err)
	}
}

// TestHMACCanonicalFormat pins the wire contract clients mirror.
func TestHMACCanonicalFormat(t *testing.T) {
	if got, want := proto.CanonicalHello("node1", 1700000000, "abc"), "node1|1700000000|abc"; got != want {
		t.Fatalf("canonical = %q, want %q", got, want)
	}
	secret, node, nonce := "s3cret", "node1", "n-1"
	ts := int64(1700000000)
	wantMAC := hmac.New(sha256.New, []byte(secret))
	_, _ = wantMAC.Write([]byte("node1|1700000000|n-1"))
	want := hex.EncodeToString(wantMAC.Sum(nil))
	if got := proto.SignHello(secret, node, ts, nonce); got != want {
		t.Fatalf("SignHello = %q, want %q", got, want)
	}
	if !proto.VerifyHelloSignature(secret, node, ts, nonce, want) {
		t.Fatal("VerifyHelloSignature should accept the generated signature")
	}
	if !proto.VerifyHelloSignature(secret, node, ts, nonce, strings.ToUpper(want)) {
		t.Fatal("VerifyHelloSignature should accept uppercase hex")
	}
	if proto.VerifyHelloSignature("other-secret", node, ts, nonce, want) {
		t.Fatal("VerifyHelloSignature must reject a wrong secret")
	}
	if proto.VerifyHelloSignature(secret, node, ts, nonce, "not-hex") {
		t.Fatal("VerifyHelloSignature must reject a malformed signature")
	}
}

// TestHMACNonceCacheBounded checks capacity eviction and time-based expiry of
// the bounded anti-replay cache.
func TestHMACNonceCacheBounded(t *testing.T) {
	now := time.Now().Unix()
	c := newNonceCache(2, time.Minute)
	if !c.checkAndStore("a", now) {
		t.Fatal("first 'a' should be fresh")
	}
	if c.checkAndStore("a", now) {
		t.Fatal("second 'a' should be a replay")
	}
	if !c.checkAndStore("b", now) {
		t.Fatal("first 'b' should be fresh")
	}
	if !c.checkAndStore("c", now) {
		t.Fatal("'c' should be accepted, evicting the oldest entry")
	}
	if !c.checkAndStore("a", now) {
		t.Fatal("evicted 'a' should be treated as fresh")
	}
	if c.checkAndStore("c", now) {
		t.Fatal("'c' should still be a replay")
	}

	// Expired entries are purged, so a nonce older than the skew window no
	// longer occupies the cache and is treated as fresh by the cache itself.
	exp := newNonceCache(4, time.Second)
	if !exp.checkAndStore("e", now) {
		t.Fatal("first 'e' should be fresh")
	}
	if !exp.checkAndStore("e", now+2) {
		t.Fatal("expired 'e' should be purged and treated as fresh")
	}
}

// TestHMACConcurrentAuth exercises the package-level cache under -race: many
// distinct signed hellos must all authenticate, and exactly one of N copies of
// the same signed hello may authenticate.
func TestHMACConcurrentAuth(t *testing.T) {
	resetNonceCacheForTest()
	app := testAuthApp()

	const workers = 32
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h := signedHello("hmac-secret", "hmac", proto.RoleControl, fmt.Sprintf("concurrent-%d", i))
			errCh <- app.AuthenticateHello(h)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("distinct concurrent hello failed: %v", err)
		}
	}

	// One signed hello replayed concurrently: the cache check-and-insert must
	// be atomic, so exactly one caller wins.
	dup := signedHello("hmac-secret", "hmac", proto.RoleControl, "race-nonce")
	var mu sync.Mutex
	success := 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if app.AuthenticateHello(dup) == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if success != 1 {
		t.Fatalf("concurrent replay: %d successful authentications, want exactly 1", success)
	}
}

// TestHMACSecretCaddyfileParsing verifies the hmac_secret directive inside a
// tunnel_server node block, including its coexistence with the legacy key.
func TestHMACSecretCaddyfileParsing(t *testing.T) {
	input := `tunnel_server {
	node node1 {
		key legacy-key
		hmac_secret s3cret
		channels 2
		port 8080
	}
	node node2 {
		key plain-only
		port 9090
	}
}`
	mh, err := parseTunnelServerCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(input)})
	if err != nil {
		t.Fatalf("parse tunnel_server: %v", err)
	}
	ts, ok := mh.(*TunnelServer)
	if !ok {
		t.Fatalf("handler type = %T, want *TunnelServer", mh)
	}

	n1 := ts.Nodes["node1"]
	if n1 == nil {
		t.Fatal("node1 missing")
	}
	if n1.HMACSecret != "s3cret" {
		t.Fatalf("node1 hmac_secret = %q, want %q", n1.HMACSecret, "s3cret")
	}
	if n1.Key != "legacy-key" {
		t.Fatalf("node1 key = %q, want legacy-key kept alongside hmac_secret", n1.Key)
	}
	if n1.Channels != 2 || n1.Ports["8080"] != "127.0.0.1:8080" {
		t.Fatalf("node1 other directives lost: channels=%d ports=%v", n1.Channels, n1.Ports)
	}

	n2 := ts.Nodes["node2"]
	if n2 == nil {
		t.Fatal("node2 missing")
	}
	if n2.HMACSecret != "" {
		t.Fatalf("node2 hmac_secret = %q, want empty (plaintext posture)", n2.HMACSecret)
	}
}

// TestHMACSecretJSONConfig verifies hmac_secret is available through JSON
// config without any extra wiring (json tag on NodeConfig).
func TestHMACSecretJSONConfig(t *testing.T) {
	var cfg NodeConfig
	if err := json.Unmarshal([]byte(`{"key":"k","hmac_secret":"json-secret"}`), &cfg); err != nil {
		t.Fatalf("unmarshal node config: %v", err)
	}
	if cfg.HMACSecret != "json-secret" {
		t.Fatalf("hmac_secret = %q, want json-secret", cfg.HMACSecret)
	}
	if cfg.Key != "k" {
		t.Fatalf("key = %q, want k", cfg.Key)
	}
}
