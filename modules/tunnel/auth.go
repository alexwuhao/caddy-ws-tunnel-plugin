package tunnel

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wjzhangq/caddy-ws-tunnel-plugin/modules/tunnel/proto"
)

// Authentication (design §10).
//
// Two postures, selected per node at config time:
//
//   - HMAC (NodeConfig.HMACSecret non-empty): the hello must carry a valid,
//     fresh, single-use ts/nonce/sig triple; the plaintext Key is ignored.
//     This applies identically to control- and data-channel hellos.
//   - Plaintext legacy (HMACSecret empty): hello.Key is compared against the
//     configured Key in constant time. ts/nonce/sig are accepted and ignored.
//
// AuthenticateHello is the single entry point used by the WS handler. The old
// Authenticate(node, key) helper is kept for compatibility with existing
// callers/tests; it never authenticates an HMAC-enabled node, so it cannot be
// used to bypass signature checks.

// hmacSkew bounds |now - hello.ts| for signed hellos. hello.nonce entries are
// retained for the same duration: outside this window the timestamp check
// already rejects a replay.
const hmacSkew = 60 * time.Second

// nonceCacheSize bounds the global anti-replay cache (bounded memory regardless
// of connection volume).
const nonceCacheSize = 4096

// Auth failure sentinels. Callers wrap them with context for logs; the wrapped
// text is safe to log (node names and timestamps only, never secrets or
// signatures). The WS close reason sent to the peer stays generic.
var (
	// ErrUnknownNode: no node with that name is configured.
	ErrUnknownNode = errors.New("auth: unknown node")
	// ErrKeyMismatch: plaintext hello key does not match the node key.
	ErrKeyMismatch = errors.New("auth: plaintext key mismatch")
	// ErrHMACRequired: node uses hmac_secret but the hello carries no signature.
	ErrHMACRequired = errors.New("auth: hmac signature required")
	// ErrHMACInvalid: signature does not verify against the node secret.
	ErrHMACInvalid = errors.New("auth: hmac signature invalid")
	// ErrHMACStaleTimestamp: hello.ts is outside the allowed clock skew.
	ErrHMACStaleTimestamp = errors.New("auth: timestamp outside allowed skew")
	// ErrHMACBadNonce: hello.nonce is empty or too long.
	ErrHMACBadNonce = errors.New("auth: invalid nonce")
	// ErrHMACReplay: nonce was already accepted; the hello is a replay.
	ErrHMACReplay = errors.New("auth: nonce already used")
)

// AuthenticateHello validates a hello envelope for the node it names. It is the
// authoritative authentication decision for both channel roles.
func (a *TunnelApp) AuthenticateHello(hello *proto.Hello) error {
	if hello == nil {
		return fmt.Errorf("%w: nil hello", ErrUnknownNode)
	}
	cfg, ok := a.Nodes[hello.Node]
	if !ok || cfg == nil {
		return fmt.Errorf("%w: %q", ErrUnknownNode, hello.Node)
	}
	if cfg.HMACSecret != "" {
		return authenticateHMAC(cfg.HMACSecret, hello)
	}
	return authenticatePlaintext(cfg.Key, hello)
}

// authenticatePlaintext performs the legacy constant-time key comparison.
func authenticatePlaintext(want string, hello *proto.Hello) error {
	if subtle.ConstantTimeCompare([]byte(want), []byte(hello.Key)) != 1 {
		return fmt.Errorf("%w: node %q", ErrKeyMismatch, hello.Node)
	}
	return nil
}

// authenticateHMAC validates the signed-hello triple. Order matters: cheap
// syntactic checks first, then the MAC, and only then the anti-replay insert.
// A nonce is never recorded before its signature verifies, so unauthenticated
// peers cannot poison the cache.
func authenticateHMAC(secret string, hello *proto.Hello) error {
	if hello.Nonce == "" || len(hello.Nonce) > proto.MaxNonceLen {
		return fmt.Errorf("%w: len=%d (allowed 1..%d)", ErrHMACBadNonce, len(hello.Nonce), proto.MaxNonceLen)
	}
	if hello.Sig == "" {
		return fmt.Errorf("%w: node %q", ErrHMACRequired, hello.Node)
	}
	now := time.Now().Unix()
	skew := int64(hmacSkew / time.Second)
	if hello.Ts < now-skew || hello.Ts > now+skew {
		return fmt.Errorf("%w: ts=%d now=%d (skew=±%ds)", ErrHMACStaleTimestamp, hello.Ts, now, skew)
	}
	if !proto.VerifyHelloSignature(secret, hello.Node, hello.Ts, hello.Nonce, hello.Sig) {
		return fmt.Errorf("%w: node %q", ErrHMACInvalid, hello.Node)
	}
	if !helloNonces.checkAndStore(nonceCacheKey(hello.Node, hello.Nonce), now) {
		return fmt.Errorf("%w: node %q", ErrHMACReplay, hello.Node)
	}
	return nil
}

// Authenticate checks whether the provided plaintext key matches the configured
// key for the named node. It always returns false for nodes configured with an
// HMAC secret: those nodes must go through AuthenticateHello so a plaintext key
// can never downgrade the authentication posture.
func (a *TunnelApp) Authenticate(node, key string) bool {
	cfg, ok := a.Nodes[node]
	if !ok || cfg == nil || cfg.HMACSecret != "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cfg.Key), []byte(key)) == 1
}

// nonceCacheKey namespaces nonces per node so one node's traffic cannot make
// another node's nonce look replayed. The NUL separator cannot appear in a JSON
// string escape sequence produced by encoding/json, and node names are plain
// identifiers, so the concatenation is unambiguous.
func nonceCacheKey(node, nonce string) string {
	return node + "\x00" + nonce
}

// nonceEntry is one accepted nonce; expiry is the unix-second instant after
// which the timestamp window would reject a replay anyway.
type nonceEntry struct {
	key    string
	expiry int64
}

// nonceCache is a bounded FIFO cache of accepted nonces. FIFO (rather than true
// LRU) is sufficient: entries are only legitimate for hmacSkew seconds, and the
// oldest entry is always the first to expire. Membership is O(1) via seen; the
// ring preserves insertion order for expiry purging and capacity eviction.
type nonceCache struct {
	mu      sync.Mutex
	max     int
	ttlSecs int64
	entries []nonceEntry
	start   int // index of oldest entry
	count   int
	seen    map[string]struct{}
}

func newNonceCache(max int, ttl time.Duration) *nonceCache {
	if max <= 0 {
		max = 1
	}
	return &nonceCache{
		max:     max,
		ttlSecs: int64(ttl / time.Second),
		entries: make([]nonceEntry, max),
		seen:    make(map[string]struct{}, max),
	}
}

// helloNonces is the process-wide anti-replay cache. A single cache is shared
// by every TunnelApp instance because the app struct is owned elsewhere and
// node names are globally unique within one Caddy instance.
var helloNonces = newNonceCache(nonceCacheSize, hmacSkew)

// resetNonceCacheForTest clears the global cache. Test-only helper; production
// code never calls it.
func resetNonceCacheForTest() {
	helloNonces.reset()
}

// reset drops all accepted nonces.
func (c *nonceCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make([]nonceEntry, c.max)
	c.seen = make(map[string]struct{}, c.max)
	c.start = 0
	c.count = 0
}

// checkAndStore atomically reports whether key was absent and, if so, records
// it. now is unix seconds. It returns false for a duplicate (replay).
func (c *nonceCache) checkAndStore(key string, now int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeLocked(now)
	if _, ok := c.seen[key]; ok {
		return false
	}
	if c.count == c.max {
		// Full: evict the oldest accepted nonce. Under a sustained >max per
		// hmacSkew burst this loosens replay detection to the newest max
		// nonces, which is the documented bounded-memory tradeoff.
		old := c.entries[c.start]
		delete(c.seen, old.key)
		c.entries[c.start] = nonceEntry{}
		c.start = (c.start + 1) % c.max
		c.count--
	}
	idx := (c.start + c.count) % c.max
	c.entries[idx] = nonceEntry{key: key, expiry: now + c.ttlSecs}
	c.seen[key] = struct{}{}
	c.count++
	return true
}

// purgeLocked removes expired entries from the oldest end of the ring.
func (c *nonceCache) purgeLocked(now int64) {
	for c.count > 0 && c.entries[c.start].expiry <= now {
		delete(c.seen, c.entries[c.start].key)
		c.entries[c.start] = nonceEntry{}
		c.start = (c.start + 1) % c.max
		c.count--
	}
}

// generateSession creates a random session token (hex-encoded, 16 bytes of
// randomness) used to bind data channels to their owning control channel.
func generateSession() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
