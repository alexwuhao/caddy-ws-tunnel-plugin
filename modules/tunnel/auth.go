package tunnel

import (
	"crypto/rand"
	"encoding/hex"
)

// Auth performs server-side authentication for a Hello message.
// Current posture: plaintext key comparison via key → node lookup.
// HMAC challenge + nonce LRU can be layered in later without changing
// the caller interface — just enhance this function.

// Authenticate checks whether the provided key matches the configured key for
// the named node. Returns true if the auth succeeds.
func (a *TunnelApp) Authenticate(node, key string) bool {
	cfg, ok := a.Nodes[node]
	if !ok {
		return false
	}
	return cfg.Key == key
}

// generateSession creates a random session token (hex-encoded, 16 bytes of
// randomness) used to bind data channels to their owning control channel.
func generateSession() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
