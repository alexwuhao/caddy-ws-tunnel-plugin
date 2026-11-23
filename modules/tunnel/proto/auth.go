package proto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// HMAC hello-signing contract (design §10).
//
// A node configured with an hmac_secret authenticates every hello (control and
// data channels alike) with a signature instead of a plaintext key:
//
//	canonical = node + "|" + <decimal unix-seconds ts> + "|" + nonce
//	sig       = hex(HMAC-SHA256(secret, canonical))
//
// The server only accepts ts within ±HMACClockSkewSeconds of its own clock and
// rejects a nonce it has already accepted, so a captured hello cannot be
// replayed. The plaintext Key field is ignored when a node has hmac_secret set.
//
// Clients mirror this package; use CanonicalHello/SignHello rather than
// re-deriving the string, so the wire contract cannot drift.
const (
	// HMACClockSkewSeconds is the maximum allowed |server-now - hello.ts|, in
	// seconds, for a signed hello.
	HMACClockSkewSeconds = 60

	// MaxNonceLen is the maximum accepted hello.nonce length, in bytes.
	MaxNonceLen = 128
)

// CanonicalHello returns the exact byte string that is HMAC-signed for a hello.
// It is exported so client implementations can mirror the contract without
// guessing the field order or separators.
func CanonicalHello(node string, ts int64, nonce string) string {
	return node + "|" + strconv.FormatInt(ts, 10) + "|" + nonce
}

// SignHello returns the hex-encoded HMAC-SHA256 signature for a hello, using
// the canonical string from CanonicalHello.
func SignHello(secret, node string, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(CanonicalHello(node, ts, nonce)))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyHelloSignature reports whether sig is a valid hello signature for the
// given secret/node/ts/nonce. It decodes sig from hex and compares the MACs in
// constant time. A malformed (non-hex) sig is rejected.
func VerifyHelloSignature(secret, node string, ts int64, nonce, sig string) bool {
	got, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(CanonicalHello(node, ts, nonce)))
	return hmac.Equal(got, mac.Sum(nil))
}
