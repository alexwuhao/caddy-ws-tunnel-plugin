// Package proto is the outward-facing protocol contract between the Caddy
// tunnel plugin (server) and the external tunnel-agent (client). The client
// project mirrors these definitions; do not change field names or wire formats
// without bumping ProtocolVersion and noting compatibility.
package proto

// ProtocolVersion is the stream-header version byte and the handshake protocol
// revision. Current = 1 (design §6.2 / dev.md §4.3).
const ProtocolVersion uint8 = 1
