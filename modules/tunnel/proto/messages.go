package proto

import "encoding/json"

// Message type constants carried in Envelope.Type (design §5).
const (
	TypeHello        = "hello"
	TypeWelcome      = "welcome"
	TypeConfig       = "config" // struct only; never sent as a standalone frame
	TypeReloadConfig = "reload_config"
	TypePing         = "ping"
	TypePong         = "pong"
	TypeStats        = "stats"
	TypeDrain        = "drain"
	TypeBye          = "bye"
	TypeError        = "error"
)

// Role values for Hello.Role.
const (
	RoleControl = "control"
	RoleData    = "data"
)

// Envelope is the top-level wrapper for every control-channel WS binary
// message. Each message is one JSON object with a "type" discriminator; the
// remaining fields are decoded in a second pass into the concrete struct via
// the Payload raw bytes.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// WrapEnvelope marshals a typed payload into an Envelope-ready JSON object.
func WrapEnvelope(typ string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Type: typ, Payload: raw})
}

// Hello is sent by the client to open either the control channel or a data
// channel (design §4.3 / §5). For data-channel hellos, Session and ChannelID
// are required; Session MUST match the value handed out in the control-channel
// Welcome (design §10 anti-orphan-injection).
//
// HMAC fields (Ts/Nonce/Sig) are part of the contract but unused in the
// plaintext-key auth posture; they are accepted-and-ignored by the server for
// forward compatibility.
type Hello struct {
	Role      string `json:"role"`
	Node      string `json:"node"`
	Key       string `json:"key"`
	Session   string `json:"session,omitempty"`
	ChannelID int    `json:"channel_id,omitempty"`

	Ts    int64  `json:"ts,omitempty"`
	Nonce string `json:"nonce,omitempty"`
	Sig   string `json:"sig,omitempty"`
}

// Welcome is the server's handshake response (design §4.3). The control-channel
// Welcome carries the authoritative initial Config inline — the client reads
// its config ONLY from here; the server never emits a standalone config frame.
type Welcome struct {
	Session   string  `json:"session,omitempty"`
	Heartbeat string  `json:"heartbeat,omitempty"` // e.g. "15s"
	Mux       *Mux    `json:"mux,omitempty"`
	Channels  int     `json:"channels,omitempty"`
	ChannelID *int    `json:"channel_id,omitempty"` // set on data-channel welcome
	Config    *Config `json:"config,omitempty"`     // set on control-channel welcome
}

// Mux holds smux tuning the client should mirror.
type Mux struct {
	Version          int    `json:"version,omitempty"`
	KeepAliveItvl    string `json:"keepalive_interval,omitempty"`
	KeepAliveTimeout string `json:"keepalive_timeout,omitempty"`
	MaxFrameSize     int    `json:"max_frame_size,omitempty"`
	MaxReceiveBuffer int    `json:"max_receive_buffer,omitempty"`
	MaxStreamBuffer  int    `json:"max_stream_buffer,omitempty"`
}

// Config is the authoritative per-node config the server pushes. It rides
// inside Welcome on handshake and is reused verbatim by ReloadConfig for hot
// updates (design §5). Ports is the allowlist: numeric port id -> local
// "host:port" the client may dial. Anything not in this map is rejected.
type Config struct {
	Ports             map[string]string `json:"ports"`
	MaxStreamsPerConn int               `json:"max_streams_per_conn,omitempty"`
	Channels          int               `json:"channels,omitempty"`
	DialTimeout       string            `json:"dial_timeout,omitempty"`
	QueueTimeout      string            `json:"queue_timeout,omitempty"`
}

// Ping / Pong keep the control channel alive and measure RTT (design §5). Data
// channels are kept alive by smux keepalive, not by these.
type Ping struct {
	Nonce string `json:"nonce,omitempty"`
	Ts    int64  `json:"ts,omitempty"`
}

type Pong struct {
	Nonce string `json:"nonce,omitempty"`
	Ts    int64  `json:"ts,omitempty"`
}

// Stats is client-side telemetry fed into /status and Prometheus (design §5/§12).
type Stats struct {
	Channels        int   `json:"channels,omitempty"`
	Streams         int   `json:"streams,omitempty"`
	BytesIn         int64 `json:"bytes_in,omitempty"`
	BytesOut        int64 `json:"bytes_out,omitempty"`
	LocalDialErrors int64 `json:"local_dial_errors,omitempty"`
}

// Drain tells the client to stop accepting new streams and let in-flight ones
// finish before closing (graceful shutdown, design §13).
type Drain struct {
	Deadline string `json:"deadline,omitempty"`
}

// Bye is a graceful-close notice (either direction).
type Bye struct {
	Reason string `json:"reason,omitempty"`
}

// Error is a generic rejection (auth failure, unknown port, bad config, ...).
type Error struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Ref  string `json:"ref,omitempty"`
}
