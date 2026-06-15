package proto

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
)

func TestStreamHeaderRoundTrip(t *testing.T) {
	cases := []uint64{0, 1, 80, 8080, 9090, 5432, 65535, 1 << 20, 1<<63 - 1}
	for _, port := range cases {
		var buf bytes.Buffer
		if err := WriteStreamHeader(&buf, port); err != nil {
			t.Fatalf("write port=%d: %v", port, err)
		}
		// Append trailing payload to confirm the reader stops exactly at the
		// header boundary and leaves payload intact.
		payload := []byte("GET / HTTP/1.1\r\n")
		buf.Write(payload)

		r := NewHeaderReader(&buf)
		ver, got, err := ReadStreamHeader(r)
		if err != nil {
			t.Fatalf("read port=%d: %v", port, err)
		}
		if ver != ProtocolVersion {
			t.Fatalf("ver = %d, want %d", ver, ProtocolVersion)
		}
		if got != port {
			t.Fatalf("port = %d, want %d", got, port)
		}
		rest, _ := io.ReadAll(r)
		if !bytes.Equal(rest, payload) {
			t.Fatalf("payload corrupted: got %q want %q", rest, payload)
		}
	}
}

func TestStreamHeaderBadVersion(t *testing.T) {
	buf := bytes.NewBuffer([]byte{99, 0x01})
	r := NewHeaderReader(buf)
	if _, _, err := ReadStreamHeader(r); err != ErrUnsupportedVersion {
		t.Fatalf("err = %v, want ErrUnsupportedVersion", err)
	}
}

func TestWrapEnvelope(t *testing.T) {
	raw, err := WrapEnvelope(TypeHello, Hello{Role: RoleControl, Node: "node1", Key: "xx1"})
	if err != nil {
		t.Fatal(err)
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.Type != TypeHello {
		t.Fatalf("type = %q, want %q", env.Type, TypeHello)
	}
	var h Hello
	if err := json.Unmarshal(env.Payload, &h); err != nil {
		t.Fatal(err)
	}
	if h.Node != "node1" || h.Key != "xx1" || h.Role != RoleControl {
		t.Fatalf("hello round-trip mismatch: %+v", h)
	}
}
