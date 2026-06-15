package proto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Stream header (design §6.2, dev.md §4.3):
//
//	┌──────────┬────────────────────────┬─────────────────────┐
//	│ ver (1B) │ port id (uvarint)      │  raw HTTP bytes...   │
//	└──────────┴────────────────────────┴─────────────────────┘
//
// The server writes the full header before any HTTP bytes; the client reads
// the full header before treating the remainder as an opaque L4 byte stream.

// ErrUnsupportedVersion is returned when the header version byte is not
// ProtocolVersion.
var ErrUnsupportedVersion = errors.New("tunnel proto: unsupported stream header version")

// WriteStreamHeader writes ver(1B) + uvarint(portID) to w. Callers MUST write
// the entire header before handing the stream to an HTTP transport, otherwise
// header bytes get glued onto the first HTTP byte (dev.md §8 easy-error list).
func WriteStreamHeader(w io.Writer, portID uint64) error {
	var buf [1 + binary.MaxVarintLen64]byte
	buf[0] = ProtocolVersion
	n := binary.PutUvarint(buf[1:], portID)
	_, err := w.Write(buf[:1+n])
	return err
}

// ByteReader is the minimal interface ReadStreamHeader needs. A bufio.Reader
// over the stream satisfies it; so does anything exposing ReadByte.
type ByteReader interface {
	io.Reader
	io.ByteReader
}

// ReadStreamHeader reads and validates the version byte, then the uvarint port
// id. Pass a buffered reader (e.g. bufio.NewReader(stream)) so the remaining
// bytes after the header stay available to the caller.
func ReadStreamHeader(r ByteReader) (ver uint8, portID uint64, err error) {
	v, err := r.ReadByte()
	if err != nil {
		return 0, 0, fmt.Errorf("read version: %w", err)
	}
	if v != ProtocolVersion {
		return v, 0, ErrUnsupportedVersion
	}
	portID, err = binary.ReadUvarint(r)
	if err != nil {
		return v, 0, fmt.Errorf("read port id: %w", err)
	}
	return v, portID, nil
}

// NewHeaderReader wraps r in a bufio.Reader suitable for ReadStreamHeader. The
// returned reader must be used for all subsequent reads on the stream so no
// buffered payload bytes are lost.
func NewHeaderReader(r io.Reader) *bufio.Reader {
	return bufio.NewReader(r)
}
