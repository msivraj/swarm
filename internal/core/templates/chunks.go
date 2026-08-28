package templates

import "encoding/binary"

// packChunks concatenates chunks into one byte stream, each chunk prefixed
// with its own 4-byte big-endian length — a self-describing wire format so
// unpackChunks can split the stream back into the original chunks without
// any external framing. Shared by every template combine in this package
// that folds a step's per-worker payloads into one combined packet rather
// than one combined number (SciSimCombine's boundary exchange,
// GraphComputeCombine's superstep message stream).
//
// A nil or empty chunks packs to an empty (non-nil) stream.
func packChunks(chunks [][]byte) []byte {
	out := make([]byte, 0)
	var lenBuf [4]byte
	for _, c := range chunks {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(c)))
		out = append(out, lenBuf[:]...)
		out = append(out, c...)
	}
	return out
}

// unpackChunks is packChunks's inverse: it splits a packed stream back into
// its original chunks, in the order they were packed. A malformed stream
// (a truncated length prefix, or a length that overruns the remaining
// bytes) reports ok == false.
func unpackChunks(packed []byte) (chunks [][]byte, ok bool) {
	for len(packed) > 0 {
		if len(packed) < 4 {
			return nil, false
		}
		n := binary.BigEndian.Uint32(packed[:4])
		packed = packed[4:]
		if uint64(len(packed)) < uint64(n) {
			return nil, false
		}
		chunks = append(chunks, packed[:n])
		packed = packed[n:]
	}
	return chunks, true
}
