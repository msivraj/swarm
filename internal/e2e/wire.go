// Package e2e is the P0 end-to-end integration test (issue #22, the P0 exit
// criterion). It stands up a real controlplane.Server and real agent.Agents
// over gRPC (loopback via bufconn) and drives them with the transport gRPC
// client, exactly the way a real deployment's CLI and swarmd binaries would,
// minus the process boundary.
//
// This file holds the wire-format helpers the P0 job templates
// (internal/core/templates) use for Task.Input / TaskResult.Output, so both
// the workers under internal/e2e/workers/* and the test itself can encode
// and decode them. The templates package's own encode/decode helpers are
// unexported (they are an implementation detail of the pure core, not part
// of its API), so these mirror that byte layout by hand — see keyspace.go
// and montecarlo.go in internal/core/templates for the layouts being
// mirrored.
package e2e

import (
	"encoding/binary"
	"math"
)

// DecodeKeyspaceRange decodes a keyspace-search task's Input: two big-endian
// uint64s, [start, end). Mirrors templates.decodeKeyspaceInput.
func DecodeKeyspaceRange(b []byte) (start, end uint64, ok bool) {
	if len(b) != 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16]), true
}

// EncodeKeyspaceHit encodes a keyspace-search hit's Output: the matching key
// as a big-endian uint64. Mirrors what templates.KeyspaceMerge treats as the
// winning result's Output.
func EncodeKeyspaceHit(key uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, key)
	return b
}

// DecodeKeyspaceHit decodes a keyspace-search Aggregate.Value (or a hit
// task's Output) back to the matching key.
func DecodeKeyspaceHit(b []byte) (key uint64, ok bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// DecodeMCTaskInput decodes a monte-carlo task's Input: a big-endian int64
// Seed followed by a big-endian int64 Trials. Mirrors
// templates.decodeMCTaskInput.
func DecodeMCTaskInput(b []byte) (seed, trials int64, ok bool) {
	if len(b) != 16 {
		return 0, 0, false
	}
	return int64(binary.BigEndian.Uint64(b[0:8])), int64(binary.BigEndian.Uint64(b[8:16])), true
}

// EncodeMCResult encodes a monte-carlo block's TaskResult.Output: a
// big-endian int64 Count, followed by the big-endian bits of float64 Sum and
// SumSq. Mirrors templates.encodeMCResult.
func EncodeMCResult(count int64, sum, sumSq float64) []byte {
	out := make([]byte, 24)
	binary.BigEndian.PutUint64(out[0:8], uint64(count))
	binary.BigEndian.PutUint64(out[8:16], math.Float64bits(sum))
	binary.BigEndian.PutUint64(out[16:24], math.Float64bits(sumSq))
	return out
}

// MCAggregate is the decoded form of a monte-carlo job's Aggregate.Value.
type MCAggregate struct {
	Count    int64
	Sum      float64
	Mean     float64
	Variance float64
}

// DecodeMCAggregate decodes a monte-carlo job's Aggregate.Value: big-endian
// int64 Count, followed by the big-endian bits of float64 Sum, Mean, and
// Variance. Mirrors templates.decodeMCAggregate (the layout
// templates.MonteCarloMerge produces).
func DecodeMCAggregate(b []byte) (MCAggregate, bool) {
	if len(b) != 32 {
		return MCAggregate{}, false
	}
	return MCAggregate{
		Count:    int64(binary.BigEndian.Uint64(b[0:8])),
		Sum:      math.Float64frombits(binary.BigEndian.Uint64(b[8:16])),
		Mean:     math.Float64frombits(binary.BigEndian.Uint64(b[16:24])),
		Variance: math.Float64frombits(binary.BigEndian.Uint64(b[24:32])),
	}, true
}
