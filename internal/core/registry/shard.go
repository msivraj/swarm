// This file adds ShardOf beside the P0 Apply/Snapshot above: the FCIS proof
// of P4 is that going to a sharded store (FoundationDB behind the shell)
// adds exactly this one pure function and changes no decision logic — Apply
// and Snapshot in registry.go are untouched.
package registry

import (
	"math/bits"

	"github.com/msivraj/swarm/internal/model"
)

// ShardOf maps a registry key to the shard that owns it via a RANGE
// partition, matching how a range-sharded store (FoundationDB) actually
// splits its keyspace: contiguous keys land in the same or an adjacent
// shard, rather than being scattered by an unrelated hash.
//
// The range rule: key is projected onto its position in the byte-ordered
// keyspace by reading its first 8 bytes as a big-endian integer (shorter
// keys are zero-padded on the right; bytes beyond the 8th are not
// distinguished, so two keys sharing an 8-byte prefix map to the same
// shard). That position is then split into `shards` contiguous, equal-width
// buckets by taking the high 64 bits of position*shards — the standard
// multiply-high range map, computed exactly with math/bits.Mul64 so there
// is no floating point and no rounding bias at the boundaries. Because both
// steps are order-preserving, a key's shard is monotonic non-decreasing in
// the key's byte order: walking the keyspace in order, the shard index
// never goes backwards.
//
// ShardOf is total and deterministic: for any key and any shards, the
// result is always in [0, shards) and repeated calls with the same
// arguments always return the same ShardID. shards == 0 is treated as a
// single shard (every key maps to ShardID 0) rather than dividing by zero,
// so the function never panics.
func ShardOf(key model.Key, shards uint32) model.ShardID {
	if shards == 0 {
		return 0
	}
	hi, _ := bits.Mul64(keyPosition(key), uint64(shards))
	return model.ShardID(hi)
}

// keyPosition projects key onto its position in the byte-ordered keyspace,
// represented as a big-endian uint64 over its first 8 bytes (zero-padded if
// key is shorter). This is the same technique a range-sharded store uses to
// place a variable-length key into a fixed-width, order-preserving space.
func keyPosition(key model.Key) uint64 {
	var buf [8]byte
	copy(buf[:], key)
	var pos uint64
	for _, b := range buf {
		pos = pos<<8 | uint64(b)
	}
	return pos
}
