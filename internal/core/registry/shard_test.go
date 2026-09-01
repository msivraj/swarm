package registry

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// TestShardOfTable covers boundary keys and small shard counts.
func TestShardOfTable(t *testing.T) {
	tests := []struct {
		name   string
		key    model.Key
		shards uint32
		want   model.ShardID
	}{
		{"empty key, one shard", "", 1, 0},
		{"empty key, many shards is the lowest bucket", "", 8, 0},
		{"min byte key is the lowest bucket", "\x00\x00\x00\x00\x00\x00\x00\x00", 4, 0},
		{"max byte key is the highest bucket", "\xff\xff\xff\xff\xff\xff\xff\xff", 4, 3},
		{"shards == 1 maps every key to shard 0 (low key)", "a", 1, 0},
		{"shards == 1 maps every key to shard 0 (high key)", "\xff\xff\xff\xff\xff\xff\xff\xff", 1, 0},
		{"shards == 0 is treated as a single shard, no panic", "agent-1", 0, 0},
		{"shards == 0 with the empty key", "", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShardOf(tt.key, tt.shards); got != tt.want {
				t.Fatalf("ShardOf(%q, %d) = %d, want %d", tt.key, tt.shards, got, tt.want)
			}
		})
	}
}

// TestShardOfIsTotalAndDeterministic is the shardOf property named in the
// ticket: for an enumerated set of keys and shard counts, ShardOf always
// returns a ShardID in [0, shards), and equal (key, shards) inputs always
// produce the equal output, on any number of repeated calls.
func TestShardOfIsTotalAndDeterministic(t *testing.T) {
	keys := enumeratedKeys(500)
	shardCounts := []uint32{0, 1, 2, 3, 5, 8, 16, 64, 1000}

	for _, key := range keys {
		for _, shards := range shardCounts {
			first := ShardOf(key, shards)

			want := shards
			if want == 0 {
				want = 1 // shards == 0 behaves as a single shard
			}
			if uint32(first) >= want {
				t.Fatalf("ShardOf(%q, %d) = %d, want in [0, %d)", key, shards, first, want)
			}

			for i := 0; i < 20; i++ {
				if got := ShardOf(key, shards); got != first {
					t.Fatalf("ShardOf(%q, %d) is non-deterministic: run %d = %d, want %d", key, shards, i, got, first)
				}
			}
		}
	}
}

// TestShardOfEqualKeysEqualShard is the "well-defined function" half of the
// shardOf property: equal keys map to equal shards.
func TestShardOfEqualKeysEqualShard(t *testing.T) {
	a := model.Key("cell-42")
	b := model.Key("cell-42")
	if ShardOf(a, 16) != ShardOf(b, 16) {
		t.Fatalf("equal keys mapped to different shards: %d vs %d", ShardOf(a, 16), ShardOf(b, 16))
	}
}

// TestShardOfIsRangePartitioned checks the range property: walking an
// ordered sequence of keys, ShardOf's result is monotonic non-decreasing —
// a key's shard never goes backwards relative to an earlier, lexicographically
// smaller key. This is what makes ShardOf a RANGE partition (contiguous key
// buckets) rather than a scattering hash.
func TestShardOfIsRangePartitioned(t *testing.T) {
	// orderedKeys is sorted ascending by Go string (byte) order, which is
	// also the order ShardOf's first-8-bytes projection respects.
	orderedKeys := []model.Key{
		"", "a", "aa", "ab", "b", "ba", "c", "m", "z", "za", "zz",
		"\xff", "\xff\x00", "\xff\xff",
	}
	const shards = 6

	prevShard := ShardOf(orderedKeys[0], shards)
	for _, key := range orderedKeys[1:] {
		shard := ShardOf(key, shards)
		if shard < prevShard {
			t.Fatalf("ShardOf(%q, %d) = %d went backwards from previous key's shard %d", key, shards, shard, prevShard)
		}
		prevShard = shard
	}
}

// TestShardOfDistribution enumerates a spread of keys — varying their
// leading bytes, as real cell/agent IDs would (e.g. hashed or
// randomly-prefixed, specifically to avoid hotspotting a range-sharded
// store) — and checks the resulting shard assignment is well-defined and
// roughly balanced: every shard gets a nonzero share, and no shard holds a
// wildly disproportionate share. This is a coarse bound, not a statistical
// test, matching a range partition's guarantee (evenly spread input keys
// land in evenly spread shards), not a hash's.
func TestShardOfDistribution(t *testing.T) {
	const (
		numKeys = 2000
		shards  = 16
	)
	keys := spreadKeys(numKeys)

	counts := make([]int, shards)
	for _, key := range keys {
		counts[ShardOf(key, shards)]++
	}

	minShare := numKeys / shards / 2 // no shard may hold less than half the even share
	maxShare := numKeys / shards * 2 // ...or more than double the even share
	for shard, count := range counts {
		if count == 0 {
			t.Fatalf("shard %d got no keys out of %d spread across %d shards", shard, numKeys, shards)
		}
		if count < minShare || count > maxShare {
			t.Fatalf("shard %d got %d keys, want roughly %d..%d (even share %d)", shard, count, minShare, maxShare, numKeys/shards)
		}
	}
}

// enumeratedKeys generates n fixed, deterministic keys — never math/rand —
// covering a range of lengths and byte patterns to exercise ShardOf's
// boundary handling.
func enumeratedKeys(n int) []model.Key {
	keys := make([]model.Key, 0, n+2)
	keys = append(keys, "", "\x00", "\xff")
	for i := 0; i < n; i++ {
		keys = append(keys, model.Key(shardKeyName(i)))
	}
	return keys
}

// shardKeyName deterministically formats a plain "agent-<i>" key.
func shardKeyName(i int) string {
	digits := []byte("0123456789")
	if i == 0 {
		return "agent-0"
	}
	var suffix []byte
	for i > 0 {
		suffix = append([]byte{digits[i%10]}, suffix...)
		i /= 10
	}
	return "agent-" + string(suffix)
}

// spreadKeys deterministically enumerates n keys whose leading two bytes
// cover the full byte range 0x00..0xff, so ShardOf's range partition
// (which projects a key onto its leading bytes) spreads them across many
// shards — mirroring a real key scheme chosen to avoid hotspotting a
// range-sharded store.
func spreadKeys(n int) []model.Key {
	keys := make([]model.Key, n)
	for i := 0; i < n; i++ {
		lead := []byte{byte(i % 256), byte((i / 256) % 256)}
		buf := append(lead, []byte(shardKeyName(i))...)
		keys[i] = model.Key(buf)
	}
	return keys
}
