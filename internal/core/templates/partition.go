package templates

import (
	"encoding/binary"
	"fmt"
)

// idRange is a contiguous, half-open sub-range [Start, End) of a
// zero-based ID space (sample indices, grid cells, vertex IDs, agent IDs).
// It is the shared wire shape every coordinated template's decompose below
// uses for Task.Input: two big-endian uint64s, fixed-width and unambiguous
// to decode in the agent's shell — the same layout KeyspaceJob's
// keyspaceInput already uses for its own, independently-defined range.
type idRange struct {
	Start uint64
	End   uint64
}

func (r idRange) bytes() []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], r.Start)
	binary.BigEndian.PutUint64(b[8:16], r.End)
	return b
}

func decodeIDRange(b []byte) (idRange, bool) {
	if len(b) != 16 {
		return idRange{}, false
	}
	return idRange{
		Start: binary.BigEndian.Uint64(b[0:8]),
		End:   binary.BigEndian.Uint64(b[8:16]),
	}, true
}

// partitionRange splits the zero-based ID space [0, total) into parts
// contiguous, non-overlapping idRanges that together cover [0, total) with
// no gaps — the same even-split-with-remainder-spread tiling
// KeyspaceDecompose uses, generalized to a fixed origin of 0 since every
// caller below partitions a whole sample/grid/vertex/agent count rather than
// an arbitrary [Start, End) window.
//
// Unlike KeyspaceDecompose (which treats Shards<=0 as 1 and clamps Shards
// down when it exceeds the range), partitionRange rejects total==0,
// parts<=0, and parts>total outright: every caller here decomposes a job for
// a barrier, leader, or message-passing driver, and those coordinated
// couplings need every requested worker to actually receive live work — a
// partition with an empty share would leave a driver waiting on a worker
// that has nothing to report. what names the caller in the returned error's
// message (e.g. "dist-training").
func partitionRange(total uint64, parts int, what string) ([]idRange, error) {
	if total == 0 {
		return nil, fmt.Errorf("%s: total is zero, nothing to partition", what)
	}
	if parts <= 0 {
		return nil, fmt.Errorf("%s: parts must be positive, got %d", what, parts)
	}
	if uint64(parts) > total {
		return nil, fmt.Errorf("%s: parts (%d) exceeds total (%d)", what, parts, total)
	}

	base := total / uint64(parts)
	rem := total % uint64(parts)

	ranges := make([]idRange, 0, parts)
	cur := uint64(0)
	for i := 0; i < parts; i++ {
		size := base
		if uint64(i) < rem {
			size++ // spread the remainder over the first `rem` parts
		}
		next := cur + size
		ranges = append(ranges, idRange{Start: cur, End: next})
		cur = next
	}
	return ranges, nil
}
