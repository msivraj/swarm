// Command keyspace is a tiny worker binary that internal/e2e's end-to-end
// test builds and hands to an internal/shell/agent.Agent to exec: the agent
// pipes a task's Input to this process's stdin and captures its stdout as
// the TaskResult's Output; a zero exit means ok=true, a non-zero exit means
// ok=false (see internal/shell/agent/runner.go's runProcess).
//
// Wire format: stdin is exactly 16 bytes, the two big-endian uint64s
// [start, end) a keyspace-search task's Input carries (see
// internal/e2e.DecodeKeyspaceRange, mirroring
// internal/core/templates/keyspace.go's unexported keyspaceInput). The
// target key to search for is passed via the SWARM_E2E_TARGET_KEY
// environment variable (a decimal uint64) rather than argv or stdin, since
// every task in the job shares the same target but gets a different
// sub-range: if start <= key < end, this worker writes the key back as an
// 8-byte big-endian uint64 on stdout (internal/e2e.EncodeKeyspaceHit — the
// Output internal/core/templates.KeyspaceMerge treats as the winning
// result) and exits 0; otherwise it writes nothing and exits 1, reporting
// the shard as a miss.
package main

import (
	"io"
	"os"
	"strconv"

	"github.com/msivraj/swarm/internal/e2e"
)

func main() {
	os.Exit(run())
}

func run() int {
	in, err := io.ReadAll(os.Stdin)
	if err != nil {
		return 1
	}
	start, end, ok := e2e.DecodeKeyspaceRange(in)
	if !ok {
		return 1
	}

	key, err := strconv.ParseUint(os.Getenv("SWARM_E2E_TARGET_KEY"), 10, 64)
	if err != nil {
		return 1
	}
	if key < start || key >= end {
		return 1
	}

	if _, err := os.Stdout.Write(e2e.EncodeKeyspaceHit(key)); err != nil {
		return 1
	}
	return 0
}
