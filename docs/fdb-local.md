# Local FoundationDB test harness

The swarm's registry can run on a real distributed store via a **build-tagged
FoundationDB adapter** (`internal/shell/store/fdb.go`, `//go:build fdb`). To keep
GitHub CI lean, that adapter is **never built or tested in CI** — the hermetic gate
(`make gate-full`) uses the in-memory sharded fake under the default build tags. The
FDB path is exercised **locally**, against a real single-node `fdbserver`, via
`make test-fdb`, and enforced on push by a git `pre-push` hook.

## One-time setup (single-node, for local testing)

```sh
# Download + install the FoundationDB 7.3.79 client + server packages (amd64):
cd /tmp
curl -fL -o fdb-clients.deb https://github.com/apple/foundationdb/releases/download/7.3.79/foundationdb-clients_7.3.79-1_amd64.deb
curl -fL -o fdb-server.deb  https://github.com/apple/foundationdb/releases/download/7.3.79/foundationdb-server_7.3.79-1_amd64.deb
sudo dpkg -i fdb-clients.deb fdb-server.deb || sudo apt-get install -f -y
```

The server package auto-configures a single-node cluster (`configure new single memory`)
and starts it under systemd (`/etc/foundationdb/fdb.cluster`). Verify:

```sh
fdbcli --exec "status minimal"   # → "The database is available."
```

`memory` storage is ephemeral (data lives in RAM, cleared on `fdbserver` restart) —
ideal for tests. The Go binding needs `libfdb_c.so` (from the clients package) + a C
compiler for CGo.

## Running the FDB tests

```sh
make test-fdb        # go test -tags fdb ./internal/shell/store/... -run FDB
```

## Enforcing it on push

```sh
make install-hooks   # sets core.hooksPath=scripts/hooks
```

The `pre-push` hook then runs `make test-fdb` automatically **only** when a push
changes FDB-relevant code (`internal/shell/store/fdb*`, `internal/core/registry/serialize*`).
Non-FDB pushes are unaffected. If FDB code changed but no local FDB is available, the
push is blocked with install guidance. Escape hatch: `SWARM_SKIP_FDB=1 git push`.

## Production is different

This is a **single-node test instance**, not a deployment. A production FDB is a
real multi-node cluster on dedicated infra; standing one up is tracked separately as
the FDB **deployment recipe** (issue #179). FDB self-manages sharding / replication /
rebalancing / failover *within* a cluster; it does not self-provision machines.
