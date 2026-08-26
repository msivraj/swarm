# Swarm

A cell-based orchestrator for 2 → 1,000,000 machines, scaling via dynamic
"mitosis" (cells split and merge). Built on **Functional Core, Imperative
Shell**: every decision is a pure function; every effect lives in a thin shell.

- **Design** — `docs/` (the brief) and `docs/phases/` (per-phase component specs, P0–P6).
- **Engineering contract** — [`CLAUDE.md`](CLAUDE.md): the rules, the gates, the workflow. Read it before contributing.
- **Reference core** — [`internal/core/mitosis`](internal/core/mitosis): the shape every core follows.

## Layout

```
internal/model/   shared boundary types (pure data)
internal/core/    PURE — no I/O (enforced by tools/fcischeck + depguard)
internal/shell/   ALL I/O — gossip, gRPC, stores, WASM host
cmd/{swarm,swarmd} CLI + node daemon
```

## Gates

```
make gate-fast   # quick local loop: gofmt · vet · fcischeck · test
make gate-full   # authoritative: + golangci-lint + core coverage ≥ 90%
```

CI runs `gate-full` on every PR; a required `audit/semantic` check and all gates
green auto-merge the PR. Enforcement config is human-owned (see `CODEOWNERS`).
