# Swarm — engineering contract

The one source every agent (PM, builder, auditor) and human follows. If code and
this file disagree, this file wins — fix the code.

Swarm is a cell-based orchestrator for 2 → 1,000,000 machines. The full design
lives in `docs/` (brief) and `docs/phases/` (per-phase component specs, P0–P6).
Read the relevant phase doc before writing a component — it carries the exact
signatures and the properties to test.

## The one rule: Functional Core, Imperative Shell

- **Core** (`internal/core/**`) is **pure**: deterministic, no I/O, no clock, no
  randomness. It takes plain data and **returns descriptions of effects
  (commands)** — it never performs them. The clock and any randomness are passed
  **in as data** (`model.Instant`, seeds).
- **Shell** (`internal/shell/**`) does **all I/O**: gossip, gRPC, stores, the
  WASM host, process execution, the clock. Its loop is: gather events → call the
  core → execute the returned commands.
- **Dependency direction:** shell may import core; **core may never import
  shell** (or `net`, `os`, `database/sql`, gRPC, NATS, …).
- **Core import allow-list:** a core may import **only** the standard
  library, `internal/model` (shared data), and sibling `internal/core/*`
  packages — nothing else. **Core may call core** (e.g. `admission →
  templates`, `routing → registry`): pure-on-pure reuse, kept acyclic by the
  Go compiler and pure by the rules above. That is the *only* cross-package
  freedom — no third-party modules, no shell — and fcischeck enforces it as
  an allow-list, so nothing new slips in by omission.

`internal/core/mitosis` is the reference: read it before writing any core.

## Layout

```
internal/model/   shared boundary types (pure data)
internal/core/    PURE — enforced by fcischeck + depguard
internal/shell/   ALL I/O
cmd/{swarm,swarmd} CLI + node daemon
tools/fcischeck/  the core-purity analyzer (protected)
docs/ docs/phases/ the design brief and per-phase component specs
```

Organize code **by component** (mitosis, placement, driver, verify, …), never by
phase — cores evolve in place across phases (P4's registry is P0's, unchanged;
P6's mitosis edits P0's). Phases are tracked as **GitHub milestones**, not
directories.

## The gates (authoritative — `make gate-full`)

A PR merges only when all of these are green:

1. `go build` · `go vet`
2. `gofmt` clean · `golangci-lint`
3. **fcischeck** — no I/O import, no `time.Now`/`Since`/`Until`, no
   `math/rand`/`crypto/rand`, no dot-imports, and the **core import
   allow-list** (stdlib + `internal/model` + sibling `internal/core` only)
   inside `internal/core`
4. `go test -race ./...`
5. **core coverage ≥ 90%** (cores are pure — no excuse)
6. the auditor's required **`audit/semantic`** status check

Determinism is enforced structurally: because fcischeck forbids the clock and
randomness in core, identical inputs give identical output. Add a determinism
test for any nontrivial core (see `mitosis_test.go`).

Run the fast subset locally as you edit: `make gate-fast`.

## Testing requirements

- Every core function has a **table-driven test**; algebraic laws
  (`restore(snapshot(x)) == x`, commutativity, "a minority can't flip a verdict")
  get a **property test**.
- Tests assert on the **commands a core returns**, never on side effects — that
  is what makes them fast and total.

## Ticket workflow

- One ticket = **one component's pure core + its tests**, or **one shell + its
  integration test**. Never both classes in one ticket.
- A ticket is done when **all gates are green** and the auditor passes → it
  **auto-merges**. No human merge step.
- On a gate or audit failure: fix and push; after **2 failed attempts**, apply
  `needs-human` and stop.

## Protected paths — agents must NOT edit

`/.github/`, `/.golangci.yml`, `/tools/fcischeck/`, `/CLAUDE.md`, `/.claude/`,
`/Makefile`, `/scripts/`, `/CODEOWNERS`. These are the gates; the judged cannot
edit the judge. A local PreToolUse hook blocks it and CODEOWNERS requires human
review. If a gate is wrong, escalate — do not route around it.

## Agents

- **pm** (Opus) — approved feature → dependency-ordered GitHub issues, each
  self-contained (signatures, acceptance tests, package path, deps, DoD),
  assigned to its phase milestone.
- **builder** (Sonnet) — one ticket → branch, implement, write tests, turn
  `make gate-full` green, open a PR. Touch only files in the ticket's scope.
- **auditor** (Opus) — one PR → semantic review vs. the spec + test-quality
  review; posts the required `audit/semantic` status check.
