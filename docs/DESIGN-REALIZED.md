# Swarm — the design, realized

*Cell-based orchestration for 2 → 1,000,000 tightly-coupled machines.*
*Companion to the design brief (`docs/swarm-design.txt`) and the per-phase specs
(`docs/phases/`). This is what got built.*

**Status: all six phases complete (P0–P6).** Every phase merged autonomously
through the PM → builder → auditor pipeline with the gates green and the
`audit/semantic` check passing — **zero human merges** on the component work. The
full `-race` suite is green across 52 packages. The only open work is deferred
owner-infrastructure (see [What is deferred](#what-is-deferred)).

---

## 1. The thesis, and the one line that never moved

The brief asked for three things that fight each other on a flat topology: **tight
coupling**, **scale to a million**, and **low latency**. Swarm's answer is a fleet
that **divides instead of resizing** — coordination is confined to bounded **cells**
that split and merge as the swarm grows, so per-node coordination cost stays O(1).

Everything in this repo is built on a second, structural thesis:

> **Functional Core, Imperative Shell (FCIS).** Every *decision* is a pure
> function of plain data — deterministic, no I/O, no clock, no randomness (the
> clock and any seeds are passed *in* as data). Every *effect* — gossip, gRPC,
> WASM, FoundationDB, a GPU all-reduce, a TPM quote — lives in a thin shell that
> gathers inputs, calls a core, and carries the returned decision out.

The payoff, proven over six phases and five orders of magnitude of scale: **the
line between decision and effect never had to move.** New capability arrived as
*new pure cores plus thin shells*, never as a redesign. The last phase is the
cleanest proof — self-tuning mitosis is the *same* pure `Decide` shape with a
richer input (`DecideSignal(P99=0) == Decide`, so P0's tests still pass).

The boundary is **enforced**, not aspirational: a custom analyzer (`tools/fcischeck`)
fails the build if anything under `internal/core/**` imports I/O, reads a clock,
draws randomness, or imports outside the allow-list (stdlib + `internal/model` +
sibling `internal/core/*`) — and it scans `_test.go` too.

---

## 2. The functional core, entire

Every decision Swarm makes is one of these pure functions. You can read and test
each in isolation; that is the whole point.

| Phase | Pure cores (`internal/core/…`) |
|---|---|
| **P0** | `admission.Admit` · `placement.Place` · `mitosis.Decide` (+`Gate`) · `registry.Apply`/`Snapshot` · `runner` step · `templates` decompose/merge · `rendezvous` · `agentreg` |
| **P1** | `routing.Route`/`MergeGlobal`/`Diverged`/`Summarize` · `region.SelectRegion` · `placement.PlaceAcross` · `aggregate.Merge` |
| **P2** | driver `step` (barrier / leader / message-passing) + `resume` · `checkpoint` due/snapshot/restore · mitosis-at-checkpoint gate · `admission.AdmitGang` · `detection` deadline/isDead · GPU capability predicate |
| **P3** | `sandbox.Grants`/`VerifyModule` · `verification.Assign`/`Redundancy`/`Verdict` · `reputation.Update`/`Weight`/`NeedsK` · `honeypot.ShouldProbe`/`Check`/`OnLie` · `enrollment.AdmitOpen`(PoW)/`VerifySignature` |
| **P4** | `registry.ShardOf` · `observability.RollupRegion`/`RollupGlobal`/`Budget` · `backpressure.AdmitUnderLoad`/`UpdateLoad` · `upgrade.NextDrain`/`SkewSafe` |
| **P5** | `tenancy.WithinQuota`/`NextFair`(DRF)/`Scope` · `recovery.RecoveryPlan`/`RpoMet` · `sla.Evaluate`/`ErrorBudget`/`ShouldAlert` · `attestation.VerifyAttestation`/`TrustFromAttestation` |
| **P6** | `mitosis.DecideSignal`/`signalThreshold` · `placement.Rank`/`BestFit` · `reputation.Decay`/`Tier` (+ P3-hardening `honeypot.OnRepeatedLie`, `reputation.Eligible`) |

A property test guards each load-bearing law, e.g. *a minority can't flip a
quorum verdict*, *a fresh Sybil earns nothing*, *the two-step rollup equals the
flat one*, *never drain two cells of a job at once*, *decay never goes negative*,
*signal-based mitosis subsumes count-based*.

---

## 3. The imperative shell

Every effect lives under `internal/shell/**` (and `cmd/{swarm,swarmd}`). The loop
is always the same: **gather events → call the core → execute the returned
commands.** Highlights:

- **control plane** (`shell/controlplane`) — gRPC surface, membership reaper,
  mitosis loop (now fed measured p99 via a per-cell `CellSignalSource` seam),
  locality-preferred placement (`LocalitySource` → `BestFit`, falling back to
  `Place`), gang admission, and the backpressure admission middleware.
- **per-cell leader** (`shell/cell`) — a real multi-node hashicorp/raft cluster
  the cell's own agents elect; the elected leader hosts the coordination driver
  loop with snapshot/resume failover.
- **agent** (`shell/agent`) — pull/run/report, cross-region failover, the
  leader-host loop.
- **stores** (`shell/store`) — the `Store` interface; an in-memory sharded fake
  (CI default) *and* a real, build-tagged FoundationDB adapter (`//go:build fdb`).
- **security shells** — `shell/sandbox` (wazero WASM runner), `shell/verification`
  (K-way dispatch/collect coordinator), `shell/reputation`, `shell/honeypot`,
  `shell/enrollment` (SPIRE issuance behind an `IdentityIssuer` seam),
  `shell/attestation` (`AttestationProvider` seam).
- **ops shells** — `shell/observability` (OpenTelemetry emission), `shell/sla`,
  `shell/recovery` (+ chaos harness), `shell/tenancy`, `shell/upgrade`.

---

## 4. The arc, phase by phase

- **P0 — the runnable core.** CLI → control plane (gRPC) → agents exec native
  workers → aggregated results; cells split under load. A real 2→N MVP.
- **P1 — multi-region.** Global routing, cross-region spill, and hierarchical
  worker→cell→region→global roll-up (one associative merge per tier).
- **P2 — tight coupling.** Four coordination drivers (barrier with a min-members
  floor, leader, message-passing, independent), each a pure `step`, hosted by an
  agent-elected per-cell raft leader; gang scheduling; checkpoints; a live
  distributed-training run end-to-end.
- **P3 — the open/untrusted tier.** Anonymous machines run work sandboxed in
  **wazero** and every result is confirmed by **quorum**; a minority of liars is
  outvoted. Reputation (zero-start), honeypots, and proof-of-work close the
  Sybil gaps — all pure decisions, so the safety is property-tested, not hoped.
- **P4 — scale.** Sharded registry behind the `Store` seam (real FoundationDB,
  proven locally), bounded-cardinality metric rollups, backpressure, and
  zero-loss rolling upgrade. Almost nothing touched a core — the FCIS proof of
  the phase.
- **P5 — production readiness.** Multi-tenancy (DRF fairness + quotas +
  isolation), disaster recovery with a chaos harness that asserts the fleet
  converges to the *pure* recovery plan, SLAs with hysteresis, and
  hardware-agnostic core-tier attestation. A dedicated adversarial security
  review over all the pure security cores came back clean.
- **P6 — self-tuning.** Mitosis divides on *measured* p99 (not a size proxy),
  placement ranks by capability + rack/AZ/region locality (greedy, no solver),
  and reputation matures with time-decay + tiers. The composed capstone runs all
  six phases with self-tuning on over a simulated fleet.

Between P5 and P6, a **graduated-eviction hardening pass** matured the untrusted
tier's response to bad behavior: a honeypot lie now blacklists on the *second*
strike (absorbing a transient fault), and a chronic quorum-loser is *soft-frozen*
out of work (via reputation floor) rather than hard-booted — because a quorum
disagreement is not proof of malice, and hard-booting on it would let an attacker
grief an honest node caught in a liar-majority K-set. Provable dishonesty (a
honeypot lie) still hard-blacklists; disagreement only decays trust.

---

## 5. The two trust tiers

Swarm runs machines in two tiers, and the security model differs by tier:

- **Open / untrusted tier** — anonymous, possibly hostile machines (donated,
  rented, unknown hardware). Optionally gated by **proof-of-work** at join
  (an anti-Sybil *cost*), their *work* verified by **quorum + reputation +
  honeypots**. You assume they lie and verify the output.
- **Core / trusted tier** — machines an operator runs and trusts. **Attestation**
  optionally binds a machine's identity to genuine, measured hardware (a signed
  TPM/enclave quote) to *raise* assurance. It is **hardware-agnostic and
  optional**: a machine with no attestation still runs at a baseline trust tier,
  and *nothing in the codebase assumes a TPM* — it sits behind an
  `AttestationProvider` seam, one pluggable provider among many.

---

## 6. Where the boundary is enforced

- **`tools/fcischeck`** — fails the build on any core impurity or out-of-allow-list
  import, `_test.go` included.
- **`make gate-full`** (what CI runs) — `go build` · `gofmt` · `go vet` ·
  `golangci-lint` · **fcischeck** · `go test -race ./...` · **core coverage ≥ 90%**
  · the required `audit/semantic` status. Hermetic and fast: no live services, no
  clusters, no CGo. Heavier, infra-dependent checks (the real FDB adapter) run
  through a **local** harness (`make test-fdb`) and a pre-push hook, never in CI.
- **Adversarial audit** — every PR gets a second-line semantic + test-quality
  review that defaults to failure when uncertain and frequently *mutation-tests*
  a core to confirm its property tests actually discriminate.
- **Capstones** — each phase exits on a hermetic end-to-end test that composes the
  *real* merged components (never reimplemented) and asserts the phase's headline
  guarantee non-vacuously.

---

## 7. What is deferred

These are genuine **owner-infrastructure** activities — they need real resources,
not pipeline work — and are tracked as open issues:

- **The literal sustained ~1M-node cloud run** — the true GA gate. The hermetic
  capstones have already de-risked it to *measuring SLOs*, not *hunting logic
  bugs*, because the decision cores were exhaustively tested in isolation.
- **#179 — production FoundationDB deployment recipe.** The swarm code is
  topology-agnostic (the FDB adapter only needs a cluster file); standing up a
  real multi-node cluster is an ops artifact, analogous to how kubeadm bootstraps
  etcd out of band.
- **#194 — a real SPIRE node-attestation TPM plugin.** Optional, build-tagged out
  of hermetic CI, implementing the existing `AttestationProvider` seam — with the
  guard that nothing in the codebase may assume a TPM.

---

## 8. Where things live

```
internal/model/   shared boundary types (pure data)
internal/core/    PURE decisions — enforced by fcischeck
internal/shell/   ALL I/O
cmd/{swarm,swarmd} CLI + node daemon
tools/fcischeck/  the core-purity analyzer (protected)
docs/ docs/phases/ the design brief + per-phase specs + this retrospective
```

Code is organized **by component** (mitosis, placement, verification, …), never by
phase — cores evolved in place across phases (P6's mitosis edited P0's, additively),
and the phases live as GitHub milestones, not directories.

---

*From two machines to a million: every decision a pure function you can read and
test in isolation, every effect in a thin shell — and the line between them never
had to move. That is the design, realized.*
