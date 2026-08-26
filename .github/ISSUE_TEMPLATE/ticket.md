---
name: Build ticket
about: A self-contained unit a Sonnet builder agent can complete end to end
title: "[P?][component] core|shell: <what>"
labels: []
---

## Component
<!-- e.g. core/mitosis — cross-reference docs/phases/swarm-pN-components -->

## FCIS class
<!-- `core` (pure, no I/O) or `shell` (I/O). One ticket is one class. -->

## Signatures
```go
// The exact function signatures to implement, lifted from the phase doc.
```

## Acceptance criteria
- [ ] Implements the signatures above, in the stated package
- [ ] Tests cover: <named cases / properties that must pass>
- [ ] `make gate-full` is green — build · gofmt · vet · lint · fcischeck · test · core coverage ≥ 90%

## Package path
<!-- e.g. internal/core/mitosis -->

## Depends on
<!-- issue numbers that must merge before this can start -->

## Definition of done
All CI gates green **and** the auditor's `audit/semantic` check passes → the PR auto-merges.
Do not edit protected paths (see CLAUDE.md); a builder that needs a gate changed must escalate.
