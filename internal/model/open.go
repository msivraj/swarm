package model

// P3 boundary types: the open/untrusted tier dispatches a task to K randomly
// chosen machines running it sandboxed in WASM, and accepts only the answer a
// quorum agrees on. Every type below is plain data reasoned over by P3's pure
// cores (sandbox grants, quorum verification, reputation, honeypot,
// anonymous enrollment) — see docs/phases/swarm-p3-components.txt §02.

// MachineID identifies a machine eligible for open-tier assignment (assign()
// picks K of these from a pool).
type MachineID string

// SpiffeID is the anonymous open-tier identity SPIRE issues on enrollment —
// open-tier machines have no account, only a SPIFFE id.
type SpiffeID string

// Sig is a detached signature over a workload or module. Verified with stdlib
// crypto (e.g. crypto/ed25519) in the core — pure, fcischeck-clean.
type Sig []byte

// PubKey is the public key a Sig is verified against.
type PubKey []byte

// WasiCaps is the least-privilege WASI capability set a sandboxed task is
// granted — exactly what the task declared, nothing more (see grants()). The
// shell maps these fields to wazero/WASI config; this holds only what a task
// can DECLARE, not sandbox implementation detail. The zero value grants
// nothing: no paths, no env, no stdio, no clock — the safe default.
type WasiCaps struct {
	// ReadPaths are the filesystem path roots the task may read.
	ReadPaths []string
	// WritePaths are the filesystem path roots the task may write.
	WritePaths []string
	// Env is the allow-listed set of environment variable names the task
	// may read; unlisted variables are not visible to the module.
	Env []string
	// Stdio grants access to stdin/stdout/stderr.
	Stdio bool
	// Clock grants the task permission to read the wall/monotonic clock.
	// (Granting Clock does not give the CORE a clock — only the sandboxed
	// guest module, which the shell instantiates.)
	Clock bool
}

// Result is one machine's claimed outcome for a verified open-tier task.
// verdict() and the honeypot check() reason over these; carrying the
// identity makes a lie attributable.
type Result struct {
	ID    SpiffeID
	Value []byte
	OK    bool
}

// VerdictKind is the quorum outcome over K Results.
type VerdictKind int

const (
	// Insufficient means too few results were collected to decide.
	Insufficient VerdictKind = iota
	// Disputed means no majority of the K results agreed.
	Disputed
	// Agreed means a majority agreed on Value.
	Agreed
)

// Verdict is the quorum decision verdict() returns. The zero value is
// Insufficient — the safe/undecided case, never a false accept.
type Verdict struct {
	Kind VerdictKind
	// Value is set only when Kind == Agreed.
	Value []byte
}

// Reputation is an identity's earned trust. update() raises it on honest
// agreement and lowers it on a lie; weight() maps it to a float; needsK() /
// redundancy() map it (with Tier) to a replica count. The zero value is the
// brand-new/untrusted starting point every fresh identity gets — faking many
// identities buys no shortcut (zero-start property).
type Reputation struct {
	// Score is a signed accumulator: it rises on honest agreement and falls
	// on a detected lie. Zero means no history either way — a brand-new
	// identity, not a good one.
	Score int64
	// Observations counts the verdicts this identity has participated in,
	// so weight()/needsK() can temper trust for identities with little
	// history even if Score happens to be nonzero.
	Observations int
}

// Probe is the honeypot outcome for a known-answer spot-check.
type Probe int

const (
	// Match means the claimed result agreed with the known answer.
	Match Probe = iota
	// Lie means the claimed result disagreed with the known answer.
	Lie
)

// ActionKind is what the shell must do in response to a honeypot outcome.
type ActionKind int

const (
	// NoAction means take no action — the zero value, safe/inert default.
	NoAction ActionKind = iota
	// Blacklist means the shell must blacklist the identified machine.
	Blacklist
)

// Action is the shell-facing effect onLie() returns. The zero value
// (NoAction) is inert, so an uninitialized Action never blacklists anyone.
type Action struct {
	Kind ActionKind
	// ID is the identity to blacklist when Kind == Blacklist.
	ID SpiffeID
}

// JoinReq is an anonymous open-tier enrollment request.
type JoinReq struct {
	// PubKey is the joiner's offered signing key.
	PubKey PubKey
	// Nonce is client-chosen input the PoW is computed over.
	Nonce []byte
}

// PowProof is the joiner's proof-of-work solution for a JoinReq.
type PowProof struct {
	// Nonce echoes JoinReq.Nonce (or the challenge the client solved).
	Nonce []byte
	// Solution is the value whose hash must meet the configured difficulty.
	Solution []byte
}

// PowCfg configures the proof-of-work difficulty admitOpen() requires.
type PowCfg struct {
	// DifficultyBits is the required number of leading-zero bits of the
	// solution's hash. Data, not randomness — the shell/operator sets it.
	DifficultyBits int
}

// AdmitKind is the enrollment decision admitOpen() returns.
type AdmitKind int

const (
	// Reject means the join request is refused — the zero value, so an
	// uninitialized Admit never grants an identity.
	Reject AdmitKind = iota
	// Accept means the join request is admitted and issued an identity.
	Accept
)

// Admit is the enrollment decision. The zero value (Reject) is the safe
// default — an uninitialized Admit never admits anyone.
type Admit struct {
	Kind AdmitKind
	// ID is the issued open-tier identity when Kind == Accept.
	ID SpiffeID
}
